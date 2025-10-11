use std::{sync::Arc, time::Duration};

use itertools::Itertools;
use prometheus::{opts, register_int_gauge, IntGauge};
use solana_lite_rpc_accounts::account_store_interface::AccountStorageInterface;
use solana_lite_rpc_cluster_endpoints::{
    geyser_grpc_connector::GrpcSourceConfig,
    grpc::grpc_accounts_streaming::start_account_streaming_tasks,
};
use solana_lite_rpc_core::{
    structures::{
        account_data::{AccountNotificationMessage, AccountStream},
        account_filter::AccountFilters,
    },
    AnyhowJoinHandle,
};
use tokio::sync::{
    broadcast::{self, Sender},
    watch,
};

lazy_static::lazy_static! {
static ref ON_DEMAND_SUBSCRIPTION_RESTARTED: IntGauge =
        register_int_gauge!(opts!("literpc_count_account_on_demand_resubscribe", "Count number of account on demand has resubscribed")).unwrap();

        static ref ON_DEMAND_UPDATES: IntGauge =
        register_int_gauge!(opts!("literpc_count_account_on_demand_updates", "Count number of updates for account on demand")).unwrap();
}

pub struct SubscriptionManger {
    account_filter_watch: watch::Sender<AccountFilters>,
}

impl SubscriptionManger {
    /// Creates a new subscription manager for accounts-on-demand.
    /// 
    /// ## Design: Lazy Subscription Initialization
    /// 
    /// The subscription starts with **empty filters** and the streaming task **waits** 
    /// for the first non-empty filter update. This is intentional:
    /// 
    /// 1. System starts with no subscriptions (saves resources)
    /// 2. When a user requests an account via RPC, `AccountsOnDemand.get_account()` is called
    /// 3. This triggers `refresh_subscription()` → `update_subscriptions()` 
    /// 4. The watch channel is updated with new filters
    /// 5. The waiting streaming task receives the update and starts subscribing
    /// 
    /// This lazy initialization prevents the gRPC error "Broadcast `any` is not allowed"
    /// while still allowing dynamic subscription updates as accounts are requested.
    pub fn new(
        grpc_sources: Vec<GrpcSourceConfig>,
        accounts_storage: Arc<dyn AccountStorageInterface>,
        account_notification_sender: Sender<AccountNotificationMessage>,
    ) -> Self {
        let (account_filter_watch, reciever) = watch::channel::<AccountFilters>(vec![]);

        let (_, mut account_stream) = create_grpc_account_streaming_tasks(grpc_sources, reciever);

        tokio::spawn(async move {
            loop {
                match account_stream.recv().await {
                    Ok(message) => {
                        ON_DEMAND_UPDATES.inc();
                        let _ = account_notification_sender.send(message.clone());
                        accounts_storage
                            .update_account(message.data, message.commitment)
                            .await;
                    }
                    Err(e) => match e {
                        broadcast::error::RecvError::Closed => {
                            panic!("Account stream channel is broken");
                        }
                        broadcast::error::RecvError::Lagged(lag) => {
                            log::error!("Account on demand stream lagged by {lag:?}, missed some account updates; continue");
                            continue;
                        }
                    },
                }
            }
        });
        Self {
            account_filter_watch,
        }
    }

    pub async fn update_subscriptions(&self, filters: AccountFilters) {
        if let Err(e) = self.account_filter_watch.send(filters) {
            log::error!(
                "Error updating accounts on demand subscription with {}",
                e.to_string()
            );
        }
    }
}

/// Creates gRPC account streaming tasks that wait for filter updates.
/// 
/// This function is designed for accounts-on-demand where filters are added dynamically.
/// The streaming task will wait (without consuming resources) until filters are provided
/// via the watch channel. This prevents starting subscriptions with empty filters.
/// 
/// Filters are added when:
/// - `AccountsOnDemand.get_account()` is called (user RPC request)
/// - `AccountsOnDemand.get_program_accounts()` is called  
/// - These call `refresh_subscription()` which updates the watch channel
pub fn create_grpc_account_streaming_tasks(
    grpc_sources: Vec<GrpcSourceConfig>,
    mut account_filter_watch: watch::Receiver<AccountFilters>,
) -> (AnyhowJoinHandle, AccountStream) {
    let (account_sender, accounts_stream) = broadcast::channel::<AccountNotificationMessage>(128);

    let jh: AnyhowJoinHandle = tokio::spawn(async move {
        // Wait for the first update with non-empty filters.
        // This is a BLOCKING wait that doesn't consume resources until filters arrive.
        // 
        // Note: .changed() returns immediately with the initial value (empty vec),
        // so we loop until we get actual valid filters before starting subscriptions.
        // 
        // Filters will be sent via the watch channel when users make RPC requests.
        loop {
            match account_filter_watch.changed().await {
                Ok(_) => {
                    let filters = account_filter_watch.borrow_and_update().clone();
                    if !filters.is_empty() {
                        log::info!("Accounts on demand received initial filters, starting subscription");
                        break;
                    }
                    log::debug!("Accounts on demand waiting for valid filters...");
                }
                Err(e) => {
                    log::error!("account filter watch failed with error {}", e);
                    anyhow::bail!("Accounts on demand task failed");
                }
            }
        }

        let accounts_filters = account_filter_watch.borrow_and_update().clone();

        let has_started = Arc::new(tokio::sync::Notify::new());

        let mut current_tasks = grpc_sources
            .iter()
            .map(|grpc_config| {
                start_account_streaming_tasks(
                    grpc_config.clone(),
                    accounts_filters.clone(),
                    account_sender.clone(),
                    has_started.clone(),
                )
            })
            .collect_vec();

        while account_filter_watch.changed().await.is_ok() {
            ON_DEMAND_SUBSCRIPTION_RESTARTED.inc();
            // wait for a second to get all the accounts to update
            tokio::time::sleep(Duration::from_secs(1)).await;
            let accounts_filters = account_filter_watch.borrow_and_update().clone();

            // Skip restart if filters become empty (no accounts to monitor)
            if accounts_filters.is_empty() {
                log::debug!("Accounts on demand filters became empty, keeping previous subscription");
                continue;
            }

            let has_started = Arc::new(tokio::sync::Notify::new());

            let new_tasks = grpc_sources
                .iter()
                .map(|grpc_config| {
                    start_account_streaming_tasks(
                        grpc_config.clone(),
                        accounts_filters.clone(),
                        account_sender.clone(),
                        has_started.clone(),
                    )
                })
                .collect_vec();

            if let Err(_elapsed) =
                tokio::time::timeout(Duration::from_secs(60), has_started.notified()).await
            {
                // check if time elapsed during restart is greater than 60ms
                log::error!("Tried to restart the accounts on demand task but failed");
                new_tasks.iter().for_each(|x| x.abort());
                continue;
            }

            // abort previous tasks
            current_tasks.iter().for_each(|x| x.abort());

            current_tasks = new_tasks;
        }
        log::error!("Accounts on demand task stopped");
        anyhow::bail!("Accounts on demand task stopped");
    });

    (jh, accounts_stream)
}
