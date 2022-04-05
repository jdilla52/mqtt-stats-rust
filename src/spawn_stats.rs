#![allow(dead_code)]

use crate::config::{MqttSettings, StatsSettings};
use crate::mqtt_client::MqttClient;
use futures::StreamExt;
use log::{debug, error};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Mutex;
use uuid::Uuid;
extern crate serde;

use serde::{Deserialize, Serialize};

// we will lock these as a group for group wise access
// it will also allow for easy serialization
#[derive(Serialize, Deserialize, Clone)]
pub struct SpawnStats {
    messages_received: i64,
    last_received: i64,
    num_topics: i64,
    connection_error: i64,
    start_time: SystemTime
}

impl Default for SpawnStats {
    fn default() -> Self {
        SpawnStats {
            messages_received: 0,
            last_received: 0,
            num_topics: 0,
            connection_error: 0,
            start_time: SystemTime::now(),
        }
    }
}

pub struct Spawn {
    settings: MqttSettings,
    mqtt_client: MqttClient,
    stats: Arc<Mutex<SpawnStats>>,
}

fn mqtt_to_kafka_topic(v: &str) -> String {
    str::replace(v, "/", "-")
}

impl Spawn {
    pub async fn new(settings: MqttSettings) -> Spawn {
        let mqtt_client = MqttClient::new(settings.clone()).await;
        mqtt_client.subscribe().await; // maybe move
        
        Spawn {
            mqtt_client,
            settings,
            stats: Arc::new(Mutex::new(SpawnStats::default())),
        }
    }
    pub async fn run(&mut self) {
        // spawn_api(&self.settings.http_settings, &self.bridge_stats);
        while let Some(msg_opt) = self.mqtt_client.message_stream.next().await {
            if let Some(msg) = msg_opt {
                let stats = Arc::clone(&self.stats);
                tokio::spawn(async move {
                    let mut guard = stats.lock().await;
                });
            } else {
                let mut guard = self.stats.lock().await;
                guard.connection_error += 1;
                // A "None" means we were disconnected. Try to reconnect...
                println!("Lost connection. Attempting reconnect.");
                error!(
                    "Lost connection. Attempting reconnect. error count: {}",
                    guard.connection_error
                );
                drop(guard);
                self.mqtt_client.try_reconnect();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bridge_message() {
        Spawn::new(confy::load_path("./default.conf").unwrap()).await;
    }
}
