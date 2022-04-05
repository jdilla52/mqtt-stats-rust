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

#[derive(Serialize, Deserialize, Clone)]
pub struct SpawnStats {
    skipped_messages: i32,
    routed_messages: i32,
    errors: i32,
    connection_error: i32,
    start_time: SystemTime,
}

impl Default for SpawnStats {
    fn default() -> Self {
        SpawnStats {
            skipped_messages: 0,
            routed_messages: 0,
            errors: 0,
            connection_error: 0,
            start_time: SystemTime::now(),
        }
    }
}

impl SpawnStats {
    pub(crate) fn as_json(&self) -> String {
        return serde_json::to_string(&self).unwrap();
    }

    fn from_json(s: &str) -> Self {
        return serde_json::from_str(s).unwrap();
    }
}

pub struct Spawn {
    settings: MqttSettings,
    mqtt_client: MqttClient,
    bridge_stats: Arc<Mutex<SpawnStats>>,
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
            bridge_stats: Arc::new(Mutex::new(SpawnStats::default())),
        }
    }
    pub async fn run(&mut self) {
        // spawn_api(&self.settings.http_settings, &self.bridge_stats);
        while let Some(msg_opt) = self.mqtt_client.message_stream.next().await {
            if let Some(msg) = msg_opt {
                let stats = Arc::clone(&self.bridge_stats);
                tokio::spawn(async move {
                    let mut guard = stats.lock().await;
                });
            } else {
                let mut guard = self.bridge_stats.lock().await;
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
