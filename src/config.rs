#![allow(dead_code)]

extern crate confy;
extern crate serde;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct MqttSettings {
    pub(crate) address: String,
    pub(crate) client_id: String,
    pub(crate) mqtt_topic: Vec<String>,
    pub(crate) mqtt_qos: Vec<i32>,
    pub(crate) will_message: String,
    pub(crate) will_topic: String,
    pub(crate) user: String,
    pub(crate) pwd: String,
}

impl Default for MqttSettings {
    fn default() -> Self {
        MqttSettings {
            address: "tcp://127.0.0.1:1883".to_string(),
            client_id: "test_client".to_string(),
            mqtt_topic: vec!["#/".to_string()],
            mqtt_qos: vec![1],
            will_message: "Bridge node has failed".to_string(),
            will_topic: "test/dead".to_string(),
            user: "test".to_string(),
            pwd: "super".to_string(),
        }
    }
}

// #[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
// pub struct TopicSettings {
//     mqtt_topic: Vec<String>,
//     kafka_topic: String,
// }

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct StatsSettings {
    pub mqtt_settings: MqttSettings,
}


impl Default for StatsSettings {
    fn default() -> Self {
        StatsSettings {
            mqtt_settings: MqttSettings {
                address: "tcp://127.0.0.1:1883".to_string(),
                client_id: "test_client".to_string(),
                mqtt_topic: vec!["#".to_string()],
                mqtt_qos: vec![1],
                will_message: "Bridge node has failed".to_string(),
                will_topic: "bridge/dead".to_string(),
                user: "test".to_string(),
                pwd: "super".to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{StatsSettings, MqttSettings};

    #[test]
    fn test_parse() {
        let t: StatsSettings = confy::load_path("default./config").unwrap();
    }
    //     let t = StatsSettings {
    //         mqtt_settings: MqttSettings {
    //             address: "tcp://127.0.0.1:1883".to_string(),
    //             client_id: "test_client".to_string(),
    //             mqtt_topic: vec!["*".to_string()],
    //             mqtt_qos: vec![1],
    //             will_message: "Bridge node has failed".to_string(),
    //             will_topic: "bridge/dead".to_string(),
    //             user: "mqttAdmin".to_string(),
    //             pwd: "super".to_string(),
    //         },
    //     };
    // }
}