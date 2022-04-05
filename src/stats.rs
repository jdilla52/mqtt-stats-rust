use std::time::SystemTime;

#[derive(Clone, Copy)]
pub struct MessageStats {
    bytes: i32,
    time:SystemTime
}

impl MessageStats {
    pub fn new()->Self{
        MessageStats{
            bytes: 0,
            time: SystemTime::now(),
        }
    }

    pub fn from_time(time: SystemTime) -> Self {
       MessageStats{
           time,
           bytes: 0
       }
    }
}

// all entries must hold both the current and the last entry.
// we'll use the convention $ as the old key
// we'll move to a message buffer ie an index up to 100.
// #[derive(Clone, Copy)]
pub struct TopicStats {
    // comparison stats
    old: MessageStats,
    last: MessageStats,
    // meta stats
    qos: i32,
    created: SystemTime,
    message_count: i64
}

impl TopicStats {
    pub fn new(bytes: i32, qos: i32)->Self{
        let time = SystemTime::now();
        TopicStats{
            old: MessageStats::from_time(time),
            last: MessageStats{bytes, time},
            qos,
            created: time,
            message_count: 0
        }
    }

    pub fn create_datapoint(&self, bytes: i32, qos: i32) -> Self{
        // generate a new topic stats object - should probably be mutating current struct
        TopicStats{
            old: self.last,
            last: MessageStats{bytes, time: SystemTime::now()},
            qos,
            created: self.created,
            message_count: self.message_count + 1
        }
    }
}

#[cfg(test)]
mod stats_tests{
    use crate::stats::TopicStats;

    #[test]
    fn test_swap(){
        let mut tp = TopicStats::new(12, 2);
        let tp = tp.create_datapoint(32, 1);
        assert_eq!(tp.last.bytes, 32);
    }
    #[test]
    fn test_messages(){
        let mut tp = TopicStats::new(12, 2);
        let tp = tp.create_datapoint(32, 1);
        assert_eq!(tp.message_count, 1);
    }
}