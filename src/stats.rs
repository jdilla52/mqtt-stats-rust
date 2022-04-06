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
    message_count: i64,
    bytes_avg: f64,
    bytes_avg_variance: f32,
}

impl TopicStats {
    pub fn new(bytes: i32, qos: i32)->Self{
        let time = SystemTime::now();
        TopicStats{
            old: MessageStats::from_time(time),
            last: MessageStats{bytes, time},
            qos,
            created: time,
            message_count: 1,
            bytes_avg: bytes as f64,
            bytes_avg_variance: 0.0
        }
    }

    pub fn create_datapoint(&self, bytes: i32, qos: i32) -> Self{

        let message_count = self.message_count + 1;
        let current_variance = (self.last.bytes - bytes).abs();
        // generate a new topic stats object - should probably be mutating current struct
        TopicStats{
            old: self.last,
            last: MessageStats{bytes, time: SystemTime::now()},
            qos,
            created: self.created,
            message_count,
            bytes_avg: self.bytes_avg + (bytes as f64 - self.bytes_avg)/message_count as f64,
            bytes_avg_variance: self.bytes_avg_variance + (current_variance as f32 - self.bytes_avg_variance)/self.message_count as f32,
        }
    }
}

#[cfg(test)]
mod stats_tests{
    use crate::stats::TopicStats;

    #[test]
    fn swap(){
        let mut tp = TopicStats::new(12, 2);
        let tp = tp.create_datapoint(32, 1);
        assert_eq!(tp.last.bytes, 32);
    }
    #[test]
    fn message_count(){
        let mut tp = TopicStats::new(12, 2);
        let tp = tp.create_datapoint(32, 1);
        assert_eq!(tp.message_count, 1);
    }
    #[test]
    fn rolling_avg(){
        let mut tp = TopicStats::new(12, 2);
        let tp = tp.create_datapoint(32, 1);
        assert_eq!(tp.bytes_avg, 22.0);
    }
    #[test]
    fn bytes_variance() {
        let mut tp = TopicStats::new(12, 2);
        let tp = tp.create_datapoint(32, 1);
        assert_eq!(tp.bytes_avg, 20.0);
        let tp = tp.create_datapoint(32, 1);
        assert_eq!(tp.bytes_avg, 10.0);
    }
}