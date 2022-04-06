use chrono::{DateTime, Duration, NaiveTime, Utc};

#[derive(Clone, Copy)]
pub struct MessageStats {
    bytes: i32,
    time:NaiveTime
}

impl MessageStats {
    pub fn new()->Self{
        MessageStats{
            bytes: 0,
            time: Utc::now().time(),
        }
    }

    pub fn from_time(time: NaiveTime) -> Self {
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
    created: NaiveTime,
    message_count: i64,
    bytes_avg: f64,
    bytes_avg_variance: f32,
    time_avg_variance: Duration
}

impl TopicStats {
    pub fn new(bytes: i32, qos: i32)->Self{
        let time = Utc::now().time();
        TopicStats{
            old: MessageStats::from_time(time),
            last: MessageStats{bytes, time},
            qos,
            created: time,
            message_count: 1,
            bytes_avg: bytes as f64,
            bytes_avg_variance: 0.0,
            time_avg_variance: Duration::seconds(0)
        }
    }

    pub fn create_datapoint(&self, bytes: i32, qos: i32) -> Self{

        let message_count = self.message_count + 1;
        let time = Utc::now().time();

        let bytes_avg = self.bytes_avg + (bytes as f64 - self.bytes_avg)/message_count as f64;

        let current_bytes_difference = (self.last.bytes - bytes).abs();
        let bytes_avg_variance= self.bytes_avg_variance + (current_bytes_difference as f32 - self.bytes_avg_variance)/self.message_count as f32;

        let current_time_difference = (self.last.time - time) * -1;
        let time_avg_variance= self.time_avg_variance + (current_time_difference - self.time_avg_variance)/ self.message_count as i32;

        // generate a new topic stats object - should probably be mutating current struct
        TopicStats{
            old: self.last,
            last: MessageStats{bytes, time: Utc::now().time()},
            qos,
            created: self.created,
            message_count,
            bytes_avg,
            bytes_avg_variance,
            time_avg_variance
        }
    }
}

#[cfg(test)]
mod stats_tests{
    use std::thread;
    use std::time::Duration;
    use crate::stats::TopicStats;
    use chrono;


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
        assert_eq!(tp.bytes_avg_variance, 20.0);
        let tp = tp.create_datapoint(32, 1);
        assert_eq!(tp.bytes_avg_variance, 10.0);
    }
    #[test]
    fn time_variance() {
        let mut tp = TopicStats::new(12, 2);
        assert_eq!(tp.time_avg_variance, chrono::Duration::seconds(0));

        thread::sleep(Duration::from_secs(1));
        let tp = tp.create_datapoint(32, 1);
        let d = chrono::Duration::seconds(1);
        assert!(tp.time_avg_variance > d);
    }
}