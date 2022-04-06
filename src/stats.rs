use std::cmp;
use chrono::{DateTime, Duration, NaiveTime, Timelike, Utc};

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
    bytes_min: i32,
    bytes_max: i32,
    bytes_avg_variance: f32,
    time_avg_variance: Duration,
    time_difference_max: Duration,
    time_difference_min:Duration,
    kbs: f64
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
            bytes_min: bytes,
            bytes_max: bytes,
            bytes_avg_variance: 0.0,
            time_avg_variance: Duration::seconds(0),
            time_difference_max: Duration::seconds(0),
            time_difference_min: Duration::seconds(100000),
            kbs: 0.0
        }
    }

    pub fn create_datapoint(&self, bytes: i32, qos: i32) -> Self{

        let message_count = self.message_count + 1;
        let time = Utc::now().time();

        let bytes_avg = self.bytes_avg + (bytes as f64 - self.bytes_avg)/message_count as f64;
        let bytes_max = cmp::max(self.bytes_max, bytes);
        let bytes_min= cmp::min(self.bytes_min, bytes);

        let current_bytes_difference = (self.last.bytes - bytes).abs();
        let bytes_avg_variance= self.bytes_avg_variance + (current_bytes_difference as f32 - self.bytes_avg_variance)/self.message_count as f32;

        let time_difference= (self.last.time - time) * -1;
        let time_difference_max= cmp::max(self.time_difference_max, time_difference);
        let time_difference_min= cmp::min(self.time_difference_min, time_difference);
        let time_avg_variance= self.time_avg_variance + (time_difference - self.time_avg_variance)/ self.message_count as i32;

        let kbs = bytes as f64 / time_difference.num_nanoseconds().unwrap() as f64 * 8000000.0;

        // generate a new topic stats object - should probably be mutating current struct
        TopicStats{
            old: self.last,
            last: MessageStats{bytes, time: Utc::now().time()},
            qos,
            created: self.created,
            message_count,
            bytes_avg,
            bytes_min,
            bytes_max,
            bytes_avg_variance,
            time_avg_variance,
            time_difference_min,
            time_difference_max,
            kbs,

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
        assert_eq!(tp.message_count, 2);
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
    #[test]
    fn kbs() {
        let mut tp = TopicStats::new(10, 2);
        thread::sleep(Duration::from_secs(1));
        let tp = tp.create_datapoint(20, 2);
        assert!(tp.kbs > 0.08 );
    }

    #[test]
    fn min_max(){
        let mut tp = TopicStats::new(10, 2);
        let tp = tp.create_datapoint(20, 2);
        assert_eq!(tp.bytes_min, 10);
        assert_eq!(tp.bytes_max, 20);
    }
    #[test]
    fn min_max_time(){
        let mut tp = TopicStats::new(10, 2);
        thread::sleep(Duration::from_secs(1));
        let tp = tp.create_datapoint(20, 2);
        thread::sleep(Duration::from_secs(3));
        let tp = tp.create_datapoint(20, 2);
        assert!(tp.time_difference_max > chrono::Duration::seconds(3));
        assert!(tp.time_difference_max < chrono::Duration::seconds(4));
        assert!(tp.time_difference_min > chrono::Duration::nanoseconds(10));
        assert!(tp.time_difference_min < chrono::Duration::seconds(2));
    }
}