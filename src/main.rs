use chrono::{DateTime, Duration, Utc};
use std::{thread, time};

mod tsdb;

fn main() {
    let path = "_path_for_rocksdb_storage";
    let mut tsdb = tsdb::TimeseriesLocalDatabase::new(path.to_string());
    let utc: DateTime<Utc> = Utc::now();
    for a in 0..5 {
        println!("adding value");
        let m = tsdb::Measurement {
            name: format!("name-{}", a),
            value: 1.0,
            creation_date: Utc::now().timestamp(),
        };
        tsdb.put(m.clone()).unwrap();
        let sl = time::Duration::from_secs(2);
        thread::sleep(sl);
    }

    let r = tsdb.get(utc.timestamp());
    println!("value for {}: {:?}", utc.timestamp(), r);

    let e_range = utc + Duration::seconds(3);
    let range = tsdb.get_absolute_range(utc.timestamp(), e_range.timestamp());
    println!(
        "range from {} to {}: {:?}",
        utc.timestamp(),
        e_range.timestamp(),
        range
    );

    let range = tsdb.get_relative_range_in_seconds(utc.timestamp(), 60);
    println!("range for {} + 60 secs {:?}", utc.timestamp(), range);
    tsdb.destroy(false)
}
