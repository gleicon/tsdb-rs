// proto tsdb with rocksdb

use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Measurement {
    // non-searchable fields
    name: String,
    value: f64,
}

//#[derive(Clone)]
pub struct TimeseriesLocalDatabase {
    db: rocksdb::DBWithThreadMode<rocksdb::SingleThreaded>,
    dbpath: String,
}

impl TimeseriesLocalDatabase {
    fn new(basepath: String) -> Self {
        let db = DB::open_default(basepath.clone()).unwrap();
        return Self {
            db: db,
            dbpath: basepath.clone(),
        };
    }

    fn put(&mut self, m: Measurement) {
        let utc: DateTime<Utc> = Utc::now();
        let ts = format!("{}", utc.timestamp());
        let payload: Vec<u8> = bincode::serialize(&m).unwrap();
        self.db.put(ts, payload).unwrap();
    }

    fn get(&mut self, key: i64) -> Result<Measurement, String> {
        match self.db.get(format!("{}", key.clone())) {
            Ok(Some(value)) => {
                let m: Measurement = bincode::deserialize(&value).unwrap();
                //println!("retrieved value {}", String::from_utf8(value).unwrap())
                return Ok(m);
            }
            Ok(None) => Err(format!("value not found")),
            Err(e) => Err(format!("query error: {}", e)),
        }
    }

    fn get_absolute_range(&mut self, start: i64, end: i64) -> Result<Vec<Measurement>, String> {
        let start_dt = NaiveDateTime::from_timestamp(start, 0);
        let end_dt = NaiveDateTime::from_timestamp(end, 0);
        self._get_absolute_range(start_dt, end_dt)
    }

    fn get_relative_range_in_seconds(
        &mut self,
        start: i64,
        duration: i64,
    ) -> Result<Vec<Measurement>, String> {
        let start_dt = NaiveDateTime::from_timestamp(start, 0);
        let end_dt = start_dt + Duration::seconds(duration);
        self._get_absolute_range(start_dt, end_dt)
    }

    fn _get_absolute_range(
        &mut self,
        start_dt: chrono::NaiveDateTime,
        end_dt: chrono::NaiveDateTime,
    ) -> Result<Vec<Measurement>, String> {
        let mut mv: Vec<Measurement> = vec![];

        let mut iter = self.db.raw_iterator();
        iter.seek(format!("{}", start_dt.timestamp()));

        loop {
            if iter.valid() {
                let key = String::from_utf8(iter.key().unwrap().to_vec()).unwrap();
                let val = String::from_utf8(iter.value().unwrap().to_vec()).unwrap();
                let i: i64 = key.parse().unwrap();
                let v: f64 = val.parse().unwrap();

                let curr = NaiveDateTime::from_timestamp(i, 0);
                if curr > end_dt {
                    break;
                }
                let m = Measurement {
                    name: key,
                    value: v,
                };
                mv.push(m);
                iter.next();
            } else {
                break;
            }
        }
        Ok(mv)
    }
}
