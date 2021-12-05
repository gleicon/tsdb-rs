// proto tsdb with rocksdb

use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Measurement {
    // non-searchable opaque fields
    pub name: String,
    pub value: f64,
    pub creation_date: i64,
}

//#[derive(Clone)]
pub struct TimeseriesLocalDatabase {
    db: rocksdb::DBWithThreadMode<rocksdb::SingleThreaded>,
    dbpath: String,
}

impl TimeseriesLocalDatabase {
    pub fn new(basepath: String) -> Self {
        let db = DB::open_default(basepath.clone()).unwrap();
        return Self {
            db: db,
            dbpath: basepath.clone(),
        };
    }

    pub fn put(&mut self, m: Measurement) -> Result<i64, String> {
        let utc: DateTime<Utc> = Utc::now();
        let ts = format!("{}", utc.timestamp());
        let payload: Vec<u8> = bincode::serialize(&m).unwrap();
        self.db.put(ts.clone(), payload).unwrap();
        Ok(utc.clone().timestamp())
    }

    pub fn get(&mut self, key: i64) -> Result<Measurement, String> {
        match self.db.get(format!("{}", key.clone())) {
            Ok(Some(value)) => {
                let m: Measurement = bincode::deserialize(&value).unwrap();
                return Ok(m);
            }
            Ok(None) => Err(format!("value not found")),
            Err(e) => Err(format!("query error: {}", e)),
        }
    }

    pub fn _delete(&mut self, key: i64) -> Result<(), String> {
        match self.db.delete(format!("{}", key.clone())) {
            Ok(_a) => Ok(()),
            Err(e) => Err(format!("delete error: {}", e)),
        }
    }

    pub fn get_absolute_range(&mut self, start: i64, end: i64) -> Result<Vec<Measurement>, String> {
        let start_dt = NaiveDateTime::from_timestamp(start, 0);
        let end_dt = NaiveDateTime::from_timestamp(end, 0);
        self._get_absolute_range(start_dt, end_dt)
    }

    pub fn get_relative_range_in_seconds(
        &mut self,
        start: i64,
        duration: i64,
    ) -> Result<Vec<Measurement>, String> {
        let start_dt = NaiveDateTime::from_timestamp(start, 0);
        let end_dt = start_dt + Duration::seconds(duration);
        self._get_absolute_range(start_dt, end_dt)
    }

    pub fn destroy(&mut self, remove_dir: bool) {
        let _ = DB::destroy(&Options::default(), self.dbpath.clone());
        if remove_dir {
            match std::fs::remove_dir_all(self.dbpath.clone()) {
                Ok(f) => {
                    println!("db removed: {:?}", f);
                }
                Err(e) => {
                    println!("Error removing db {}: {}", self.dbpath, e);
                }
            }
        }
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
                let val: Measurement = bincode::deserialize(iter.value().unwrap()).unwrap();

                let i: i64 = key.parse().unwrap();

                let curr = NaiveDateTime::from_timestamp(i, 0);
                if curr > end_dt {
                    break;
                }
                mv.push(val);
                iter.next();
            } else {
                break;
            }
        }
        Ok(mv)
    }
}

#[cfg(test)]

mod tests {
    #[test]
    fn test_db_creation() {
        let path = "_path_for_test1_rocksdb_storage";
        let mut tsdb = crate::tsdb::TimeseriesLocalDatabase::new(path.to_string());
        assert!(std::path::Path::new(path).is_dir());
        tsdb.destroy(true);
    }
    #[test]
    fn test_db_destroy() {
        let path = "_path_for_test2_rocksdb_storage";
        let mut tsdb = crate::tsdb::TimeseriesLocalDatabase::new(path.to_string());
        tsdb.destroy(true); // remove dir
        assert!(!std::path::Path::new(path).is_dir());
    }
    #[test]
    fn test_db_put_and_get() {
        let path = "_path_for_test3_rocksdb_storage";
        let mut tsdb = crate::tsdb::TimeseriesLocalDatabase::new(path.to_string());
        let m = crate::tsdb::Measurement {
            name: "test".to_string(),
            value: 0.1,
            creation_date: chrono::Utc::now().timestamp(),
        };
        let key = tsdb.put(m.clone()).unwrap();
        let mv = tsdb.get(key).unwrap();
        assert_eq!(m.value, mv.value);
        tsdb._delete(key).unwrap();
        tsdb.destroy(true);
    }
    #[test]
    fn test_absolute_ts_range() {
        let path = "_path_for_test4_rocksdb_storage";
        let mut tsdb = crate::tsdb::TimeseriesLocalDatabase::new(path.to_string());
        let utc: chrono::DateTime<chrono::Utc> = chrono::Utc::now();
        for a in 0..4 {
            let ts = (utc + chrono::Duration::seconds(a)).timestamp();
            let m = crate::tsdb::Measurement {
                name: format!("name-{}", a),
                value: 1.0,
                creation_date: ts,
            };
            tsdb.put(m.clone()).unwrap();
            let sl = std::time::Duration::from_secs(2);
            std::thread::sleep(sl);
        }

        let end_range = utc + chrono::Duration::seconds(3); // 3 second range should have 2 measures: utc + 3
        let range = tsdb
            .get_absolute_range(utc.timestamp(), end_range.timestamp())
            .unwrap();
        assert_eq!(range.len(), 2);
        tsdb.destroy(true);
    }
    #[test]
    fn test_relative_ts_range() {
        let path = "_path_for_test5_rocksdb_storage";
        let mut tsdb = crate::tsdb::TimeseriesLocalDatabase::new(path.to_string());
        let utc: chrono::DateTime<chrono::Utc> = chrono::Utc::now();
        for a in 0..4 {
            let ts = (utc + chrono::Duration::seconds(a)).timestamp();
            let m = crate::tsdb::Measurement {
                name: format!("name-{}", a),
                value: 1.0,
                creation_date: ts,
            };
            tsdb.put(m.clone()).unwrap();
            let sl = std::time::Duration::from_secs(2);
            std::thread::sleep(sl);
        }

        let range = tsdb
            .get_relative_range_in_seconds(utc.timestamp(), 3)
            .unwrap();
        assert_eq!(range.len(), 2);
        tsdb.destroy(true);
    }
}
