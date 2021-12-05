use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use rocksdb::{Options, DB};
mod tsdb;

fn main() {
    let path = "_path_for_rocksdb_storage";
    {
        let db = DB::open_default(path).unwrap();
        let utc: DateTime<Utc> = Utc::now();
        for a in 0..10 {
            let nt = utc + Duration::seconds(a);
            let key = format!("{}", nt.timestamp());
            db.put(key, format!("{}", nt)).unwrap();
        }

        // db.put(b"my key 1", b"my value 1").unwrap();
        // db.put(b"my key 12", b"my value 2").unwrap();
        // db.put(b"my key 3", b"my value 3").unwrap();
        // db.put(b"my key 14", b"my value 4").unwrap();
        // db.put(b"my key 5", b"my value 5").unwrap();

        let kk = format!("{}", utc.timestamp_millis());
        // match db.get(kk.clone()) {
        //     Ok(Some(value)) => println!("retrieved value {}", String::from_utf8(value).unwrap()),
        //     Ok(None) => println!("value not found"),
        //     Err(e) => println!("operational problem encountered: {}", e),
        // }
        let mut iter = db.raw_iterator();
        iter.seek(kk);

        loop {
            if iter.valid() {
                let key = String::from_utf8(iter.key().unwrap().to_vec()).unwrap();
                let val = String::from_utf8(iter.value().unwrap().to_vec()).unwrap();

                println!("{:?} {:?}", key, val);
                let ts = utc + Duration::seconds(1);
                let i: i64 = key.parse().unwrap();
                let ndt = NaiveDateTime::from_timestamp(i, 0);
                //                if DateTime::parse_from_str(&key, "%s").unwrap() > ts {
                if ndt > ts.naive_utc() {
                    println!("range");
                    break;
                }
                iter.next();
            } else {
                println!("No keys found ");
                break;
            }
        }
        //db.delete(b"my key").unwrap();
    }
    let _ = DB::destroy(&Options::default(), path);
}
