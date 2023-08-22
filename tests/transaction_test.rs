use std::{thread, time::Duration};

use mapuche_embedded::{
    cmd::{Command, Get, Lrange, Lrem, Push, Set, Zadd, Zcard, Zrange, Zrem},
    OpenOptions,
};
use tokio::{join, task::spawn};

#[tokio::test]
async fn db_conn() {
    let options = OpenOptions::new();
    let db = options.open("./mapuche_store").await.unwrap();
    let conn = db.conn();
    let set_cmd = Command::Set(Set::new("test1", "value", None, None));
    let frame = conn.execute(set_cmd).await.unwrap();
    println!("{:?}", frame);
    let get_cmd = Command::Get(Get::new("test1"));
    let frame = conn.execute(get_cmd).await.unwrap();
    println!("{:?}", frame);

    let push_cmd = Command::Lpush(Push::new("testlist", vec!["aaa", "bbb"].as_slice()));
    let frame = conn.execute(push_cmd).await.unwrap();
    println!("{:?}", frame);

    let lrange_cmd = Command::Lrange(Lrange::new("testlist", 0, -1));
    let frame = conn.execute(lrange_cmd).await.unwrap();
    println!("{:?}", frame);

    let lrem_cmd = Command::Lrem(Lrem::new("testlist", 1, "aaa"));
    let frame = conn.execute(lrem_cmd).await.unwrap();
    println!("{:?}", frame);

    let lrange_cmd = Command::Lrange(Lrange::new("testlist", 0, -1));
    let frame = conn.execute(lrange_cmd).await.unwrap();
    println!("{:?}", frame);
}

#[tokio::test]
async fn multi_thread() {
    let options = OpenOptions::new();
    let db = options.open("./mapuche_store").await.unwrap();

    let db1 = db.clone();
    let t1 = spawn(async move {
        for i in 0..100 {
            let conn = db1.conn();
            let zadd_cmd = Command::Zadd(Zadd::new(
                "testz",
                vec![i.to_string()].as_slice(),
                vec![i.into()].as_slice(),
                None,
                false,
            ));
            conn.execute(zadd_cmd).await.unwrap();
            println!("zadd {:?}", i);
        }
    });

    let db2 = db.clone();
    let t2 = spawn(async move {
        for i in 100..200 {
            let conn = db2.conn();
            let zadd_cmd = Command::Zadd(Zadd::new(
                "testz",
                vec![i.to_string()].as_slice(),
                vec![i.into()].as_slice(),
                None,
                false,
            ));
            conn.execute(zadd_cmd).await.unwrap();
            println!("zadd {:?}", i);
        }
    });

    let db3 = db.clone();
    let t3 = spawn(async move {
        for i in 200..300 {
            let conn = db3.conn();
            let zadd_cmd = Command::Zadd(Zadd::new(
                "testz",
                vec![i.to_string()].as_slice(),
                vec![i.into()].as_slice(),
                None,
                false,
            ));
            conn.execute(zadd_cmd).await.unwrap();
            println!("zadd {:?}", i);
        }
    });

    let db4 = db.clone();
    let t4 = spawn(async move {
        for i in 0..100 {
            let conn = db4.conn();
            let zrem_cmd = Command::Zrem(Zrem::new("testz", vec![i.to_string()].as_slice()));
            conn.execute(zrem_cmd).await.unwrap();
            println!("zrem {:?}", i);
        }
    });

    let db5 = db.clone();
    let t5 = spawn(async move {
        for i in 100..200 {
            let conn = db5.conn();
            let zrem_cmd = Command::Zrem(Zrem::new("testz", vec![i.to_string()].as_slice()));
            conn.execute(zrem_cmd).await.unwrap();
            println!("zrem {:?}", i);
        }
    });

    let db6 = db.clone();
    let t6 = spawn(async move {
        for i in 200..300 {
            let conn = db6.conn();
            let zrem_cmd = Command::Zrem(Zrem::new("testz", vec![i.to_string()].as_slice()));
            conn.execute(zrem_cmd).await.unwrap();
            println!("zrem {:?}", i);
        }
    });

    t1.await.unwrap();
    t2.await.unwrap();
    t3.await.unwrap();
    t4.await.unwrap();
    t5.await.unwrap();
    t6.await.unwrap();

    let conn = db.conn();
    let zcard_cmd = Command::Zcard(Zcard::new("testz"));
    let frame = conn.execute(zcard_cmd).await.unwrap();
    println!("{:?}", frame);

    let zrange_cmd = Command::Zrange(Zrange::new("testz", 0, -1, false, false));
    let frame = conn.execute(zrange_cmd).await.unwrap();
    println!("{:?}", frame);
}

#[tokio::test]
async fn ttt() {
    let task_a = spawn(print_char_periodically('A', Duration::from_millis(2)));
    let task_b = spawn(print_char_periodically('B', Duration::from_millis(2)));
    let task_c = spawn(print_char_periodically('C', Duration::from_millis(2)));

    // 等待三个任务完成
    let _ = join!(task_a, task_b, task_c);
}

async fn print_char_periodically(ch: char, interval: Duration) {
    for _ in 0..5 {
        println!("{}", ch);
        thread::sleep(interval);
    }
}
