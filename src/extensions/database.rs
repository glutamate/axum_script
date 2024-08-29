use crate::routing::RouteRequest;
use crate::sqltojson::row_to_json;
use deno_core::op2;
use deno_core::serde_v8::from_v8;
use deno_core::{serde_v8::to_v8, OpState};
use serde_json::value::Number;
use serde_json::Value;
use sqlx::Pool;
use sqlx::{migrate::MigrateDatabase, Any, AnyPool, Sqlite};
use std::cell::RefCell;
use std::collections::HashMap;
use std::env;
use std::rc::Rc;
use std::sync::RwLock;
use tokio::sync::mpsc;

//async fn op_connect_db(state: Rc<RefCell<OpState>>, #[serde] conn_obj: serde_json::Value) -> () {

#[op2(async)]
async fn op_connect_db(state: Rc<RefCell<OpState>>, #[string] conn_obj: String) -> () {
    let state = state.borrow();

    let opoolref = state.borrow::<Rc<RefCell<Option<Pool<Any>>>>>();

    let pool = connect_database(&conn_obj).await;
    dbg!("connected to db from inside op");
    opoolref.replace(Some(pool));
    return ();
}

#[op2(async)]
#[serde]
async fn op_query(
    state: Rc<RefCell<OpState>>,
    #[string] sqlq: String,
    #[serde] pars: Vec<serde_json::Value>,
) -> serde_json::Value {
    let state = state.borrow();
    let opoolref = state.borrow::<Rc<RefCell<Option<Pool<Any>>>>>();
    let opool = opoolref.borrow();
    if let Some(pool) = &(*opool) {
        //let mut q =;

        let boundq: sqlx::query::Query<Any, sqlx::any::AnyArguments> =
            pars.into_iter()
                .fold(sqlx::query(&sqlq), |q, par| match par {
                    Value::String(s) => q.bind(s),
                    Value::Bool(b) => q.bind(b),
                    Value::Number(x) => {
                        if Number::is_i64(&x) {
                            q.bind(x.as_i64())
                        } else {
                            q.bind(x.as_f64())
                        }
                    }
                    _ => panic!("unknonw argumen"),
                });
        let rows = boundq.fetch_all(&(*pool)).await.unwrap();
        let rows: Vec<Value> = rows.iter().map(row_to_json).collect();
        return Value::Array(rows);
    } else {
        panic!("not connected to database")
    }
}

#[op2(async)]
#[serde]
async fn op_execute(
    state: Rc<RefCell<OpState>>,
    #[string] sqlq: String,
    #[serde] pars: Vec<serde_json::Value>,
) -> () {
    let state = state.borrow();
    let opoolref = state.borrow::<Rc<RefCell<Option<Pool<Any>>>>>();
    let opool = opoolref.borrow();
    if let Some(pool) = &(*opool) {
        let boundq: sqlx::query::Query<Any, sqlx::any::AnyArguments> =
            pars.into_iter()
                .fold(sqlx::query(&sqlq), |q, par| match par {
                    // TODO share code with query
                    Value::String(s) => q.bind(s),
                    Value::Bool(b) => q.bind(b),
                    Value::Number(x) => {
                        if Number::is_i64(&x) {
                            q.bind(x.as_i64())
                        } else {
                            q.bind(x.as_f64())
                        }
                    }
                    _ => panic!("unknonw argumen"),
                });
        let qres = boundq.execute(&(*pool)).await;
        match qres {
            Ok(_v) => return (),
            Err(e) => {
                dbg!(e);
                panic!("error in execute")
            }
        };
    } else {
        panic!("not connected to database")
    }
}

deno_core::extension!(
    database_extension,
    ops = [op_query, op_execute, op_connect_db],
    js = ["src/extensions/database.js"]
);

pub async fn connect_database(db_url: &str) -> Pool<Any> {
    sqlx::any::install_default_drivers();
    if !Sqlite::database_exists(db_url).await.unwrap_or(false) {
        println!("Creating database {}", db_url);
        match Sqlite::create_database(db_url).await {
            Ok(_) => println!("Create db success"),
            Err(error) => panic!("error: {}", error),
        }
    } else {
        println!("Database already exists");
    }
    let dbr = AnyPool::connect(db_url).await;
    match dbr {
        Ok(db) => db,
        Err(e) => panic!("error: {}", e),
    }
}
