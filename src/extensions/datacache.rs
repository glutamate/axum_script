use crate::routing::RouteRequest;
use deno_core::op2;
use deno_core::serde_v8::from_v8;
use deno_core::{serde_v8::to_v8, OpState};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;
use std::env;
use std::rc::Rc;
use std::sync::RwLock;
use tokio::sync::mpsc;

static CACHE_VALUE_LOCK: RwLock<Value> = RwLock::new(Value::Null);

#[op2()]
#[serde]
fn op_get_cache_value() -> serde_json::Value {
    let r1 = CACHE_VALUE_LOCK.read().unwrap();
    return (*r1).clone(); //TODO this is bad
}

#[op2()]
#[serde]
fn op_get_cache_subset_value(#[serde] subset: serde_json::Value) -> serde_json::Value {
    //fn op_get_cache_subset_value(subset: serde_json::Value) -> Value {
    let r1 = CACHE_VALUE_LOCK.read().unwrap();
    match (subset, &(*r1)) {
        (Value::String(key), Value::Object(o)) => o.get(&key).unwrap_or(&Value::Null).clone(),
        (Value::Array(keys), Value::Object(o)) => {
            let mut mp = serde_json::Map::new();
            keys.into_iter().for_each(|vkey| match vkey {
                Value::String(key) => {
                    mp.insert(key.clone(), o.get(&key).unwrap_or(&Value::Null).clone());
                    return ();
                }
                _ => {
                    panic!("invalid key");
                }
            });
            Value::Object(mp)
        }
        _ => panic!("unknown subset"),
    }
}

#[op2()]
fn op_create_cache(state: &mut OpState, #[global] create_cache_fn: v8::Global<v8::Function>) -> () {
    let hmref = state.borrow::<Rc<RefCell<HashMap<String, v8::Global<v8::Function>>>>>();
    let mut routes = hmref.borrow_mut();
    routes.insert(String::from("__create_cache"), create_cache_fn);
    return ();
    //    return rows.len().try_into().unwrap();
}

#[op2()]
#[serde]
fn op_with_cache<'s>(
    scope: &mut v8::HandleScope<'s>,
    #[global] gxformer: v8::Global<v8::Function>,
) -> serde_json::Value {
    let r1 = CACHE_VALUE_LOCK.read().unwrap();
    let xformer = gxformer.open(scope);
    let v8_val = to_v8(scope, &(*r1)).unwrap();
    let fres = xformer.call(scope, v8_val, &[v8_val]);
    match fres {
        Some(v) => {
            return from_v8(scope, v).unwrap();
        }
        None => {
            panic!("withcache function error");
        }
    }
    //    return rows.len().try_into().unwrap();
}

#[op2(async)]
async fn op_flush_cache(state: Rc<RefCell<OpState>>) -> () {
    let state = state.borrow();
    let txref = state.borrow::<Rc<RefCell<Option<mpsc::Sender<RouteRequest>>>>>();
    let otxreq = txref.borrow_mut();
    //let (tx, rx) = oneshot::channel();
    if let Some(txreq) = otxreq.as_ref() {
        let sendres = txreq
            .send(RouteRequest {
                route_name: String::from("__create_cache"),
                response_channel: None,
                route_args: serde_json::Map::new(),
                //request: req,
            })
            .await;

        match sendres {
            Ok(_) => (),
            Err(e) => {
                panic!("Send Error: {}", e);
            }
        }
        //TODO await response before returning
        /*match rx.await {
            Ok(_v) => return (),
            Err(_e) => {
                panic!("error in flush cache")
            }
        };*/
    }
}

deno_core::extension!(
    datacache_extension,
    ops = [
        op_create_cache,
        op_flush_cache,
        op_get_cache_value,
        op_get_cache_subset_value,
        op_with_cache,
    ],
    js = ["src/extensions/datacache.js"]
);

pub fn set_data_cache(serde_val: Value) {
    let mut cache = CACHE_VALUE_LOCK.write().unwrap();
    *cache = serde_val;
}
