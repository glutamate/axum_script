use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{MatchedPath, RawPathParams};
use axum::response::{IntoResponse, Response};
use axum::{
    extract::{Request, State},
    http::StatusCode,
    response::Html,
    routing::get,
    Json, Router,
};
use axum_login::{AuthUser, AuthnBackend, UserId};
use deno_core::op2;
use deno_core::serde_v8::from_v8;
use deno_core::JsRuntime;
use deno_core::{serde_v8::to_v8, OpState};
use serde_json::value::Number;
use serde_json::{json, Value};
use sqltojson::row_to_json;
use sqlx::Pool;
use sqlx::{migrate::MigrateDatabase, Any, AnyPool, Sqlite};
use std::cell::RefCell;
use std::collections::HashMap;
use std::env;
use std::rc::Rc;
use std::sync::RwLock;
use std::thread;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task;
use tokio::time::{sleep, Duration};

mod sqltojson;

static CACHE_VALUE_LOCK: RwLock<Value> = RwLock::new(Value::Null);

#[op2()]
fn op_route(state: &mut OpState, #[string] path: &str, #[global] router: v8::Global<v8::Function>) {
    let hmref = state.borrow::<Rc<RefCell<HashMap<String, v8::Global<v8::Function>>>>>();
    let mut routes = hmref.borrow_mut();
    routes.insert(String::from(path), router);
    ()
}

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

#[op2(async)]
async fn op_sleep(ms: u32) {
    sleep(Duration::from_millis(ms.into())).await;
}

deno_core::extension!(
    my_extension,
    ops = [
        op_route,
        op_query,
        op_execute,
        op_sleep,
        op_create_cache,
        op_flush_cache,
        op_get_cache_value,
        op_get_cache_subset_value,
        op_with_cache,
        op_connect_db
    ],
    js = ["src/runtime.js"]
);

fn get_init_dir() -> String {
    let args: Vec<String> = env::args().collect();
    return if args.len() < 2 {
        env::current_dir()
            .unwrap()
            .into_os_string()
            .into_string()
            .unwrap()
    } else {
        args[1].clone()
    };
}

async fn connect_database(db_url: &str) -> Pool<Any> {
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

struct JsRunnerInner {
    routes: HashMap<String, v8::Global<v8::Function>>,
    runtime: Rc<RefCell<JsRuntime>>,
    // db_pool: Pool<Sqlite>,
}

#[derive(Clone)]
struct JsRunner {
    inner: Rc<JsRunnerInner>,
}

impl std::ops::Deref for JsRunner {
    type Target = JsRunnerInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl JsRunner {
    async fn new(tx_req: Option<mpsc::Sender<RouteRequest>>) -> JsRunner {
        let dir = get_init_dir();
        let setup_path = [dir, String::from("setup.js")].concat();

        let init_module =
            deno_core::resolve_path(&setup_path, env::current_dir().unwrap().as_path()).unwrap();
        let mut js_runtime = deno_core::JsRuntime::new(deno_core::RuntimeOptions {
            module_loader: Some(Rc::new(deno_core::FsModuleLoader)),
            extensions: vec![my_extension::init_ops_and_esm()],
            ..Default::default()
        });
        // following https://github.com/DataDog/datadog-static-analyzer/blob/cde26f42f1cdbbeb09650403318234f277138bbd/crates/static-analysis-kernel/src/analysis/ddsa_lib/runtime.rs#L54
        let pool: Rc<RefCell<Option<Pool<Any>>>> = Rc::new(RefCell::new(None));

        let route_map: HashMap<String, v8::Global<v8::Function>> = HashMap::new();

        let hmref = Rc::new(RefCell::new(route_map));
        let txref = Rc::new(RefCell::new(tx_req));

        js_runtime.op_state().borrow_mut().put(Rc::clone(&pool));
        js_runtime.op_state().borrow_mut().put(Rc::clone(&hmref));
        js_runtime.op_state().borrow_mut().put(Rc::clone(&txref));

        let mod_id = js_runtime.load_main_es_module(&init_module).await;
        let result = js_runtime.mod_evaluate(mod_id.unwrap());
        js_runtime.run_event_loop(Default::default()).await.unwrap();
        result.await.unwrap();

        return JsRunner {
            inner: Rc::new(JsRunnerInner {
                routes: (*hmref.borrow()).clone(),
                runtime: Rc::new(RefCell::new(js_runtime)),
            }),
        };
    }

    async fn run_loop(&self, mut rx_req: mpsc::Receiver<RouteRequest>) {
        let local = task::LocalSet::new();
        local
            .run_until(async move {
                while let Some(req) = rx_req.recv().await {
                    let this = self.clone();
                    task::spawn_local(async move {
                        let response = this.run_route(&req).await;
                        if let Some(resp_chan) = req.response_channel {
                            resp_chan.send(response).unwrap();
                        }

                        // ...
                    });
                }
            })
            .await;
    }

    #[tokio::main(flavor = "current_thread")]
    async fn run_thread(tx_req: mpsc::Sender<RouteRequest>, rx_req: mpsc::Receiver<RouteRequest>) {
        let runner = JsRunner::new(Some(tx_req)).await;
        runner.run_loop(rx_req).await;
    }

    fn spawn_thread() -> mpsc::Sender<RouteRequest> {
        let (tx_req, rx_req) = mpsc::channel(128);
        let tx_req1 = tx_req.clone();
        thread::spawn(move || {
            JsRunner::run_thread(tx_req1, rx_req);
        });
        return tx_req;
    }

    async fn run_route_value(
        &self,
        req: &RouteRequest,
    ) -> Result<v8::Global<v8::Value>, Response<Body>> {
        let hm = &self.routes;

        if let Some(gf) = hm.get(&*(req.route_name)) {
            let func_res_promise = {
                let runtime = unsafe { &mut *self.runtime.as_ptr() };
                let args = {
                    let mut scope = &mut runtime.handle_scope();
                    let params = serde_json::Value::Object(req.route_args.clone());
                    let jsreq = json!({"params": params});
                    let v8_arg: v8::Local<v8::Value> = to_v8(&mut scope, jsreq).unwrap();

                    &[v8::Global::new(&mut *scope, v8_arg)]
                };

                runtime.call_with_args(gf, args)
            };

            let func_res0 = unsafe { &mut *self.runtime.as_ptr() }
                .with_event_loop_promise(func_res_promise, Default::default())
                .await;
            if let Err(e) = func_res0 {
                dbg!(e);
                return Err((StatusCode::INTERNAL_SERVER_ERROR, Html("Error")).into_response());
            }
            let func_res1 = func_res0.unwrap();

            return Ok(func_res1);
        } else {
            return Err((StatusCode::NOT_FOUND, Html("404 not found")).into_response());
        }
    }
    async fn run_route(&self, req: &RouteRequest) -> Response<Body> {
        let res = self.run_route_value(req).await;
        if req.route_name == "__create_cache" {
            let runtime = unsafe { &mut *self.runtime.as_ptr() };
            let scope = &mut runtime.handle_scope();
            let v8_val = v8::Local::new(scope, res.unwrap());
            let serde_val: Value = from_v8(scope, v8_val).unwrap();
            //save to global

            let mut cache = CACHE_VALUE_LOCK.write().unwrap();
            *cache = serde_val;

            return Html("").into_response();
        } else {
            match res {
                Ok(func_res1) => {
                    let runtime = unsafe { &mut *self.runtime.as_ptr() };
                    let scope = &mut runtime.handle_scope();
                    let func_res = func_res1.open(scope);

                    if func_res.is_string() {
                        let s = func_res
                            .to_string(scope)
                            .unwrap()
                            .to_rust_string_lossy(scope);
                        return Html(s).into_response();
                    } else {
                        let lres = v8::Local::new(scope, func_res1);
                        let res: serde_json::Map<String, Value> = from_v8(scope, lres).unwrap();
                        if res.contains_key("json") {
                            return annotate_response(&res, Json(res.get("json")).into_response());
                        }
                        if res.contains_key("html") {
                            let body: String =
                                serde_json::from_value(res.get("html").unwrap().clone()).unwrap();
                            return annotate_response(&res, Html(body).into_response());
                        }

                        return Html("").into_response();
                    }
                }
                Err(e) => e,
            }
        }
    }

    async fn populate_initial_cache(&self) {
        if self.inner.routes.contains_key("__create_cache") {
            //let (tx, _) = oneshot::channel();
            let req = RouteRequest {
                route_name: String::from("__create_cache"),
                response_channel: None,
                route_args: serde_json::Map::new(),
                //request: req,
            };
            self.run_route(&req).await;
        }
    }
}

fn annotate_response(
    resp_obj: &serde_json::Map<String, Value>,
    resp: Response<Body>,
) -> Response<Body> {
    let resp1 = if resp_obj.contains_key("status") {
        let code: u16 = serde_json::from_value(resp_obj.get("status").unwrap().clone()).unwrap();
        let scode = StatusCode::from_u16(code).unwrap();
        (scode, resp).into_response()
    } else {
        resp
    };
    return resp1;
}

struct RouteRequest {
    route_name: String,
    response_channel: Option<oneshot::Sender<Response<Body>>>,
    route_args: serde_json::Map<String, Value>,
    //request: Request,
}

#[derive(Clone)]
struct RouteState {
    tx_req: mpsc::Sender<RouteRequest>,
}
fn main() {
    let paths = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let runner = JsRunner::new(None).await;
            let routemap = runner.routes.clone();
            runner.populate_initial_cache().await;
            drop(runner);
            routemap.keys().cloned().collect::<Vec<_>>()
        });

    let paths = paths.iter();

    let axum = async {
        let tx_req = JsRunner::spawn_thread();

        print!("Starting server");
        let rstate = RouteState { tx_req };
        let app: Router = paths
            .fold(Router::new(), |router, path| {
                if path.starts_with("/") {
                    router.route(path, get(req_handler))
                } else {
                    router
                }
            })
            .with_state(rstate);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:4000")
            .await
            .unwrap();
        println!("listening on {}", listener.local_addr().unwrap());
        axum::serve(listener, app).await.unwrap();
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed building the Runtime")
        .block_on(axum);
}

async fn req_handler(
    State(state): State<RouteState>,
    match_path: MatchedPath,
    raw_params: RawPathParams,
    req: Request,
) -> Response<Body> {
    let path = match_path.as_str();
    let parvals =
        serde_json::Map::from_iter(raw_params.iter().map(|(k, v)| (String::from(k), v.into())));
    let (tx, rx) = oneshot::channel();
    let sendres = state
        .tx_req
        .send(RouteRequest {
            route_name: String::from(path),
            response_channel: Some(tx),
            route_args: parvals,
            //request: req,
        })
        .await;
    match sendres {
        Ok(_) => match rx.await {
            Ok(v) => v,
            Err(e) => {
                dbg!(e);
                return (StatusCode::INTERNAL_SERVER_ERROR, Html("Error")).into_response();
            }
        },
        Err(e) => {
            dbg!(e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Html("Error")).into_response();
        }
    }
}

#[derive(Debug, Clone)]
struct AuthConfiguration {
    id_field: String,
    pw_hash_field: Vec<u8>,
}

#[derive(Debug, Clone)]
struct User {
    id: String,
    pw_hash: Vec<u8>,
    raw: Value,
}
impl AuthUser for User {
    type Id = String;

    fn id(&self) -> Self::Id {
        self.id.clone()
    }

    fn session_auth_hash(&self) -> &[u8] {
        &self.pw_hash
    }
}

#[derive(Clone)]
struct Credentials {
    raw: Value,
}

#[derive(Clone)]
struct Backend {
    auth_config: AuthConfiguration,
    pool: Pool<Any>,
}

#[async_trait]
impl AuthnBackend for Backend {
    type User = User;
    type Credentials = Credentials;
    type Error = std::convert::Infallible;

    async fn authenticate(
        &self,
        creds: Self::Credentials,
    ) -> Result<Option<Self::User>, Self::Error> {
        let user: Option<Self::User> = sqlx::query_as("select * from users where username = ? ")
            .bind(creds.username)
            .fetch_optional(&self.pool)
            .await?;

        // Verifying the password is blocking and potentially slow, so we'll do so via
        // `spawn_blocking`.
        task::spawn_blocking(|| {
            // We're using password-based authentication--this works by comparing our form
            // input with an argon2 password hash.
            Ok(user.filter(|user| verify_password(creds.password, &user.password).is_ok()))
        })
        .await?
    }

    async fn get_user(&self, user_id: &UserId<Self>) -> Result<Option<Self::User>, Self::Error> {
        let user = sqlx::query_as("select * from users where id = ?")
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(user)
    }
}
