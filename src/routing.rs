use axum::body::Body;
use axum::response::Response;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub struct RouteRequest {
    pub route_name: String,
    pub response_channel: Option<oneshot::Sender<Response<Body>>>,
    pub route_args: serde_json::Map<String, Value>,
    //request: Request,
}

#[derive(Clone)]
pub struct RouteState {
    pub tx_req: mpsc::Sender<RouteRequest>,
}
