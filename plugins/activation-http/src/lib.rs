//! HTTP activation ingress for Cymule.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use cymule_durable::{
    DurableError, DurableResult, ParkedWaitIndex, WaitActivationSource, WaitDelivery,
    WaitSourceDriver,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

/// Frozen HTTP signal request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpSignalRequest {
    /// Stable producer delivery identity.
    pub activation_id: String,
    /// Plan-declared signal key.
    pub key: String,
    /// Typed signal value.
    pub value: Value,
}

/// Authorization hook evaluated before a signal enters the bounded channel.
pub trait HttpActivationAuthorizer: Send + Sync + 'static {
    /// Whether headers authorize this exact signal observation.
    fn authorize(&self, headers: &HeaderMap, request: &HttpSignalRequest) -> bool;
}

/// Explicit local/test authorizer that permits every request.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

impl HttpActivationAuthorizer for AllowAll {
    fn authorize(&self, _headers: &HeaderMap, _request: &HttpSignalRequest) -> bool {
        true
    }
}

#[derive(Clone)]
struct HttpState {
    sender: mpsc::Sender<Ingress>,
    authorizer: Arc<dyn HttpActivationAuthorizer>,
}

struct Ingress {
    request: HttpSignalRequest,
    response: oneshot::Sender<IngressOutcome>,
}

enum IngressOutcome {
    Committed,
    Conflict,
}

struct Pending {
    request: HttpSignalRequest,
    response: oneshot::Sender<IngressOutcome>,
    digest: String,
    wait_ids: Option<BTreeSet<String>>,
}

/// Driver half of one bounded HTTP ingress channel.
pub struct HttpSignalDriver {
    receiver: mpsc::Receiver<Ingress>,
    pending: Option<Pending>,
    acknowledged: BTreeMap<String, String>,
}

/// Build a signal router and its single-consumer driver.
pub fn signal_router(
    capacity: usize,
    authorizer: impl HttpActivationAuthorizer,
) -> DurableResult<(Router, HttpSignalDriver)> {
    if capacity == 0 || capacity > 65_536 {
        return Err(DurableError::Validation(
            "HTTP activation capacity must be 1..=65536".to_owned(),
        ));
    }
    let (sender, receiver) = mpsc::channel(capacity);
    let state = HttpState {
        sender,
        authorizer: Arc::new(authorizer),
    };
    let router = Router::new()
        .route("/v1/signals", post(receive_signal))
        .with_state(state);
    Ok((
        router,
        HttpSignalDriver {
            receiver,
            pending: None,
            acknowledged: BTreeMap::new(),
        },
    ))
}

async fn receive_signal(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(request): Json<HttpSignalRequest>,
) -> impl IntoResponse {
    if validate_request(&request).is_err() {
        return StatusCode::BAD_REQUEST;
    }
    if !state.authorizer.authorize(&headers, &request) {
        return StatusCode::UNAUTHORIZED;
    }
    let (response, completion) = oneshot::channel();
    if state
        .sender
        .try_send(Ingress { request, response })
        .is_err()
    {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    match completion.await {
        Ok(IngressOutcome::Committed) => StatusCode::ACCEPTED,
        Ok(IngressOutcome::Conflict) => StatusCode::CONFLICT,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

impl WaitSourceDriver for HttpSignalDriver {
    fn receive(
        &mut self,
        index: &ParkedWaitIndex,
        max_targets: usize,
    ) -> DurableResult<Option<WaitDelivery>> {
        loop {
            if self.pending.is_none() {
                let ingress = match self.receiver.try_recv() {
                    Ok(ingress) => ingress,
                    Err(
                        mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected,
                    ) => return Ok(None),
                };
                let digest = request_digest(&ingress.request)?;
                if let Some(existing) = self.acknowledged.get(&ingress.request.activation_id) {
                    let _ = ingress.response.send(if existing == &digest {
                        IngressOutcome::Committed
                    } else {
                        IngressOutcome::Conflict
                    });
                    continue;
                }
                self.pending = Some(Pending {
                    request: ingress.request,
                    response: ingress.response,
                    digest,
                    wait_ids: None,
                });
            }
            let pending = self.pending.as_mut().expect("pending ingress exists");
            let source = WaitActivationSource::Signal {
                key: pending.request.key.clone(),
            };
            if pending.wait_ids.is_none() {
                let selection = index.select(&source, max_targets)?;
                if selection.wait_ids.is_empty() {
                    return Ok(None);
                }
                pending.wait_ids = Some(selection.wait_ids);
            }
            return Ok(Some(WaitDelivery {
                activation_id: pending.request.activation_id.clone(),
                source,
                wait_ids: pending.wait_ids.clone().expect("targets selected"),
                value: pending.request.value.clone(),
            }));
        }
    }

    fn acknowledge(&mut self, activation_id: &str) -> DurableResult<()> {
        let Some(pending) = self.pending.take() else {
            return if self.acknowledged.contains_key(activation_id) {
                Ok(())
            } else {
                Err(DurableError::NotFound(format!(
                    "HTTP activation {activation_id} is not pending"
                )))
            };
        };
        if pending.request.activation_id != activation_id {
            let current = pending.request.activation_id.clone();
            self.pending = Some(pending);
            return Err(DurableError::Conflict {
                expected: Some(activation_id.to_owned()),
                current: Some(current),
            });
        }
        self.acknowledged
            .insert(activation_id.to_owned(), pending.digest);
        let _ = pending.response.send(IngressOutcome::Committed);
        Ok(())
    }
}

fn validate_request(request: &HttpSignalRequest) -> DurableResult<()> {
    for (kind, identity) in [
        ("activation", request.activation_id.as_str()),
        ("signal", request.key.as_str()),
    ] {
        if identity.is_empty() || identity.len() > 512 || identity.chars().any(char::is_control) {
            return Err(DurableError::Validation(format!(
                "HTTP {kind} identity must contain 1..=512 printable characters"
            )));
        }
    }
    Ok(())
}

fn request_digest(request: &HttpSignalRequest) -> DurableResult<String> {
    cymule_core::canonical_digest(request).map_err(DurableError::from)
}
