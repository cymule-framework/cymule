//! HTTP activation ingress for Cymule.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

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
use rusqlite::{Connection, ErrorCode, OptionalExtension, TransactionBehavior, params};
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
    Unavailable,
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

#[derive(Clone)]
struct DurableHttpState {
    database: Arc<PathBuf>,
    sender: mpsc::Sender<DurableIngress>,
    authorizer: Arc<dyn HttpActivationAuthorizer>,
}

struct DurableIngress {
    activation_id: String,
    response: oneshot::Sender<IngressOutcome>,
}

/// SQLite-backed HTTP signal source whose ingress and selected targets survive
/// process death.
pub struct SqliteHttpSignalDriver {
    connection: Connection,
    receiver: mpsc::Receiver<DurableIngress>,
    waiters: BTreeMap<String, Vec<oneshot::Sender<IngressOutcome>>>,
    waiter_count: usize,
    max_waiters: usize,
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

/// Build a production HTTP signal router over one durable `SQLite` spool.
///
/// The handler persists the exact request before waiting. It returns `202`
/// only after [`WaitSourceDriver::acknowledge`] marks the selected delivery as
/// committed. If the process dies first, the request fails at transport level
/// and an identical retry joins or observes the retained delivery.
pub fn durable_signal_router(
    path: impl AsRef<Path>,
    capacity: usize,
    authorizer: impl HttpActivationAuthorizer,
) -> DurableResult<(Router, SqliteHttpSignalDriver)> {
    if capacity == 0 || capacity > 65_536 {
        return Err(DurableError::Validation(
            "HTTP activation capacity must be 1..=65536".to_owned(),
        ));
    }
    let path = path.as_ref().to_path_buf();
    let connection = open_spool(&path, true)?;
    let (sender, receiver) = mpsc::channel(capacity);
    let router = Router::new()
        .route("/v1/signals", post(receive_durable_signal))
        .with_state(DurableHttpState {
            database: Arc::new(path),
            sender,
            authorizer: Arc::new(authorizer),
        });
    Ok((
        router,
        SqliteHttpSignalDriver {
            connection,
            receiver,
            waiters: BTreeMap::new(),
            waiter_count: 0,
            max_waiters: capacity,
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
        Ok(IngressOutcome::Unavailable) | Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn receive_durable_signal(
    State(state): State<DurableHttpState>,
    headers: HeaderMap,
    Json(request): Json<HttpSignalRequest>,
) -> impl IntoResponse {
    if validate_request(&request).is_err() {
        return StatusCode::BAD_REQUEST;
    }
    if !state.authorizer.authorize(&headers, &request) {
        return StatusCode::UNAUTHORIZED;
    }
    let database = Arc::clone(&state.database);
    let persisted_request = request.clone();
    let retained =
        match tokio::task::spawn_blocking(move || persist_request(&database, &persisted_request))
            .await
        {
            Ok(Ok(retained)) => retained,
            Ok(Err(DurableError::Conflict { .. })) => return StatusCode::CONFLICT,
            Ok(Err(_)) | Err(_) => return StatusCode::SERVICE_UNAVAILABLE,
        };
    if retained {
        return StatusCode::ACCEPTED;
    }
    let (response, completion) = oneshot::channel();
    if state
        .sender
        .try_send(DurableIngress {
            activation_id: request.activation_id,
            response,
        })
        .is_err()
    {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    match completion.await {
        Ok(IngressOutcome::Committed) => StatusCode::ACCEPTED,
        Ok(IngressOutcome::Conflict) => StatusCode::CONFLICT,
        Ok(IngressOutcome::Unavailable) | Err(_) => StatusCode::SERVICE_UNAVAILABLE,
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

impl SqliteHttpSignalDriver {
    fn drain_waiters(&mut self) -> DurableResult<()> {
        loop {
            let ingress = match self.receiver.try_recv() {
                Ok(ingress) => ingress,
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                    return Ok(());
                }
            };
            let acknowledged: Option<bool> = self
                .connection
                .query_row(
                    "SELECT acknowledged FROM cymule_http_signals WHERE activation_id = ?1",
                    [&ingress.activation_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sqlite_error)?;
            match acknowledged {
                Some(true) => {
                    let _ = ingress.response.send(IngressOutcome::Committed);
                }
                Some(false) if self.waiter_count < self.max_waiters => {
                    self.waiters
                        .entry(ingress.activation_id)
                        .or_default()
                        .push(ingress.response);
                    self.waiter_count += 1;
                }
                Some(false) | None => {
                    let _ = ingress.response.send(IngressOutcome::Unavailable);
                }
            }
        }
    }

    fn retained_targets(
        &self,
        activation_id: &str,
        selected: Option<Vec<u8>>,
        source: &WaitActivationSource,
        index: &ParkedWaitIndex,
        max_targets: usize,
    ) -> DurableResult<Option<BTreeSet<String>>> {
        if let Some(selected) = selected {
            let targets = cymule_core::decode_json(&selected)?;
            validate_targets(&targets, max_targets)?;
            return Ok(Some(targets));
        }
        let selection = index.select(source, max_targets)?;
        if selection.wait_ids.is_empty() {
            return Ok(None);
        }
        let bytes = cymule_core::canonical_bytes(&selection.wait_ids)?;
        self.connection
            .execute(
                "UPDATE cymule_http_signals SET selected_wait_ids = ?1
                 WHERE activation_id = ?2 AND selected_wait_ids IS NULL
                   AND acknowledged = 0",
                params![bytes, activation_id],
            )
            .map_err(contention)?;
        let retained: Vec<u8> = self
            .connection
            .query_row(
                "SELECT selected_wait_ids FROM cymule_http_signals
                 WHERE activation_id = ?1 AND acknowledged = 0",
                [activation_id],
                |row| row.get(0),
            )
            .map_err(sqlite_error)?;
        let targets = cymule_core::decode_json(&retained)?;
        validate_targets(&targets, max_targets)?;
        Ok(Some(targets))
    }
}

impl WaitSourceDriver for SqliteHttpSignalDriver {
    fn receive(
        &mut self,
        index: &ParkedWaitIndex,
        max_targets: usize,
    ) -> DurableResult<Option<WaitDelivery>> {
        self.drain_waiters()?;
        let rows = {
            let mut statement = self
                .connection
                .prepare(
                    "SELECT activation_id, signal_key, value_json, selected_wait_ids
                     FROM cymule_http_signals WHERE acknowledged = 0
                     ORDER BY activation_id LIMIT 1024",
                )
                .map_err(sqlite_error)?;
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, Option<Vec<u8>>>(3)?,
                    ))
                })
                .map_err(sqlite_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sqlite_error)?
        };
        for (activation_id, key, value, selected) in rows {
            let source = WaitActivationSource::Signal { key };
            let Some(wait_ids) =
                self.retained_targets(&activation_id, selected, &source, index, max_targets)?
            else {
                continue;
            };
            return Ok(Some(WaitDelivery {
                activation_id,
                source,
                wait_ids,
                value: cymule_core::decode_json(&value)?,
            }));
        }
        Ok(None)
    }

    fn acknowledge(&mut self, activation_id: &str) -> DurableResult<()> {
        validate_identity("activation", activation_id)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(contention)?;
        let existing: Option<(bool, Option<Vec<u8>>)> = transaction
            .query_row(
                "SELECT acknowledged, selected_wait_ids FROM cymule_http_signals
                 WHERE activation_id = ?1",
                [activation_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sqlite_error)?;
        let Some((acknowledged, selected)) = existing else {
            return Err(DurableError::NotFound(format!(
                "HTTP activation {activation_id} is missing"
            )));
        };
        if selected.is_none() {
            return Err(DurableError::Validation(format!(
                "HTTP activation {activation_id} has not selected durable targets"
            )));
        }
        if !acknowledged {
            transaction
                .execute(
                    "UPDATE cymule_http_signals SET acknowledged = 1
                     WHERE activation_id = ?1 AND acknowledged = 0",
                    [activation_id],
                )
                .map_err(sqlite_error)?;
        }
        transaction.commit().map_err(sqlite_error)?;
        if let Some(waiters) = self.waiters.remove(activation_id) {
            self.waiter_count = self.waiter_count.saturating_sub(waiters.len());
            for waiter in waiters {
                let _ = waiter.send(IngressOutcome::Committed);
            }
        }
        Ok(())
    }
}

fn validate_request(request: &HttpSignalRequest) -> DurableResult<()> {
    for (kind, identity) in [
        ("activation", request.activation_id.as_str()),
        ("signal", request.key.as_str()),
    ] {
        validate_identity(kind, identity)?;
    }
    Ok(())
}

fn request_digest(request: &HttpSignalRequest) -> DurableResult<String> {
    cymule_core::canonical_digest(request).map_err(DurableError::from)
}

fn validate_identity(kind: &str, identity: &str) -> DurableResult<()> {
    if identity.is_empty() || identity.len() > 512 || identity.chars().any(char::is_control) {
        return Err(DurableError::Validation(format!(
            "HTTP {kind} identity must contain 1..=512 printable characters"
        )));
    }
    Ok(())
}

fn validate_targets(wait_ids: &BTreeSet<String>, max_targets: usize) -> DurableResult<()> {
    if max_targets == 0 || wait_ids.is_empty() || wait_ids.len() > max_targets {
        return Err(DurableError::Validation(format!(
            "retained HTTP delivery has {} targets outside requested bound {max_targets}",
            wait_ids.len()
        )));
    }
    Ok(())
}

fn open_spool(path: &Path, initialize: bool) -> DurableResult<Connection> {
    let connection = Connection::open(path).map_err(sqlite_error)?;
    connection
        .busy_timeout(Duration::ZERO)
        .map_err(sqlite_error)?;
    if initialize {
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(sqlite_error)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(sqlite_error)?;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS cymule_http_signals (
                    activation_id TEXT PRIMARY KEY NOT NULL,
                    signal_key TEXT NOT NULL,
                    value_json BLOB NOT NULL,
                    request_digest TEXT NOT NULL,
                    selected_wait_ids BLOB,
                    acknowledged INTEGER NOT NULL DEFAULT 0
                ) STRICT;
                CREATE INDEX IF NOT EXISTS cymule_http_signals_pending
                    ON cymule_http_signals(acknowledged, activation_id);",
            )
            .map_err(sqlite_error)?;
    }
    Ok(connection)
}

fn persist_request(path: &Path, request: &HttpSignalRequest) -> DurableResult<bool> {
    let connection = open_spool(path, false)?;
    let digest = request_digest(request)?;
    let value = cymule_core::canonical_bytes(&request.value)?;
    let transaction = connection.unchecked_transaction().map_err(contention)?;
    let existing: Option<(String, bool)> = transaction
        .query_row(
            "SELECT request_digest, acknowledged FROM cymule_http_signals
             WHERE activation_id = ?1",
            [&request.activation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sqlite_error)?;
    if let Some((existing, acknowledged)) = existing {
        if existing != digest {
            return Err(DurableError::Conflict {
                expected: Some(request.activation_id.clone()),
                current: Some("HTTP-activation-identity-reused".to_owned()),
            });
        }
        transaction.commit().map_err(sqlite_error)?;
        return Ok(acknowledged);
    }
    transaction
        .execute(
            "INSERT INTO cymule_http_signals(
                activation_id, signal_key, value_json, request_digest,
                selected_wait_ids, acknowledged
             ) VALUES (?1, ?2, ?3, ?4, NULL, 0)",
            params![request.activation_id, request.key, value, digest],
        )
        .map_err(contention)?;
    transaction.commit().map_err(sqlite_error)?;
    Ok(false)
}

fn contention(error: rusqlite::Error) -> DurableError {
    match &error {
        rusqlite::Error::SqliteFailure(failure, _)
            if matches!(
                failure.code,
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
            ) =>
        {
            DurableError::Conflict {
                expected: Some("sqlite-HTTP-writer-available".to_owned()),
                current: Some("sqlite-HTTP-writer-active".to_owned()),
            }
        }
        _ => sqlite_error(error),
    }
}

fn sqlite_error(error: impl std::fmt::Display) -> DurableError {
    DurableError::Substrate(error.to_string())
}
