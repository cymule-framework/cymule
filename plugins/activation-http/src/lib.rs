//! HTTP activation ingress for Cymule.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::DefaultBodyLimit;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::post;
use cymule_durable::{
    DurableError, DurableResult, ParkedWaitView, SignalKeyPageOutcome, WaitDelivery,
    WaitSourceCursor, WaitSourceDriver,
};
use cymule_durable_protocol::WaitActivationSource;
use rusqlite::{Connection, ErrorCode, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::oneshot;

const HTTP_SIGNAL_KEY_SCAN_LIMIT: usize = 1_024;
const HTTP_SIGNAL_BODY_LIMIT: usize = 2 * 1024 * 1024;
const HTTP_ACKNOWLEDGEMENT_WAIT_WINDOW: Duration = Duration::from_secs(30);

/// Exact physical generation accepted by the durable HTTP signal spool.
pub const HTTP_SPOOL_SCHEMA_VERSION: &str = "cymule.activation-http-spool/1";
/// Stable code returned before mutation for every unsupported HTTP spool generation.
pub const UNSUPPORTED_STORE_GENERATION_CODE: &str = "unsupported_store_generation";

const HTTP_SPOOL_SCHEMA: [(&str, &str, &str, &str); 4] = [
    (
        "table",
        "cymule_http_spool_meta",
        "cymule_http_spool_meta",
        "CREATE TABLE cymule_http_spool_meta (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            schema_version TEXT NOT NULL
         ) STRICT",
    ),
    (
        "table",
        "cymule_http_signals",
        "cymule_http_signals",
        "CREATE TABLE cymule_http_signals (
            activation_id TEXT PRIMARY KEY NOT NULL,
            signal_key TEXT NOT NULL,
            value_json BLOB NOT NULL,
            request_digest TEXT NOT NULL,
            selected_wait_ids BLOB,
            acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(acknowledged IN (0, 1))
         ) STRICT",
    ),
    (
        "index",
        "cymule_http_signals_pending",
        "cymule_http_signals",
        "CREATE INDEX cymule_http_signals_pending
            ON cymule_http_signals(acknowledged, activation_id)",
    ),
    (
        "index",
        "cymule_http_signals_matching",
        "cymule_http_signals",
        "CREATE INDEX cymule_http_signals_matching
            ON cymule_http_signals(acknowledged, signal_key, activation_id)",
    ),
];

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

/// Authorization hook evaluated before a signal enters the bounded channel or
/// durable spool.
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
struct DurableHttpState {
    database: Arc<PathBuf>,
    waiters: Arc<Mutex<DurableWaiterRegistry>>,
    authorizer: Arc<dyn HttpActivationAuthorizer>,
    ingress_barrier: Option<DurableIngressBarrier>,
}

#[derive(Clone)]
struct DurableIngressBarrier {
    persisted: Arc<tokio::sync::Barrier>,
    release: Arc<tokio::sync::Barrier>,
}

struct DurableWaiterRegistry {
    waiters: BTreeMap<String, BTreeMap<u64, oneshot::Sender<()>>>,
    waiter_count: usize,
    max_waiters: usize,
    next_waiter_id: u64,
}

struct DurableWaiter {
    registry: Arc<Mutex<DurableWaiterRegistry>>,
    activation_id: String,
    waiter_id: u64,
    completion: oneshot::Receiver<()>,
}

struct StoredSignal {
    activation_id: String,
    key: String,
    value: Vec<u8>,
    selected: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistRequestOutcome {
    Pending,
    Acknowledged,
    IdentityConflict,
}

/// SQLite-backed HTTP signal source whose ingress and selected targets survive
/// process death.
pub struct SqliteHttpSignalDriver {
    connection: Connection,
    waiters: Arc<Mutex<DurableWaiterRegistry>>,
    signal_cursor: Option<WaitSourceCursor>,
}

/// Build a production HTTP signal router over one durable `SQLite` spool.
///
/// The handler persists the exact request before waiting. It returns `202`
/// only after [`WaitSourceDriver::acknowledge`] marks the selected delivery as
/// committed. If the process dies first, the request fails at transport level
/// and an identical retry joins or observes the retained delivery.
///
/// # Errors
///
/// Returns an error when `capacity` is invalid or when the durable `SQLite` spool
/// cannot be opened or validated.
pub fn durable_signal_router(
    path: impl AsRef<Path>,
    capacity: usize,
    authorizer: impl HttpActivationAuthorizer,
) -> DurableResult<(Router, SqliteHttpSignalDriver)> {
    durable_signal_router_inner(path, capacity, authorizer, None)
}

fn durable_signal_router_inner(
    path: impl AsRef<Path>,
    capacity: usize,
    authorizer: impl HttpActivationAuthorizer,
    ingress_barrier: Option<DurableIngressBarrier>,
) -> DurableResult<(Router, SqliteHttpSignalDriver)> {
    if capacity == 0 || capacity > 65_536 {
        return Err(DurableError::Validation(
            "HTTP activation capacity must be 1..=65536".to_owned(),
        ));
    }
    let path = path.as_ref().to_path_buf();
    let connection = open_spool(&path, true)?;
    let waiters = Arc::new(Mutex::new(DurableWaiterRegistry {
        waiters: BTreeMap::new(),
        waiter_count: 0,
        max_waiters: capacity,
        next_waiter_id: 0,
    }));
    let router = Router::new()
        .route("/v1/signals", post(receive_durable_signal))
        .layer(DefaultBodyLimit::max(HTTP_SIGNAL_BODY_LIMIT))
        .with_state(DurableHttpState {
            database: Arc::new(path),
            waiters: Arc::clone(&waiters),
            authorizer: Arc::new(authorizer),
            ingress_barrier,
        });
    Ok((
        router,
        SqliteHttpSignalDriver {
            connection,
            waiters,
            signal_cursor: None,
        },
    ))
}

async fn receive_durable_signal(
    State(state): State<DurableHttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let request = match decode_request(&headers, &body) {
        Ok(request) => request,
        Err(status) => return status,
    };
    if validate_request(&request).is_err() {
        return StatusCode::BAD_REQUEST;
    }
    if !state.authorizer.authorize(&headers, &request) {
        return StatusCode::UNAUTHORIZED;
    }
    let database = Arc::clone(&state.database);
    let persisted_request = request.clone();
    let Ok(Ok(persisted)) =
        tokio::task::spawn_blocking(move || persist_request(&database, &persisted_request)).await
    else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };
    match persisted {
        PersistRequestOutcome::Acknowledged => return StatusCode::ACCEPTED,
        PersistRequestOutcome::IdentityConflict => return StatusCode::CONFLICT,
        PersistRequestOutcome::Pending => {}
    }

    if let Some(barrier) = &state.ingress_barrier {
        barrier.persisted.wait().await;
        barrier.release.wait().await;
    }

    let activation_id = request.activation_id;
    let Some(mut waiter) = register_waiter(&state.waiters, &activation_id) else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };
    match read_acknowledged_async(Arc::clone(&state.database), activation_id.clone()).await {
        Ok(true) => StatusCode::ACCEPTED,
        Ok(false) => {
            bounded_acknowledgement_recheck(
                state.database,
                activation_id,
                &mut waiter,
                HTTP_ACKNOWLEDGEMENT_WAIT_WINDOW,
            )
            .await
        }
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn bounded_acknowledgement_recheck(
    database: Arc<PathBuf>,
    activation_id: String,
    waiter: &mut DurableWaiter,
    wait_window: Duration,
) -> StatusCode {
    // Process-local notification is only a latency hint. A different process
    // can commit the SQLite acknowledgement without owning this waiter, so the
    // fixed wait window also wakes exactly one durable readback. An unconfirmed
    // request returns 503 and relies on the producer's identical-ID retry
    // instead of starting a polling loop.
    let _ = tokio::time::timeout(wait_window, waiter.notified()).await;
    match read_acknowledged_async(database, activation_id).await {
        Ok(true) => StatusCode::ACCEPTED,
        Ok(false) | Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

impl SqliteHttpSignalDriver {
    fn retained_targets(
        &self,
        activation_id: &str,
        selected: Option<Vec<u8>>,
        source: &WaitActivationSource,
        view: &mut dyn ParkedWaitView,
        max_targets: usize,
    ) -> DurableResult<Option<BTreeSet<String>>> {
        if let Some(selected) = selected {
            let targets = cymule_core::decode_json(&selected)?;
            validate_targets(&targets, max_targets)?;
            return Ok(Some(targets));
        }
        let selection = view.select(source, max_targets)?;
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
            .map_err(contention)?;
        let targets = cymule_core::decode_json(&retained)?;
        validate_targets(&targets, max_targets)?;
        Ok(Some(targets))
    }
}

impl WaitSourceDriver for SqliteHttpSignalDriver {
    fn receive(
        &mut self,
        view: &mut dyn ParkedWaitView,
        max_targets: usize,
    ) -> DurableResult<Option<WaitDelivery>> {
        let retained: Option<StoredSignal> = self
            .connection
            .query_row(
                "SELECT activation_id, signal_key, value_json, selected_wait_ids
                 FROM cymule_http_signals
                 WHERE acknowledged = 0 AND selected_wait_ids IS NOT NULL
                 ORDER BY activation_id LIMIT 1",
                [],
                |row| {
                    Ok(StoredSignal {
                        activation_id: row.get(0)?,
                        key: row.get(1)?,
                        value: row.get(2)?,
                        selected: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(contention)?;
        if let Some(retained) = retained {
            let source = WaitActivationSource::Signal { key: retained.key };
            let wait_ids = self
                .retained_targets(
                    &retained.activation_id,
                    retained.selected,
                    &source,
                    view,
                    max_targets,
                )?
                .ok_or_else(|| {
                    DurableError::Validation(format!(
                        "retained HTTP activation {} lost its selected targets",
                        retained.activation_id
                    ))
                })?;
            return Ok(Some(WaitDelivery {
                activation_id: retained.activation_id,
                source,
                wait_ids,
                value: cymule_core::decode_json(&retained.value)?,
            }));
        }

        for _ in 0..HTTP_SIGNAL_KEY_SCAN_LIMIT {
            let page = match view.signal_key_page(self.signal_cursor.as_ref(), 1)? {
                SignalKeyPageOutcome::Page(page) => page,
                SignalKeyPageOutcome::Stale { .. } => {
                    self.signal_cursor = None;
                    match view.signal_key_page(None, 1)? {
                        SignalKeyPageOutcome::Page(page) => page,
                        SignalKeyPageOutcome::Stale { .. } => {
                            return Err(DurableError::Integrity {
                                code: "wait_source_cursor_reset_stale".to_owned(),
                                message: "wait-source view rejected an empty cursor as stale"
                                    .to_owned(),
                            });
                        }
                    }
                }
            };
            let exhausted = page.remaining == 0;
            self.signal_cursor = page.next_cursor;
            let Some(key) = page.keys.into_iter().next() else {
                return Ok(None);
            };
            let row: Option<(String, Vec<u8>)> = self
                .connection
                .query_row(
                    "SELECT activation_id, value_json
                     FROM cymule_http_signals
                     WHERE acknowledged = 0 AND selected_wait_ids IS NULL
                       AND signal_key = ?1
                     ORDER BY activation_id LIMIT 1",
                    [&key],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(contention)?;
            let Some((activation_id, value)) = row else {
                if exhausted {
                    return Ok(None);
                }
                continue;
            };
            let source = WaitActivationSource::Signal { key };
            let wait_ids = self
                .retained_targets(&activation_id, None, &source, view, max_targets)?
                .ok_or_else(|| {
                    DurableError::Validation(format!(
                        "indexed HTTP activation {activation_id} has no selectable target"
                    ))
                })?;
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
            .map_err(contention)?;
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
                .map_err(contention)?;
        }
        transaction.commit().map_err(contention)?;
        notify_waiters(&self.waiters, activation_id);
        Ok(())
    }
}

impl DurableWaiter {
    async fn notified(&mut self) -> Result<(), ()> {
        (&mut self.completion).await.map_err(|_| ())
    }
}

impl Drop for DurableWaiter {
    fn drop(&mut self) {
        unregister_waiter(&self.registry, &self.activation_id, self.waiter_id);
    }
}

fn register_waiter(
    registry: &Arc<Mutex<DurableWaiterRegistry>>,
    activation_id: &str,
) -> Option<DurableWaiter> {
    let mut retained = registry
        .lock()
        .expect("HTTP waiter registry mutex poisoned");
    if retained.waiter_count >= retained.max_waiters {
        return None;
    }
    let waiter_id = retained.next_waiter_id;
    retained.next_waiter_id = retained.next_waiter_id.checked_add(1)?;
    let (response, completion) = oneshot::channel();
    retained
        .waiters
        .entry(activation_id.to_owned())
        .or_default()
        .insert(waiter_id, response);
    retained.waiter_count += 1;
    drop(retained);
    Some(DurableWaiter {
        registry: Arc::clone(registry),
        activation_id: activation_id.to_owned(),
        waiter_id,
        completion,
    })
}

fn unregister_waiter(
    registry: &Arc<Mutex<DurableWaiterRegistry>>,
    activation_id: &str,
    waiter_id: u64,
) {
    let mut retained = registry
        .lock()
        .expect("HTTP waiter registry mutex poisoned");
    let removed = retained
        .waiters
        .get_mut(activation_id)
        .and_then(|waiters| waiters.remove(&waiter_id))
        .is_some();
    if retained
        .waiters
        .get(activation_id)
        .is_some_and(BTreeMap::is_empty)
    {
        retained.waiters.remove(activation_id);
    }
    if removed {
        retained.waiter_count -= 1;
    }
}

fn notify_waiters(registry: &Arc<Mutex<DurableWaiterRegistry>>, activation_id: &str) {
    let waiters = {
        let mut retained = registry
            .lock()
            .expect("HTTP waiter registry mutex poisoned");
        let waiters = retained.waiters.remove(activation_id).unwrap_or_default();
        retained.waiter_count -= waiters.len();
        waiters
    };
    for (_, waiter) in waiters {
        let _ = waiter.send(());
    }
}

async fn read_acknowledged_async(
    database: Arc<PathBuf>,
    activation_id: String,
) -> DurableResult<bool> {
    tokio::task::spawn_blocking(move || read_acknowledged(&database, &activation_id))
        .await
        .map_err(|error| DurableError::Substrate {
            code: "http_acknowledgement_read_task_failed".to_owned(),
            message: error.to_string(),
        })?
}

fn read_acknowledged(path: &Path, activation_id: &str) -> DurableResult<bool> {
    let connection = open_spool(path, false)?;
    connection
        .query_row(
            "SELECT acknowledged FROM cymule_http_signals WHERE activation_id = ?1",
            [activation_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(contention)?
        .ok_or_else(|| {
            DurableError::NotFound(format!(
                "HTTP activation {activation_id} disappeared before acknowledgement"
            ))
        })
}

fn decode_request(headers: &HeaderMap, body: &[u8]) -> Result<HttpSignalRequest, StatusCode> {
    if !has_json_content_type(headers) {
        return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    cymule_core::decode_json(body).map_err(|_| StatusCode::BAD_REQUEST)
}

fn has_json_content_type(headers: &HeaderMap) -> bool {
    let Some(content_type) = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
    else {
        return false;
    };
    let content_type = content_type.to_ascii_lowercase();
    content_type == "application/json"
        || content_type
            .strip_prefix("application/")
            .is_some_and(|subtype| subtype.ends_with("+json"))
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
    cymule_core::validate_identity(&format!("HTTP {kind} identity"), identity).map_err(Into::into)
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

fn open_spool(path: &Path, allow_initialize: bool) -> DurableResult<Connection> {
    let mut flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    if allow_initialize {
        flags |= OpenFlags::SQLITE_OPEN_CREATE;
    }
    let mut connection = Connection::open_with_flags(path, flags).map_err(contention)?;
    require_file_backed_http_spool(&connection)?;
    connection
        .busy_timeout(Duration::ZERO)
        .map_err(contention)?;
    if allow_initialize {
        initialize_or_require_http_spool(&mut connection)?;
    } else {
        require_current_http_spool(&connection)?;
    }
    configure_http_spool_connection(&connection)?;
    Ok(connection)
}

fn require_file_backed_http_spool(connection: &Connection) -> DurableResult<()> {
    if connection.path().is_none_or(str::is_empty) {
        return Err(DurableError::Validation(
            "HTTP SQLite spool must be file-backed".to_owned(),
        ));
    }
    Ok(())
}

fn initialize_or_require_http_spool(connection: &mut Connection) -> DurableResult<()> {
    if sqlite_schema_objects(connection)?.is_empty() {
        initialize_empty_http_spool(connection)?;
    } else {
        require_current_http_spool(connection)?;
    }
    Ok(())
}

fn initialize_empty_http_spool(connection: &mut Connection) -> DurableResult<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(contention)?;
    if sqlite_schema_objects(&transaction)?.is_empty() {
        for (_, _, _, ddl) in HTTP_SPOOL_SCHEMA {
            transaction.execute_batch(ddl).map_err(contention)?;
        }
        transaction
            .execute(
                "INSERT INTO cymule_http_spool_meta(singleton, schema_version) VALUES (1, ?1)",
                [HTTP_SPOOL_SCHEMA_VERSION],
            )
            .map_err(contention)?;
        require_current_http_spool(&transaction)?;
    } else {
        require_current_http_spool(&transaction)?;
    }
    transaction.commit().map_err(contention)
}

fn require_current_http_spool(connection: &Connection) -> DurableResult<()> {
    let observed = sqlite_schema_objects(connection)?;
    if observed.len() != HTTP_SPOOL_SCHEMA.len() {
        return Err(unsupported_generation(&format!(
            "HTTP SQLite object set is not the exact {HTTP_SPOOL_SCHEMA_VERSION} generation"
        )));
    }
    for (expected_kind, expected_name, expected_table, expected_ddl) in HTTP_SPOOL_SCHEMA {
        let expected_ddl = normalize_ddl(expected_ddl);
        let observed_ddl = observed
            .iter()
            .find(|(kind, name, table, _)| {
                kind == expected_kind && name == expected_name && table == expected_table
            })
            .map(|(_, _, _, ddl)| normalize_ddl(ddl));
        if observed_ddl.as_deref() != Some(expected_ddl.as_str()) {
            return Err(unsupported_generation(&format!(
                "HTTP SQLite {expected_kind} {expected_name} does not match the exact {HTTP_SPOOL_SCHEMA_VERSION} DDL"
            )));
        }
    }
    let mut statement = connection
        .prepare(
            "SELECT COALESCE(CAST(singleton AS TEXT), ''),
                    COALESCE(CAST(schema_version AS TEXT), '')
             FROM cymule_http_spool_meta ORDER BY singleton",
        )
        .map_err(contention)?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(contention)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(contention)?;
    if rows.as_slice() != [("1".to_owned(), HTTP_SPOOL_SCHEMA_VERSION.to_owned())] {
        return Err(unsupported_generation(&format!(
            "HTTP SQLite schema authority is not the singleton {HTTP_SPOOL_SCHEMA_VERSION} generation"
        )));
    }
    Ok(())
}

fn sqlite_schema_objects(
    connection: &Connection,
) -> DurableResult<Vec<(String, String, String, String)>> {
    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name, sql FROM sqlite_master
             WHERE sql IS NOT NULL AND name NOT GLOB 'sqlite_*'
             ORDER BY type, name",
        )
        .map_err(contention)?;
    statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(contention)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(contention)
}

fn normalize_ddl(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_ascii_whitespace() && *character != ';')
        .flat_map(char::to_lowercase)
        .collect()
}

fn configure_http_spool_connection(connection: &Connection) -> DurableResult<()> {
    let observed_journal_mode: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .map_err(contention)?;
    if !observed_journal_mode.eq_ignore_ascii_case("wal") {
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(contention)?;
    }
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(contention)?;
    let journal_mode: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .map_err(contention)?;
    let synchronous: i64 = connection
        .pragma_query_value(None, "synchronous", |row| row.get(0))
        .map_err(contention)?;
    if !journal_mode.eq_ignore_ascii_case("wal") || synchronous != 2 {
        return Err(DurableError::Substrate {
            code: "http_spool_durability_not_applied".to_owned(),
            message: "HTTP SQLite spool did not retain WAL plus FULL synchronous durability"
                .to_owned(),
        });
    }
    Ok(())
}

fn persist_request(
    path: &Path,
    request: &HttpSignalRequest,
) -> DurableResult<PersistRequestOutcome> {
    let mut connection = open_spool(path, false)?;
    let digest = request_digest(request)?;
    let value = cymule_core::canonical_bytes(&request.value)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(contention)?;
    let existing: Option<(String, bool)> = transaction
        .query_row(
            "SELECT request_digest, acknowledged FROM cymule_http_signals
             WHERE activation_id = ?1",
            [&request.activation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(contention)?;
    if let Some((existing, acknowledged)) = existing {
        if existing != digest {
            return Ok(PersistRequestOutcome::IdentityConflict);
        }
        transaction.commit().map_err(contention)?;
        return Ok(if acknowledged {
            PersistRequestOutcome::Acknowledged
        } else {
            PersistRequestOutcome::Pending
        });
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
    transaction.commit().map_err(contention)?;
    Ok(PersistRequestOutcome::Pending)
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
    DurableError::Substrate {
        code: "http_spool_sqlite_failed".to_owned(),
        message: error.to_string(),
    }
}

fn unsupported_generation(detail: &str) -> DurableError {
    DurableError::Validation(format!("{UNSUPPORTED_STORE_GENERATION_CODE}: {detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test(flavor = "current_thread")]
    async fn acknowledged_before_waiter_registration_completes_from_durable_readback() {
        let directory = tempfile::tempdir().expect("temporary directory creates");
        let database = directory.path().join("lost-wakeup.sqlite");
        let barrier = DurableIngressBarrier {
            persisted: Arc::new(tokio::sync::Barrier::new(2)),
            release: Arc::new(tokio::sync::Barrier::new(2)),
        };
        let (router, mut driver) =
            durable_signal_router_inner(&database, 1, AllowAll, Some(barrier.clone()))
                .expect("durable router builds");
        let response = tokio::spawn(router.oneshot(
            Request::post("/v1/signals")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"activation_id":"activation:lost-wakeup","key":"signal:test","value":true}"#,
                ))
                .expect("request builds"),
        ));

        barrier.persisted.wait().await;
        let targets = BTreeSet::from(["wait:test".to_owned()]);
        driver
            .connection
            .execute(
                "UPDATE cymule_http_signals SET selected_wait_ids = ?1
                 WHERE activation_id = 'activation:lost-wakeup'",
                [cymule_core::canonical_bytes(&targets).expect("targets encode")],
            )
            .expect("test selection persists");
        driver
            .acknowledge("activation:lost-wakeup")
            .expect("activation acknowledges before registration");
        barrier.release.wait().await;

        let response = tokio::time::timeout(Duration::from_secs(2), response)
            .await
            .expect("durable acknowledgement cannot lose the waiter")
            .expect("response task joins")
            .expect("router responds");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bounded_waiter_registry_returns_http_backpressure() {
        let directory = tempfile::tempdir().expect("temporary directory creates");
        let database = directory.path().join("bounded-waiters.sqlite");
        let (router, driver) =
            durable_signal_router(&database, 1, AllowAll).expect("durable router builds");
        let first = tokio::spawn(
            router.clone().oneshot(
                Request::post("/v1/signals")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"activation_id":"activation:first","key":"signal:test","value":true}"#,
                    ))
                    .expect("request builds"),
            ),
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if driver
                    .waiters
                    .lock()
                    .expect("HTTP waiter registry mutex is healthy")
                    .waiter_count
                    == 1
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first request registers its waiter");

        let second = router
            .oneshot(
                Request::post("/v1/signals")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"activation_id":"activation:second","key":"signal:test","value":true}"#,
                    ))
                    .expect("request builds"),
            )
            .await
            .expect("router responds");
        assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
        first.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn independent_driver_acknowledgement_completes_from_bounded_durable_readback() {
        let directory = tempfile::tempdir().expect("temporary directory creates");
        let database = directory.path().join("cross-instance-ack.sqlite");
        let (_first_router, first) =
            durable_signal_router(&database, 1, AllowAll).expect("first instance opens");
        let (_second_router, mut second) =
            durable_signal_router(&database, 1, AllowAll).expect("second instance opens");
        let request = HttpSignalRequest {
            activation_id: "activation:cross-instance".to_owned(),
            key: "signal:cross-instance".to_owned(),
            value: serde_json::json!(true),
        };
        assert_eq!(
            persist_request(&database, &request).expect("request persists"),
            PersistRequestOutcome::Pending
        );
        let mut waiter = register_waiter(&first.waiters, &request.activation_id)
            .expect("first instance registers its local waiter");
        assert!(
            !read_acknowledged(&database, &request.activation_id)
                .expect("initial durable acknowledgement reads")
        );

        let targets = BTreeSet::from(["wait:cross-instance".to_owned()]);
        second
            .connection
            .execute(
                "UPDATE cymule_http_signals SET selected_wait_ids = ?1
                 WHERE activation_id = ?2",
                params![
                    cymule_core::canonical_bytes(&targets).expect("targets encode"),
                    &request.activation_id
                ],
            )
            .expect("second instance persists selection");
        second
            .acknowledge(&request.activation_id)
            .expect("second instance acknowledges");
        assert_eq!(
            first
                .waiters
                .lock()
                .expect("first waiter registry remains healthy")
                .waiter_count,
            1,
            "another instance cannot deliver a process-local notification"
        );

        assert_eq!(
            bounded_acknowledgement_recheck(
                Arc::new(database),
                request.activation_id,
                &mut waiter,
                Duration::from_millis(1),
            )
            .await,
            StatusCode::ACCEPTED
        );
    }

    #[test]
    fn concurrent_exact_ingress_classification_is_atomic() {
        let directory = tempfile::tempdir().expect("temporary directory creates");
        let database = directory.path().join("concurrent-ingress.sqlite");
        let _initialized = durable_signal_router(&database, 1, AllowAll)
            .expect("durable router initializes the spool");
        let request = HttpSignalRequest {
            activation_id: "activation:concurrent".to_owned(),
            key: "signal:concurrent".to_owned(),
            value: serde_json::json!({"accepted": true}),
        };
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let run =
            |database: PathBuf, request: HttpSignalRequest, barrier: Arc<std::sync::Barrier>| {
                barrier.wait();
                persist_request(&database, &request)
            };
        let first_database = database.clone();
        let first_request = request.clone();
        let first_barrier = Arc::clone(&barrier);
        let first = std::thread::spawn(move || run(first_database, first_request, first_barrier));
        let second = std::thread::spawn(move || run(database, request, barrier));
        let outcomes = [
            first.join().expect("first ingress writer joins"),
            second.join().expect("second ingress writer joins"),
        ];

        assert!(
            outcomes
                .iter()
                .any(|outcome| { matches!(outcome, Ok(PersistRequestOutcome::Pending)) })
        );
        assert!(outcomes.iter().all(|outcome| {
            matches!(outcome, Ok(PersistRequestOutcome::Pending))
                || matches!(outcome, Err(DurableError::Conflict { .. }))
        }));
    }
}
