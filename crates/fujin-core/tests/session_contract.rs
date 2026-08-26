mod support;

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use bytes::Bytes;
use fujin_connector::{
    Capabilities, CompletionSink, ConnectorConfig, Header, Message, OperationToken,
};
use fujin_core::{SessionCore, SessionState};
use fujin_error::{CoreError, Result};
use fujin_middleware::{BindMiddlewareRunner, NoBindMiddleware};
use parking_lot::Mutex;
use support::{CompletionRecorder, WriterPlan, catalog_and_state};

use async_trait::async_trait;
#[derive(Debug, Default)]
struct RecordingBindRunner {
    calls: Mutex<Vec<String>>,
}

#[async_trait]
impl BindMiddlewareRunner for RecordingBindRunner {
    async fn run(
        &self,
        connector_name: &str,
        connector: &ConnectorConfig,
        metadata: &mut BTreeMap<String, String>,
    ) -> Result<()> {
        self.calls.lock().push(connector_name.to_owned());
        metadata.insert("middleware".into(), "ran".into());
        assert_eq!(connector.settings["routes"]["pub"]["topic"], "before");
        Ok(())
    }
}

#[tokio::test]
async fn bind_pins_routes_runs_middleware_and_compiles_private_overrides() {
    let (catalog, state, _) = catalog_and_state().await;
    let runner = Arc::new(RecordingBindRunner::default());
    let recorder = Arc::new(CompletionRecorder::default());
    let mut core = SessionCore::new(
        Arc::clone(&catalog),
        Arc::clone(&runner) as Arc<dyn BindMiddlewareRunner>,
        recorder as Arc<dyn CompletionSink>,
    );
    let mut metadata = BTreeMap::new();
    let overrides = BTreeMap::from([("routes.pub.topic".into(), "after".into())]);

    let bound = core
        .bind("connector", &mut metadata, &overrides)
        .await
        .expect("bind connector");

    assert_eq!(core.state(), SessionState::Connected);
    assert!(
        bound.routes["topic"]
            .capabilities
            .contains(Capabilities::HEADERS)
    );
    assert_eq!(metadata["middleware"], "ran");
    assert_eq!(runner.calls.lock().as_slice(), ["connector"]);
    let compiled = state.compiled_settings();
    assert_eq!(compiled.len(), 2);
    assert_eq!(compiled[0]["routes"]["pub"]["topic"], "before");
    assert_eq!(compiled[1]["routes"]["pub"]["topic"], "after");
    assert_eq!(
        core.bind("connector", &mut metadata, &BTreeMap::new())
            .await,
        Err(CoreError::AlreadyBound)
    );

    core.close().await.expect("close session");
    catalog.close().await.expect("close catalog");
}

#[tokio::test]
async fn produce_reuses_writer_preserves_header_mode_and_routes_completions() {
    let (catalog, state, _) = catalog_and_state().await;
    let recorder = Arc::new(CompletionRecorder::default());
    let mut core = SessionCore::new(
        Arc::clone(&catalog),
        Arc::new(NoBindMiddleware),
        Arc::clone(&recorder) as Arc<dyn CompletionSink>,
    );
    core.bind("connector", &mut BTreeMap::new(), &BTreeMap::new())
        .await
        .expect("bind connector");
    assert_eq!(state.runtime_opens(), 0);

    core.produce(
        OperationToken::external(1).expect("external token"),
        "topic",
        Message::new(Bytes::from_static(b"one")),
    )
    .expect("produce ordinary message");
    core.produce(
        OperationToken::external(2).expect("external token"),
        "topic",
        Message::with_headers(
            Bytes::from_static(b"two"),
            vec![Header {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
            }],
        ),
    )
    .expect("produce headered message");

    assert_eq!(state.runtime_opens(), 1);
    let writers = state.writers();
    assert_eq!(writers.len(), 1);
    let snapshot = writers[0].snapshot();
    assert_eq!(snapshot.produce, 2);
    assert_eq!(snapshot.messages[0].headers, None);
    assert_eq!(snapshot.messages[1].headers.as_ref().map(Vec::len), Some(1));
    let completions = recorder.values();
    assert_eq!(completions.len(), 2);
    assert_eq!(completions[0].token.value(), 1);
    assert_eq!(completions[1].token.value(), 2);

    assert_eq!(
        core.produce(
            OperationToken::external(3).expect("external token"),
            "plain",
            Message::with_headers(Bytes::from_static(b"three"), Vec::new()),
        ),
        Err(CoreError::OperationUnsupported)
    );
    assert!(matches!(
        core.produce(
            OperationToken::external(4).expect("external token"),
            "topic",
            Message::with_headers(
                Bytes::from_static(b"bad"),
                vec![Header {
                    key: Bytes::new(),
                    value: Bytes::new(),
                }],
            ),
        ),
        Err(CoreError::InvalidHeaders(_))
    ));

    core.close().await.expect("close session");
    catalog.close().await.expect("close catalog");
}

#[tokio::test]
async fn transaction_lifecycle_rejects_invalid_transitions_and_reuses_successful_writer() {
    let (catalog, state, _) = catalog_and_state().await;
    let recorder = Arc::new(CompletionRecorder::default());
    let mut core = SessionCore::new(
        Arc::clone(&catalog),
        Arc::new(NoBindMiddleware),
        Arc::clone(&recorder) as Arc<dyn CompletionSink>,
    );
    core.bind("connector", &mut BTreeMap::new(), &BTreeMap::new())
        .await
        .expect("bind connector");

    assert_eq!(
        core.commit_transaction().await,
        Err(CoreError::NoTransaction)
    );
    assert_eq!(
        core.transaction_produce(
            OperationToken::external(7).expect("external token"),
            Message::new(Bytes::from_static(b"message")),
        ),
        Err(CoreError::NoTransaction)
    );
    core.begin_transaction("tx")
        .await
        .expect("begin transaction");
    assert_eq!(core.state(), SessionState::InTransaction);
    assert_eq!(
        core.begin_transaction("tx").await,
        Err(CoreError::TransactionActive)
    );
    assert_eq!(
        core.produce(
            OperationToken::external(8).expect("external token"),
            "tx",
            Message::new(Bytes::from_static(b"ordinary")),
        ),
        Err(CoreError::TransactionCommandRequired)
    );
    core.transaction_produce(
        OperationToken::external(9).expect("external token"),
        Message::new(Bytes::from_static(b"transactional")),
    )
    .expect("transaction produce");
    core.commit_transaction().await.expect("commit transaction");
    assert_eq!(core.state(), SessionState::Connected);

    core.begin_transaction("tx")
        .await
        .expect("begin second transaction");
    core.rollback_transaction()
        .await
        .expect("rollback transaction");
    assert_eq!(
        core.rollback_transaction().await,
        Err(CoreError::NoTransaction)
    );
    core.close().await.expect("close session");

    let writers = state.writers();
    assert_eq!(writers.len(), 1);
    let writer = writers[0].snapshot();
    assert_eq!(writer.produce, 1);
    assert_eq!(writer.begin, 2);
    assert_eq!(writer.flush, 1);
    assert_eq!(writer.commit, 1);
    assert_eq!(writer.rollback, 1);
    assert_eq!(writer.close, 1);
    let external = recorder.values();
    assert_eq!(external.len(), 1);
    assert_eq!(external[0].token.value(), 9);
    catalog.close().await.expect("close catalog");
}

#[tokio::test]
async fn transaction_terminal_failures_end_local_state_and_close_poisoned_writer() {
    for (plan, expected) in [
        (
            WriterPlan {
                flush: Some(CoreError::Unavailable("flush failed".into())),
                ..WriterPlan::default()
            },
            "aborted",
        ),
        (
            WriterPlan {
                commit: Some(CoreError::Unavailable("commit failed".into())),
                ..WriterPlan::default()
            },
            "unknown",
        ),
        (
            WriterPlan {
                rollback: Some(CoreError::Unavailable("rollback failed".into())),
                ..WriterPlan::default()
            },
            "rollback",
        ),
    ] {
        let (catalog, state, _) = catalog_and_state().await;
        state.push_plan(plan);
        let mut core = SessionCore::new(
            Arc::clone(&catalog),
            Arc::new(NoBindMiddleware),
            Arc::new(CompletionRecorder::default()),
        );
        core.bind("connector", &mut BTreeMap::new(), &BTreeMap::new())
            .await
            .expect("bind connector");
        core.begin_transaction("tx")
            .await
            .expect("begin transaction");

        let error = if expected == "rollback" {
            core.rollback_transaction()
                .await
                .expect_err("rollback fails")
        } else {
            core.commit_transaction().await.expect_err("commit fails")
        };
        match expected {
            "aborted" => assert!(matches!(error, CoreError::TransactionAborted(_))),
            "unknown" => assert!(matches!(error, CoreError::CommitOutcomeUnknown(_))),
            "rollback" => assert!(matches!(error, CoreError::Unavailable(_))),
            _ => unreachable!(),
        }
        assert_eq!(core.state(), SessionState::Connected);
        assert_eq!(state.writers()[0].snapshot().close, 1);
        core.close().await.expect("close session");
        catalog.close().await.expect("close catalog");
    }
}

#[tokio::test]
async fn close_flushes_closes_and_is_idempotent() {
    let (catalog, state, _) = catalog_and_state().await;
    let mut core = SessionCore::new(
        Arc::clone(&catalog),
        Arc::new(NoBindMiddleware),
        Arc::new(CompletionRecorder::default()),
    );
    core.bind("connector", &mut BTreeMap::new(), &BTreeMap::new())
        .await
        .expect("bind connector");
    core.produce(
        OperationToken::external(1).expect("external token"),
        "topic",
        Message::new(Bytes::from_static(b"payload")),
    )
    .expect("produce message");

    core.close().await.expect("close session");
    core.close().await.expect("idempotent close");
    assert_eq!(core.state(), SessionState::Closed);
    assert_eq!(state.writers()[0].snapshot().flush, 1);
    assert_eq!(state.writers()[0].snapshot().close, 1);
    assert_eq!(
        core.produce(
            OperationToken::external(2).expect("external token"),
            "topic",
            Message::new(Bytes::from_static(b"after close")),
        ),
        Err(CoreError::Closed)
    );
    catalog.close().await.expect("close catalog");
    assert_eq!(state.runtime_closes(), 1);
}

#[tokio::test]
async fn close_timeout_returns_while_owned_cleanup_continues() {
    let (catalog, state, _) = catalog_and_state().await;
    state.push_plan(WriterPlan {
        hang_close: true,
        ..WriterPlan::default()
    });
    let mut core = SessionCore::new(
        Arc::clone(&catalog),
        Arc::new(NoBindMiddleware),
        Arc::new(CompletionRecorder::default()),
    );
    core.bind("connector", &mut BTreeMap::new(), &BTreeMap::new())
        .await
        .expect("bind connector");
    core.produce(
        OperationToken::external(1).expect("external token"),
        "topic",
        Message::new(Bytes::from_static(b"payload")),
    )
    .expect("produce message");

    let error = core
        .close_with_timeout(Duration::from_millis(10))
        .await
        .expect_err("cleanup deadline");

    assert!(matches!(error, CoreError::Unavailable(_)));
    assert_eq!(core.state(), SessionState::Closed);
    assert_eq!(state.writers()[0].snapshot().close, 1);
}
