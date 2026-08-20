mod support;

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use fujin_core::{
    CoreError, Header, Message, NoBindMiddleware, OperationToken, ReaderMessage, SessionCore,
    SessionEventSink,
};
use support::{
    CompletionRecorder, FetchPlan, ReaderPlan, SessionRecorder, SettlementPlan, catalog_and_state,
};

fn reader_message(payload: &'static [u8], adapter_id: u8, with_headers: bool) -> ReaderMessage {
    ReaderMessage {
        payload: Bytes::from_static(payload),
        source: "topic".into(),
        headers: if with_headers {
            vec![Header {
                key: Bytes::from_static(b"key"),
                value: Bytes::from_static(b"value"),
            }]
        } else {
            Vec::new()
        },
        adapter_message_id: Bytes::from(vec![adapter_id]),
    }
}

#[tokio::test]
async fn subscribe_emits_success_before_delivery_and_reuses_id_after_failed_setup() {
    let (catalog, state, _) = catalog_and_state().await;
    state.push_reader_plan(ReaderPlan {
        ready_error: Some(CoreError::Unavailable("setup failed".into())),
        ..ReaderPlan::default()
    });
    state.push_reader_plan(ReaderPlan {
        subscription_messages: vec![reader_message(b"payload", 1, true)],
        terminal: Some(CoreError::Unavailable("subscription ended".into())),
        ..ReaderPlan::default()
    });
    let events = Arc::new(SessionRecorder::default());
    let mut core = SessionCore::new_with_events(
        Arc::clone(&catalog),
        Arc::new(NoBindMiddleware),
        Arc::new(CompletionRecorder::default()),
        Arc::clone(&events) as Arc<dyn SessionEventSink>,
    );
    core.bind("connector", &mut BTreeMap::new(), &BTreeMap::new())
        .await
        .expect("bind connector");

    assert!(matches!(
        core.subscribe("read", false, true, |_| Ok(())).await,
        Err(CoreError::Unavailable(_))
    ));
    assert_eq!(state.readers()[0].snapshot().close, 1);

    let ready_id = Arc::new(AtomicUsize::new(usize::MAX));
    let captured = Arc::clone(&ready_id);
    let subscription_id = core
        .subscribe("read", false, true, move |id| {
            captured.store(usize::from(id), Ordering::Release);
            Ok(())
        })
        .await
        .expect("subscribe after failed setup");
    assert_eq!(subscription_id, 0);
    assert_eq!(ready_id.load(Ordering::Acquire), 0);

    let deliveries = events.deliveries();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].subscription_id, 0);
    assert_eq!(deliveries[0].payload, Bytes::from_static(b"payload"));
    assert!(deliveries[0].message_id.is_some());
    assert_eq!(deliveries[0].headers.as_ref().map(Vec::len), Some(1));
    assert!(matches!(
        events.terminals().as_slice(),
        [(0, CoreError::Unavailable(_))]
    ));

    core.unsubscribe(subscription_id)
        .await
        .expect("unsubscribe reader");
    assert_eq!(
        core.unsubscribe(subscription_id).await,
        Err(CoreError::SubscriptionNotFound(subscription_id))
    );
    core.close().await.expect("close session");
    catalog.close().await.expect("close catalog");
}

#[tokio::test]
async fn fetch_reuses_implicit_reader_and_validates_bounds_and_header_mode() {
    let (catalog, state, _) = catalog_and_state().await;
    state.push_reader_plan(ReaderPlan {
        fetches: VecDeque::from([
            FetchPlan::success(vec![
                reader_message(b"one", 1, false),
                reader_message(b"two", 2, false),
            ]),
            FetchPlan::success(Vec::new()),
        ]),
        ..ReaderPlan::default()
    });
    state.push_reader_plan(ReaderPlan {
        fetches: VecDeque::from([FetchPlan::success(vec![reader_message(
            b"headered",
            3,
            true,
        )])]),
        ..ReaderPlan::default()
    });
    state.push_reader_plan(ReaderPlan {
        fetches: VecDeque::from([FetchPlan::success(vec![
            reader_message(b"over-one", 4, false),
            reader_message(b"over-two", 5, false),
            reader_message(b"over-three", 6, false),
        ])]),
        ..ReaderPlan::default()
    });
    state.push_reader_plan(ReaderPlan {
        fetches: VecDeque::from([FetchPlan {
            reported_count: 2,
            messages: vec![reader_message(b"mismatch", 7, true)],
            error: None,
        }]),
        ..ReaderPlan::default()
    });
    let mut core = SessionCore::new(
        Arc::clone(&catalog),
        Arc::new(NoBindMiddleware),
        Arc::new(CompletionRecorder::default()),
    );
    core.bind("connector", &mut BTreeMap::new(), &BTreeMap::new())
        .await
        .expect("bind connector");

    assert_eq!(
        core.fetch("read", false, false, 0).await,
        Err(CoreError::InvalidBatchSize)
    );
    let first = core
        .fetch("read", false, false, 2)
        .await
        .expect("fetch messages");
    let second = core
        .fetch("read", false, false, 2)
        .await
        .expect("reuse implicit reader");
    assert_eq!(first.subscription_id, second.subscription_id);
    assert_eq!(first.messages.len(), 2);
    assert!(
        first
            .messages
            .iter()
            .all(|message| message.message_id.is_some())
    );
    assert!(second.messages.is_empty());
    assert_eq!(state.readers()[0].snapshot().fetch, 2);

    let headered = core
        .fetch("read", false, true, 1)
        .await
        .expect("separate header reader");
    assert_ne!(headered.subscription_id, first.subscription_id);
    assert_eq!(headered.messages[0].headers.as_ref().map(Vec::len), Some(1));
    assert_eq!(state.readers().len(), 2);

    assert!(matches!(
        core.fetch("read", true, false, 2).await,
        Err(CoreError::Internal(_))
    ));
    assert!(matches!(
        core.fetch("read", true, true, 2).await,
        Err(CoreError::Internal(_))
    ));
    core.close().await.expect("close session");
    assert!(
        state
            .readers()
            .iter()
            .all(|reader| reader.snapshot().close == 1)
    );
    catalog.close().await.expect("close catalog");
}

#[tokio::test]
async fn settlement_validates_scope_duplicates_and_consumption() {
    let (catalog, state, _) = catalog_and_state().await;
    state.push_reader_plan(ReaderPlan {
        fetches: VecDeque::from([FetchPlan::success(vec![
            reader_message(b"one", 1, false),
            reader_message(b"two", 2, false),
        ])]),
        settlements: VecDeque::from([
            SettlementPlan {
                results: vec![None],
                ..SettlementPlan::default()
            },
            SettlementPlan {
                results: vec![None],
                ..SettlementPlan::default()
            },
        ]),
        ..ReaderPlan::default()
    });
    state.push_reader_plan(ReaderPlan {
        fetches: VecDeque::from([FetchPlan::success(vec![reader_message(
            b"new incarnation",
            1,
            false,
        )])]),
        ..ReaderPlan::default()
    });
    let mut core = SessionCore::new(
        Arc::clone(&catalog),
        Arc::new(NoBindMiddleware),
        Arc::new(CompletionRecorder::default()),
    );
    core.bind("connector", &mut BTreeMap::new(), &BTreeMap::new())
        .await
        .expect("bind connector");

    let fetched = core
        .fetch("read", false, false, 2)
        .await
        .expect("fetch settleable messages");
    let first = fetched.messages[0]
        .message_id
        .clone()
        .expect("manual message ID");
    let second = fetched.messages[1]
        .message_id
        .clone()
        .expect("manual message ID");
    assert!(
        core.ack(255, Vec::new())
            .await
            .expect("empty ACK")
            .is_empty()
    );
    assert!(matches!(
        core.ack(
            fetched.subscription_id,
            vec![second.clone(), second.clone()]
        )
        .await,
        Err(CoreError::InvalidMessageId(_))
    ));
    let acked = core
        .ack(fetched.subscription_id, vec![first.clone()])
        .await
        .expect("ACK first message");
    assert_eq!(acked[0].message_id, first);
    assert!(matches!(
        core.ack(fetched.subscription_id, vec![first.clone()]).await,
        Err(CoreError::InvalidMessageId(_))
    ));
    let nacked = core
        .nack(fetched.subscription_id, vec![second.clone()])
        .await
        .expect("NACK second message");
    assert_eq!(nacked[0].message_id, second);
    assert_eq!(state.readers()[0].snapshot().settlement, 2);

    core.unsubscribe(fetched.subscription_id)
        .await
        .expect("remove first incarnation");
    let replacement = core
        .fetch("read", false, false, 1)
        .await
        .expect("reuse subscription ID with new incarnation");
    assert_eq!(replacement.subscription_id, fetched.subscription_id);
    assert_ne!(replacement.messages[0].message_id.as_ref(), Some(&first));
    assert!(matches!(
        core.ack(replacement.subscription_id, vec![first]).await,
        Err(CoreError::InvalidMessageId(_))
    ));

    core.close().await.expect("close session");
    catalog.close().await.expect("close catalog");
}

#[tokio::test]
async fn settlement_matches_duplicate_adapter_ids_by_occurrence() {
    let (catalog, state, _) = catalog_and_state().await;
    state.push_reader_plan(ReaderPlan {
        fetches: VecDeque::from([FetchPlan::success(vec![
            reader_message(b"first", 1, false),
            reader_message(b"second", 1, false),
        ])]),
        settlements: VecDeque::from([SettlementPlan {
            results: vec![None, None],
            ..SettlementPlan::default()
        }]),
        ..ReaderPlan::default()
    });
    let mut core = SessionCore::new(
        Arc::clone(&catalog),
        Arc::new(NoBindMiddleware),
        Arc::new(CompletionRecorder::default()),
    );
    core.bind("connector", &mut BTreeMap::new(), &BTreeMap::new())
        .await
        .expect("bind connector");
    let fetched = core
        .fetch("read", false, false, 2)
        .await
        .expect("fetch duplicate adapter IDs");
    let message_ids: Vec<_> = fetched
        .messages
        .iter()
        .map(|message| message.message_id.clone().expect("message ID"))
        .collect();

    let results = core
        .ack(fetched.subscription_id, message_ids.clone())
        .await
        .expect("settle duplicate adapter IDs");

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].message_id, message_ids[0]);
    assert_eq!(results[1].message_id, message_ids[1]);
    assert!(results.iter().all(|result| result.result.is_ok()));
    core.close().await.expect("close session");
    catalog.close().await.expect("close catalog");
}

#[tokio::test]
async fn nack_requires_route_capability() {
    let (catalog, state, _) = catalog_and_state().await;
    state.push_reader_plan(ReaderPlan {
        fetches: VecDeque::from([FetchPlan::success(vec![reader_message(
            b"no nack", 3, false,
        )])]),
        ..ReaderPlan::default()
    });
    let mut core = SessionCore::new(
        Arc::clone(&catalog),
        Arc::new(NoBindMiddleware),
        Arc::new(CompletionRecorder::default()),
    );
    core.bind("connector", &mut BTreeMap::new(), &BTreeMap::new())
        .await
        .expect("bind connector");

    let fetched = core
        .fetch("read_no_nack", false, false, 1)
        .await
        .expect("fetch no-nack route");
    assert_eq!(
        core.nack(
            fetched.subscription_id,
            vec![fetched.messages[0].message_id.clone().expect("message ID")],
        )
        .await,
        Err(CoreError::OperationUnsupported)
    );

    core.close().await.expect("close session");
    catalog.close().await.expect("close catalog");
}

#[tokio::test]
async fn settlement_failures_report_and_only_consume_successful_ids() {
    let (catalog, state, _) = catalog_and_state().await;
    state.push_reader_plan(ReaderPlan {
        fetches: VecDeque::from([FetchPlan::success(vec![
            reader_message(b"first", 1, false),
            reader_message(b"second", 2, false),
        ])]),
        settlements: VecDeque::from([
            SettlementPlan {
                top_error: Some(CoreError::Unavailable("top-level failure".into())),
                ..SettlementPlan::default()
            },
            SettlementPlan {
                results: vec![
                    None,
                    Some(CoreError::Unavailable("per-message failure".into())),
                ],
                ..SettlementPlan::default()
            },
            SettlementPlan {
                results: vec![None],
                ..SettlementPlan::default()
            },
        ]),
        ..ReaderPlan::default()
    });
    let mut core = SessionCore::new(
        Arc::clone(&catalog),
        Arc::new(NoBindMiddleware),
        Arc::new(CompletionRecorder::default()),
    );
    core.bind("connector", &mut BTreeMap::new(), &BTreeMap::new())
        .await
        .expect("bind connector");

    let fetched = core
        .fetch("read", false, false, 2)
        .await
        .expect("fetch settleable messages");
    let first = fetched.messages[0]
        .message_id
        .clone()
        .expect("first message ID");
    let second = fetched.messages[1]
        .message_id
        .clone()
        .expect("second message ID");
    assert_eq!(
        core.ack(fetched.subscription_id, vec![first.clone(), second.clone()])
            .await,
        Err(CoreError::Unavailable("top-level failure".into()))
    );

    let partial = core
        .ack(fetched.subscription_id, vec![first.clone(), second.clone()])
        .await
        .expect("retry after top-level failure");
    assert_eq!(partial.len(), 2);
    assert_eq!(partial[0].message_id, first);
    assert_eq!(partial[0].result, Ok(()));
    assert_eq!(partial[1].message_id, second);
    assert!(matches!(partial[1].result, Err(CoreError::Unavailable(_))));
    assert!(matches!(
        core.ack(fetched.subscription_id, vec![first]).await,
        Err(CoreError::InvalidMessageId(_))
    ));
    core.ack(fetched.subscription_id, vec![second])
        .await
        .expect("retry per-message failure");
    assert_eq!(state.readers()[0].snapshot().settlement, 3);

    core.close().await.expect("close session");
    catalog.close().await.expect("close catalog");
}

#[tokio::test]
async fn auto_settle_delivery_has_no_id_and_rejects_settlement() {
    let (catalog, state, _) = catalog_and_state().await;
    state.push_reader_plan(ReaderPlan {
        fetches: VecDeque::from([FetchPlan::success(vec![reader_message(b"auto", 1, false)])]),
        ..ReaderPlan::default()
    });
    let mut core = SessionCore::new(
        Arc::clone(&catalog),
        Arc::new(NoBindMiddleware),
        Arc::new(CompletionRecorder::default()),
    );
    core.bind("connector", &mut BTreeMap::new(), &BTreeMap::new())
        .await
        .expect("bind connector");

    let fetched = core
        .fetch("read", true, false, 1)
        .await
        .expect("auto-settle fetch");
    assert_eq!(fetched.messages[0].message_id, None);
    assert_eq!(
        core.ack(
            fetched.subscription_id,
            vec![Bytes::from_static(b"invalid")]
        )
        .await,
        Err(CoreError::OperationUnsupported)
    );

    core.produce(
        OperationToken::external(10).expect("external token"),
        "topic",
        Message::new(Bytes::from_static(b"writer still works")),
    )
    .expect("mixed reader writer session");
    core.close().await.expect("close session");
    catalog.close().await.expect("close catalog");
}
