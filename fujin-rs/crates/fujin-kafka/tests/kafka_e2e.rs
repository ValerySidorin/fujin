#![cfg(feature = "rdkafka")]

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use bytes::Bytes;
use fujin_core::{
    Catalog, Completion, CompletionSink, ConnectorConfig, DescriptorRegistry, GenerationCompiler,
    Header, Message, NoBindMiddleware, NoConnectorMiddleware, OperationToken, SessionCore,
    SessionEventSink,
};
use tokio::sync::mpsc;

#[derive(Debug)]
struct CompletionChannel(mpsc::UnboundedSender<Completion>);

impl CompletionSink for CompletionChannel {
    fn complete(&self, completion: Completion) {
        let _ = self.0.send(completion);
    }
}

#[derive(Debug)]
enum SessionEvent {
    Delivery(fujin_core::Delivery),
    Terminal(fujin_core::CoreError),
}

#[derive(Debug)]
struct EventChannel(mpsc::UnboundedSender<SessionEvent>);

impl SessionEventSink for EventChannel {
    fn delivery(&self, delivery: fujin_core::Delivery) {
        let _ = self.0.send(SessionEvent::Delivery(delivery));
    }

    fn subscription_terminal(&self, _subscription_id: u8, error: fujin_core::CoreError) {
        let _ = self.0.send(SessionEvent::Terminal(error));
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn kafka_produce_subscribe_settle_and_transaction() {
    if std::env::var_os("FUJIN_KAFKA_E2E").is_none() {
        return;
    }

    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos()
    );
    let topic = format!("fujin-rs-{unique}");
    let group = format!("fujin-rs-group-{unique}");
    let transactional_id = format!("fujin-rs-tx-{unique}");
    let registry = Arc::new(DescriptorRegistry::default());
    registry
        .register("kafka", fujin_kafka::descriptor())
        .expect("register Kafka connector");
    let compiler = Arc::new(GenerationCompiler::new(
        registry,
        Arc::new(NoConnectorMiddleware),
    ));
    let settings = serde_json::json!({
        "common": {
            "brokers": ["127.0.0.1:9092"],
            "properties": {
                "message.timeout.ms": "30000"
            }
        },
        "routes": {
            "events": {
                "produce_topic": topic,
                "consume_topics": [topic],
                "group": group,
                "properties": {
                    "auto.offset.reset": "earliest",
                    "allow.auto.create.topics": "true"
                }
            },
            "transactions": {
                "produce_topic": topic,
                "transactional_id": transactional_id,
                "properties": {
                    "allow.auto.create.topics": "true"
                }
            }
        }
    });
    let configs = BTreeMap::from([(
        "primary".into(),
        ConnectorConfig {
            connector_type: "kafka".into(),
            overridable: Vec::new(),
            bind_middlewares: Vec::new(),
            connector_middlewares: Vec::new(),
            settings,
        },
    )]);
    let catalog = Arc::new(
        Catalog::compile(&configs, compiler)
            .await
            .expect("compile Kafka catalog"),
    );
    let (completion_sender, mut completions) = mpsc::unbounded_channel();
    let (event_sender, mut events) = mpsc::unbounded_channel();
    let mut core = SessionCore::new_with_events(
        Arc::clone(&catalog),
        Arc::new(NoBindMiddleware),
        Arc::new(CompletionChannel(completion_sender)),
        Arc::new(EventChannel(event_sender)),
    );
    core.bind("primary", &mut BTreeMap::new(), &BTreeMap::new())
        .await
        .expect("bind Kafka connector");
    let subscription_id = core
        .subscribe("events", false, true, |_| Ok(()))
        .await
        .expect("subscribe Kafka route");

    core.produce(
        OperationToken::external(1).expect("produce token"),
        "events",
        Message::with_headers(
            Bytes::from_static(b"broker-backed-payload"),
            vec![Header {
                key: Bytes::from_static(b"source"),
                value: Bytes::from_static(b"fujin-rs"),
            }],
        ),
    )
    .expect("accept Kafka produce");
    let completion = tokio::time::timeout(Duration::from_secs(30), completions.recv())
        .await
        .expect("Kafka produce deadline")
        .expect("Kafka produce completion");
    assert_eq!(completion.token.value(), 1);
    completion.result.expect("Kafka produce delivery");

    let event = tokio::time::timeout(Duration::from_secs(30), events.recv())
        .await
        .expect("Kafka delivery deadline")
        .expect("Kafka session event");
    let delivery = match event {
        SessionEvent::Delivery(delivery) => delivery,
        SessionEvent::Terminal(error) => panic!("Kafka subscription terminated: {error}"),
    };
    assert_eq!(delivery.subscription_id, subscription_id);
    assert_eq!(
        delivery.payload,
        Bytes::from_static(b"broker-backed-payload")
    );
    assert_eq!(delivery.headers.as_ref().map(Vec::len), Some(1));
    let message_id = delivery.message_id.expect("manual settlement ID");
    let settled = core
        .ack(subscription_id, vec![message_id.clone()])
        .await
        .expect("commit Kafka offset");
    assert_eq!(settled.len(), 1);
    assert_eq!(settled[0].message_id, message_id);
    settled[0].result.clone().expect("Kafka offset commit");

    core.begin_transaction("transactions")
        .await
        .expect("begin Kafka transaction");
    core.transaction_produce(
        OperationToken::external(2).expect("transaction token"),
        Message::new(Bytes::from_static(b"transactional-payload")),
    )
    .expect("accept transactional produce");
    let completion = tokio::time::timeout(Duration::from_secs(30), completions.recv())
        .await
        .expect("Kafka transaction delivery deadline")
        .expect("Kafka transaction completion");
    assert_eq!(completion.token.value(), 2);
    completion.result.expect("Kafka transactional delivery");
    core.commit_transaction()
        .await
        .expect("commit Kafka transaction");

    core.close().await.expect("close Kafka session");
    catalog.close().await.expect("close Kafka catalog");
}
