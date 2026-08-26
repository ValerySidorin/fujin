//! Kafka connector for Fujin, backed by librdkafka.

mod implementation {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };

    use bytes::{BufMut, Bytes, BytesMut};
    use fujin_connector::{
        AcceptanceGuarantee, AckGranularity, BoxFuture, Capabilities, CompiledConnector,
        Completion, CompletionSink, ConnectorDescriptor, ConnectorRuntime, Delivery, Header,
        Message, NackEffect, OperationToken, Reader, ReaderEvent, ReaderEventSink, ReadyCallback,
        RouteProfile, SettlementKind, SettlementProfile, SettlementResult, Writer,
    };
    use fujin_error::{CoreError, Result};
    use parking_lot::Mutex;
    use rdkafka::{
        ClientConfig, Message as KafkaMessage, Offset, TopicPartitionList,
        consumer::{CommitMode, Consumer, StreamConsumer},
        error::KafkaError,
        message::{Header as KafkaHeader, Headers, OwnedHeaders},
        producer::{FutureProducer, FutureRecord, Producer},
        util::Timeout,
    };
    use serde::Deserialize;
    use tokio::{sync::mpsc, task::JoinHandle};
    use tokio_util::sync::CancellationToken;

    const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
    const FETCH_IDLE_TIMEOUT: Duration = Duration::from_millis(100);

    #[derive(Clone, Debug, Deserialize)]
    struct KafkaConfig {
        common: CommonConfig,
        routes: BTreeMap<String, RouteConfig>,
    }

    #[derive(Clone, Debug, Deserialize)]
    struct CommonConfig {
        brokers: Vec<String>,
        #[serde(default)]
        properties: BTreeMap<String, String>,
    }

    #[derive(Clone, Debug, Deserialize)]
    struct RouteConfig {
        #[serde(default)]
        produce_topic: Option<String>,
        #[serde(default)]
        consume_topics: Vec<String>,
        #[serde(default)]
        group: Option<String>,
        #[serde(default)]
        transactional_id: Option<String>,
        #[serde(default)]
        properties: BTreeMap<String, String>,
    }

    #[derive(Debug, Default)]
    pub struct KafkaDescriptor;

    impl ConnectorDescriptor for KafkaDescriptor {
        fn compile(&self, settings: &serde_json::Value) -> Result<Arc<dyn CompiledConnector>> {
            let config: KafkaConfig =
                serde_json::from_value(settings.clone()).map_err(|error| {
                    CoreError::InvalidConfig(format!("Kafka configuration: {error}"))
                })?;
            if config.common.brokers.is_empty()
                || config.common.brokers.iter().any(String::is_empty)
            {
                return Err(CoreError::InvalidConfig(
                    "Kafka common.brokers must contain non-empty addresses".into(),
                ));
            }
            if config.routes.is_empty() {
                return Err(CoreError::InvalidConfig("Kafka routes are empty".into()));
            }
            let mut profiles = BTreeMap::new();
            for (name, route) in &config.routes {
                if name.is_empty() {
                    return Err(CoreError::InvalidConfig("Kafka route name is empty".into()));
                }
                let produce = route
                    .produce_topic
                    .as_ref()
                    .is_some_and(|topic| !topic.is_empty());
                let consume = !route.consume_topics.is_empty()
                    && route.consume_topics.iter().all(|topic| !topic.is_empty());
                if !produce && !consume {
                    return Err(CoreError::InvalidConfig(format!(
                        "Kafka route {name:?} has neither produce_topic nor consume_topics"
                    )));
                }
                if route.transactional_id.is_some() && !produce {
                    return Err(CoreError::InvalidConfig(format!(
                        "Kafka route {name:?} has transactional_id without produce_topic"
                    )));
                }
                if consume && route.group.as_ref().is_none_or(String::is_empty) {
                    return Err(CoreError::InvalidConfig(format!(
                        "Kafka route {name:?} requires group for consumption"
                    )));
                }
                let mut capabilities = Capabilities::HEADERS;
                let mut guarantee = AcceptanceGuarantee::Unspecified;
                let mut settlement = SettlementProfile::default();
                if produce {
                    capabilities = capabilities.union(Capabilities::PRODUCE);
                    guarantee = AcceptanceGuarantee::Durable;
                    if route.transactional_id.is_some() {
                        capabilities = capabilities.union(Capabilities::TRANSACTIONS);
                    }
                }
                if consume {
                    capabilities = capabilities
                        .union(Capabilities::SUBSCRIBE)
                        .union(Capabilities::FETCH)
                        .union(Capabilities::MANUAL_SETTLEMENT);
                    settlement = SettlementProfile {
                        ack: AckGranularity::Cumulative,
                        nack: NackEffect::Unsupported,
                    };
                }
                profiles.insert(
                    name.clone(),
                    RouteProfile {
                        capabilities,
                        produce_guarantee: guarantee,
                        settlement,
                    },
                );
            }
            Ok(Arc::new(KafkaCompiled { config, profiles }))
        }
    }

    struct KafkaCompiled {
        config: KafkaConfig,
        profiles: BTreeMap<String, RouteProfile>,
    }

    impl std::fmt::Debug for KafkaCompiled {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("KafkaCompiled")
                .field("routes", &self.profiles.keys())
                .finish_non_exhaustive()
        }
    }

    impl CompiledConnector for KafkaCompiled {
        fn routes(&self) -> &BTreeMap<String, RouteProfile> {
            &self.profiles
        }

        fn open_runtime(&self) -> Result<Arc<dyn ConnectorRuntime>> {
            Ok(Arc::new(KafkaRuntime {
                config: self.config.clone(),
                shared_producers: Mutex::new(BTreeMap::new()),
                next_transactional_writer: AtomicU64::new(1),
            }))
        }
    }

    struct KafkaRuntime {
        config: KafkaConfig,
        shared_producers: Mutex<BTreeMap<String, FutureProducer>>,
        next_transactional_writer: AtomicU64,
    }

    impl std::fmt::Debug for KafkaRuntime {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("KafkaRuntime")
                .field("routes", &self.config.routes.keys())
                .finish_non_exhaustive()
        }
    }

    impl KafkaRuntime {
        fn client_config(&self, route: &RouteConfig) -> ClientConfig {
            let mut config = ClientConfig::new();
            config.set("bootstrap.servers", self.config.common.brokers.join(","));
            for (key, value) in &self.config.common.properties {
                config.set(key, value);
            }
            for (key, value) in &route.properties {
                config.set(key, value);
            }
            config
        }

        fn producer(&self, route_name: &str, route: &RouteConfig) -> Result<FutureProducer> {
            if let Some(transactional_id) = route.transactional_id.as_ref() {
                let mut config = self.client_config(route);
                let sequence = self
                    .next_transactional_writer
                    .fetch_add(1, Ordering::Relaxed);
                config.set("transactional.id", format!("{transactional_id}-{sequence}"));
                return config.create().map_err(kafka_unavailable);
            }
            let mut producers = self.shared_producers.lock();
            if let Some(producer) = producers.get(route_name) {
                return Ok(producer.clone());
            }
            let producer: FutureProducer = self
                .client_config(route)
                .create()
                .map_err(kafka_unavailable)?;
            producers.insert(route_name.to_owned(), producer.clone());
            Ok(producer)
        }
    }

    impl ConnectorRuntime for KafkaRuntime {
        fn open_reader(
            &self,
            route: &str,
            auto_settle: bool,
            events: Arc<dyn ReaderEventSink>,
        ) -> Result<Arc<dyn Reader>> {
            let route = self
                .config
                .routes
                .get(route)
                .ok_or_else(|| CoreError::RouteNotFound(route.into()))?
                .clone();
            if route.consume_topics.is_empty() {
                return Err(CoreError::OperationUnsupported);
            }
            let mut config = self.client_config(&route);
            config.set(
                "group.id",
                route.group.as_deref().ok_or_else(|| {
                    CoreError::InvalidConfig("Kafka consumer group is empty".into())
                })?,
            );
            config.set("enable.auto.commit", auto_settle.to_string());
            config.set("enable.auto.offset.store", auto_settle.to_string());
            let consumer: StreamConsumer = config.create().map_err(kafka_unavailable)?;
            let topics: Vec<&str> = route.consume_topics.iter().map(String::as_str).collect();
            consumer.subscribe(&topics).map_err(kafka_unavailable)?;
            Ok(KafkaReader::new(Arc::new(consumer), auto_settle, events))
        }

        fn open_writer(
            &self,
            route: &str,
            completions: Arc<dyn CompletionSink>,
        ) -> Result<Arc<dyn Writer>> {
            let route_config = self
                .config
                .routes
                .get(route)
                .ok_or_else(|| CoreError::RouteNotFound(route.into()))?;
            let topic = route_config
                .produce_topic
                .clone()
                .ok_or(CoreError::OperationUnsupported)?;
            Ok(Arc::new(KafkaWriter {
                producer: self.producer(route, route_config)?,
                topic,
                transactional: route_config.transactional_id.is_some(),
                initialized: std::sync::atomic::AtomicBool::new(false),
                completions,
            }))
        }

        fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
            Box::pin(async move {
                let producers = std::mem::take(&mut *self.shared_producers.lock());
                tokio::task::spawn_blocking(move || {
                    for producer in producers.into_values() {
                        producer
                            .flush(Timeout::After(OPERATION_TIMEOUT))
                            .map_err(kafka_unavailable)?;
                    }
                    Ok(())
                })
                .await
                .map_err(|error| {
                    CoreError::Internal(format!("join Kafka runtime close: {error}"))
                })?
            })
        }
    }

    struct KafkaWriter {
        producer: FutureProducer,
        topic: String,
        transactional: bool,
        initialized: std::sync::atomic::AtomicBool,
        completions: Arc<dyn CompletionSink>,
    }

    impl std::fmt::Debug for KafkaWriter {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("KafkaWriter")
                .field("topic", &self.topic)
                .field("transactional", &self.transactional)
                .finish_non_exhaustive()
        }
    }

    impl KafkaWriter {
        fn blocking_operation(
            &self,
            token: OperationToken,
            operation: impl FnOnce(FutureProducer) -> Result<()> + Send + 'static,
        ) {
            let producer = self.producer.clone();
            let completions = Arc::clone(&self.completions);
            tokio::spawn(async move {
                let result = tokio::task::spawn_blocking(move || operation(producer))
                    .await
                    .map_err(|error| {
                        CoreError::Internal(format!("join Kafka producer operation: {error}"))
                    })
                    .and_then(std::convert::identity);
                completions.complete(Completion { token, result });
            });
        }
    }

    impl Writer for KafkaWriter {
        fn produce(&self, token: OperationToken, message: Message) -> Result<()> {
            let mut headers =
                OwnedHeaders::new_with_capacity(message.headers.as_ref().map_or(0, Vec::len));
            if let Some(values) = message.headers.as_ref() {
                for header in values {
                    let key = std::str::from_utf8(&header.key).map_err(|_| {
                        CoreError::InvalidHeaders("Kafka header key is not UTF-8".into())
                    })?;
                    headers = headers.insert(KafkaHeader {
                        key,
                        value: Some(header.value.as_ref()),
                    });
                }
            }
            let record = FutureRecord::<(), [u8]>::to(&self.topic)
                .payload(message.payload.as_ref())
                .headers(headers);
            let delivery = self
                .producer
                .send_result(record)
                .map_err(|(error, _)| kafka_unavailable(error))?;
            let completions = Arc::clone(&self.completions);
            tokio::spawn(async move {
                let result = match delivery.await {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err((error, _))) => Err(kafka_unavailable(error)),
                    Err(error) => Err(CoreError::Internal(format!(
                        "Kafka delivery channel closed: {error}"
                    ))),
                };
                completions.complete(Completion { token, result });
            });
            Ok(())
        }

        fn flush(&self, token: OperationToken) -> Result<()> {
            self.blocking_operation(token, |producer| {
                producer
                    .flush(Timeout::After(OPERATION_TIMEOUT))
                    .map_err(kafka_unavailable)
            });
            Ok(())
        }

        fn begin_transaction(&self, token: OperationToken) -> Result<()> {
            if !self.transactional {
                return Err(CoreError::OperationUnsupported);
            }
            let initialize = !self.initialized.swap(true, Ordering::AcqRel);
            self.blocking_operation(token, move |producer| {
                if initialize {
                    producer
                        .init_transactions(Timeout::After(OPERATION_TIMEOUT))
                        .map_err(kafka_unavailable)?;
                }
                producer.begin_transaction().map_err(kafka_unavailable)
            });
            Ok(())
        }

        fn commit_transaction(&self, token: OperationToken) -> Result<()> {
            if !self.transactional {
                return Err(CoreError::OperationUnsupported);
            }
            self.blocking_operation(token, |producer| {
                producer
                    .commit_transaction(Timeout::After(OPERATION_TIMEOUT))
                    .map_err(kafka_unavailable)
            });
            Ok(())
        }

        fn rollback_transaction(&self, token: OperationToken) -> Result<()> {
            if !self.transactional {
                return Err(CoreError::OperationUnsupported);
            }
            self.blocking_operation(token, |producer| {
                producer
                    .abort_transaction(Timeout::After(OPERATION_TIMEOUT))
                    .map_err(kafka_unavailable)
            });
            Ok(())
        }

        fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
            Box::pin(async move {
                let producer = self.producer.clone();
                tokio::task::spawn_blocking(move || {
                    producer
                        .flush(Timeout::After(OPERATION_TIMEOUT))
                        .map_err(kafka_unavailable)
                })
                .await
                .map_err(|error| CoreError::Internal(format!("join Kafka writer close: {error}")))?
            })
        }
    }

    enum ReaderCommand {
        Subscribe {
            with_headers: bool,
        },
        Fetch {
            token: OperationToken,
            maximum: u32,
            with_headers: bool,
        },
        Settle {
            token: OperationToken,
            kind: SettlementKind,
            settlements: Vec<SettlementResult>,
        },
    }

    struct KafkaReader {
        auto_settle: bool,
        commands: mpsc::UnboundedSender<ReaderCommand>,
        shutdown: CancellationToken,
        worker: Mutex<Option<JoinHandle<()>>>,
    }

    impl std::fmt::Debug for KafkaReader {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("KafkaReader")
                .field("auto_settle", &self.auto_settle)
                .finish_non_exhaustive()
        }
    }

    impl KafkaReader {
        fn new(
            consumer: Arc<StreamConsumer>,
            auto_settle: bool,
            events: Arc<dyn ReaderEventSink>,
        ) -> Arc<Self> {
            let (commands, receiver) = mpsc::unbounded_channel();
            let shutdown = CancellationToken::new();
            let worker_shutdown = shutdown.clone();
            let worker = tokio::spawn(reader_worker(
                consumer,
                auto_settle,
                events,
                receiver,
                worker_shutdown,
            ));
            Arc::new(Self {
                auto_settle,
                commands,
                shutdown,
                worker: Mutex::new(Some(worker)),
            })
        }
    }

    impl Reader for KafkaReader {
        fn subscribe(&self, with_headers: bool, ready: ReadyCallback) -> Result<()> {
            ready()?;
            self.commands
                .send(ReaderCommand::Subscribe { with_headers })
                .map_err(|_| CoreError::Closed)
        }

        fn fetch(&self, token: OperationToken, maximum: u32, with_headers: bool) -> Result<()> {
            self.commands
                .send(ReaderCommand::Fetch {
                    token,
                    maximum,
                    with_headers,
                })
                .map_err(|_| CoreError::Closed)
        }

        fn settle(
            &self,
            token: OperationToken,
            kind: SettlementKind,
            settlements: Vec<SettlementResult>,
        ) -> Result<()> {
            if kind == SettlementKind::Nack {
                return Err(CoreError::OperationUnsupported);
            }
            self.commands
                .send(ReaderCommand::Settle {
                    token,
                    kind,
                    settlements,
                })
                .map_err(|_| CoreError::Closed)
        }

        fn adapter_message_id_prefix_len(&self) -> usize {
            14
        }

        fn auto_settle(&self) -> bool {
            self.auto_settle
        }

        fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
            Box::pin(async move {
                self.shutdown.cancel();
                let worker = self.worker.lock().take();
                if let Some(worker) = worker {
                    worker.await.map_err(|error| {
                        CoreError::Internal(format!("join Kafka reader worker: {error}"))
                    })?;
                }
                Ok(())
            })
        }
    }

    async fn reader_worker(
        consumer: Arc<StreamConsumer>,
        auto_settle: bool,
        events: Arc<dyn ReaderEventSink>,
        mut commands: mpsc::UnboundedReceiver<ReaderCommand>,
        shutdown: CancellationToken,
    ) {
        let mut subscription_headers = None;
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                command = commands.recv() => {
                    let Some(command) = command else { return; };
                    match command {
                        ReaderCommand::Subscribe { with_headers } => {
                            subscription_headers = Some(with_headers);
                        }
                        ReaderCommand::Fetch { token, maximum, with_headers } => {
                            let mut messages = Vec::with_capacity(maximum as usize);
                            let mut result = Ok(());
                            while messages.len() < maximum as usize {
                                match tokio::time::timeout(FETCH_IDLE_TIMEOUT, consumer.recv()).await {
                                    Ok(Ok(message)) => match reader_message(&message, with_headers, auto_settle) {
                                        Ok(message) => messages.push(message),
                                        Err(error) => {
                                            result = Err(error);
                                            break;
                                        }
                                    },
                                    Ok(Err(error)) => {
                                        result = Err(kafka_unavailable(error));
                                        break;
                                    }
                                    Err(_) => break,
                                }
                            }
                            let reported_count = u32::try_from(messages.len()).unwrap_or(maximum);
                            events.emit(ReaderEvent::FetchComplete {
                                token,
                                reported_count,
                                messages,
                                result,
                            });
                        }
                        ReaderCommand::Settle { token, kind, mut settlements } => {
                            debug_assert_eq!(kind, SettlementKind::Ack);
                            for settlement in &mut settlements {
                                settlement.result = commit_message_id(&consumer, &settlement.message_id);
                            }
                            events.emit(ReaderEvent::SettlementComplete {
                                token,
                                result: Ok(()),
                                messages: settlements,
                            });
                        }
                    }
                }
                message = consumer.recv(), if subscription_headers.is_some() => {
                    match message {
                        Ok(message) => match reader_message(
                            &message,
                            subscription_headers.unwrap_or(false),
                            auto_settle,
                        ) {
                            Ok(message) => events.emit(ReaderEvent::Message(message)),
                            Err(error) => {
                                events.emit(ReaderEvent::Terminal(Err(error)));
                                return;
                            }
                        },
                        Err(error) if subscription_error_is_recoverable(&error) => {}
                        Err(error) => {
                            events.emit(ReaderEvent::Terminal(Err(kafka_unavailable(error))));
                            return;
                        }
                    }
                }
            }
        }
    }

    fn subscription_error_is_recoverable(error: &KafkaError) -> bool {
        matches!(
            error,
            KafkaError::MessageConsumption(_)
                | KafkaError::PartitionEOF(_)
                | KafkaError::NoMessageReceived
        )
    }

    fn reader_message(
        message: &rdkafka::message::BorrowedMessage<'_>,
        with_headers: bool,
        auto_settle: bool,
    ) -> Result<Delivery> {
        let headers = if with_headers {
            message.headers().map_or_else(Vec::new, |headers| {
                (0..headers.count())
                    .map(|index| {
                        let header = headers.get(index);
                        Header {
                            key: Bytes::copy_from_slice(header.key.as_bytes()),
                            value: Bytes::copy_from_slice(header.value.unwrap_or_default()),
                        }
                    })
                    .collect()
            })
        } else {
            Vec::new()
        };
        Ok(Delivery {
            payload: Bytes::copy_from_slice(message.payload().unwrap_or_default()),
            headers: with_headers.then_some(headers),
            message_id: if auto_settle {
                None
            } else {
                Some(encode_message_id(
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )?)
            },
        })
    }

    fn encode_message_id(topic: &str, partition: i32, offset: i64) -> Result<Bytes> {
        let topic_length = u16::try_from(topic.len())
            .map_err(|_| CoreError::Internal("Kafka topic exceeds message ID encoding".into()))?;
        let mut encoded = BytesMut::with_capacity(14 + topic.len());
        encoded.put_u16(topic_length);
        encoded.extend_from_slice(topic.as_bytes());
        encoded.put_i32(partition);
        encoded.put_i64(offset);
        Ok(encoded.freeze())
    }

    fn commit_message_id(consumer: &StreamConsumer, message_id: &Bytes) -> Result<()> {
        if message_id.len() < 14 {
            return Err(CoreError::InvalidMessageId(
                "Kafka message ID is truncated".into(),
            ));
        }
        let topic_length = usize::from(u16::from_be_bytes([message_id[0], message_id[1]]));
        let expected = 14_usize
            .checked_add(topic_length)
            .ok_or_else(|| CoreError::InvalidMessageId("Kafka message ID is too large".into()))?;
        if message_id.len() != expected {
            return Err(CoreError::InvalidMessageId(
                "Kafka message ID length is invalid".into(),
            ));
        }
        let topic = std::str::from_utf8(&message_id[2..2 + topic_length])
            .map_err(|_| CoreError::InvalidMessageId("Kafka topic is not UTF-8".into()))?;
        let partition_start = 2 + topic_length;
        let partition = i32::from_be_bytes(
            message_id[partition_start..partition_start + 4]
                .try_into()
                .expect("validated Kafka partition bytes"),
        );
        let offset = i64::from_be_bytes(
            message_id[partition_start + 4..]
                .try_into()
                .expect("validated Kafka offset bytes"),
        );
        let committed = offset
            .checked_add(1)
            .ok_or_else(|| CoreError::InvalidMessageId("Kafka offset overflow".into()))?;
        let mut offsets = TopicPartitionList::new();
        offsets
            .add_partition_offset(topic, partition, Offset::Offset(committed))
            .map_err(kafka_unavailable)?;
        consumer
            .commit(&offsets, CommitMode::Sync)
            .map_err(kafka_unavailable)
    }

    fn kafka_unavailable(error: impl std::fmt::Display) -> CoreError {
        CoreError::Unavailable(format!("Kafka: {error}"))
    }

    #[must_use]
    pub fn descriptor() -> Arc<dyn ConnectorDescriptor> {
        Arc::new(KafkaDescriptor)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn compiles_route_capabilities_without_broker_io() {
            let settings = serde_json::json!({
                "common": { "brokers": ["localhost:9092"] },
                "routes": {
                    "events": {
                        "produce_topic": "events",
                        "consume_topics": ["events"],
                        "group": "workers",
                        "transactional_id": "fujin-events"
                    }
                }
            });

            let compiled = KafkaDescriptor
                .compile(&settings)
                .expect("compile Kafka config");
            let profile = compiled.routes()["events"];
            assert!(profile.capabilities.contains(Capabilities::PRODUCE));
            assert!(profile.capabilities.contains(Capabilities::TRANSACTIONS));
            assert!(profile.capabilities.contains(Capabilities::FETCH));
            assert!(profile.capabilities.contains(Capabilities::SUBSCRIBE));
            assert!(
                profile
                    .capabilities
                    .contains(Capabilities::MANUAL_SETTLEMENT)
            );
            assert_eq!(profile.produce_guarantee, AcceptanceGuarantee::Durable);
        }

        #[test]
        fn message_id_round_trip_validates_bounds() {
            let encoded = encode_message_id("events", 3, 42).expect("encode message ID");
            assert_eq!(encoded.len(), 20);
            assert!(matches!(
                commit_message_id_without_broker(&encoded),
                Ok(("events", 3, 43))
            ));
            assert!(commit_message_id_without_broker(&Bytes::from_static(b"short")).is_err());
        }

        #[test]
        fn subscription_continues_after_nonfatal_consumer_errors() {
            assert!(subscription_error_is_recoverable(
                &rdkafka::error::KafkaError::MessageConsumption(
                    rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition,
                ),
            ));
            assert!(!subscription_error_is_recoverable(
                &rdkafka::error::KafkaError::MessageConsumptionFatal(
                    rdkafka::error::RDKafkaErrorCode::TopicAuthorizationFailed,
                ),
            ));
        }

        fn commit_message_id_without_broker(message_id: &Bytes) -> Result<(&str, i32, i64)> {
            if message_id.len() < 14 {
                return Err(CoreError::InvalidMessageId("truncated".into()));
            }
            let topic_length = usize::from(u16::from_be_bytes([message_id[0], message_id[1]]));
            if message_id.len() != 14 + topic_length {
                return Err(CoreError::InvalidMessageId("length".into()));
            }
            let topic = std::str::from_utf8(&message_id[2..2 + topic_length])
                .map_err(|_| CoreError::InvalidMessageId("topic".into()))?;
            let start = 2 + topic_length;
            let partition = i32::from_be_bytes(message_id[start..start + 4].try_into().unwrap());
            let offset = i64::from_be_bytes(message_id[start + 4..].try_into().unwrap()) + 1;
            Ok((topic, partition, offset))
        }
    }
}

pub use implementation::{KafkaDescriptor, descriptor};

#[must_use]
pub fn plugin() -> fujin_connector::ConnectorPlugin {
    fujin_connector::ConnectorPlugin::new("kafka", KafkaDescriptor)
}

#[cfg(test)]
mod plugin_tests {
    #[test]
    fn plugin_uses_kafka_registered_name() {
        assert_eq!(super::plugin().name(), "kafka");
    }
}
