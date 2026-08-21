use std::collections::BTreeMap;

use bytes::{Buf, Bytes, BytesMut};
use fujin_core::Header;

use crate::{DEFAULT_MAX_FRAME_SIZE, HelloRequest, NativeError, Request, RequestCode};

#[derive(Clone, Copy, Debug)]
enum PairTarget {
    Metadata,
    Overrides,
}

#[derive(Clone, Debug)]
struct BindPairs {
    connector: String,
    metadata: BTreeMap<String, String>,
    overrides: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
struct ProduceHeaders {
    route: Bytes,
    values: Vec<Header>,
}

#[derive(Clone, Debug)]
enum State {
    Opcode,
    HelloFormat,
    HelloVersionCount {
        format: u8,
    },
    HelloVersions {
        format: u8,
        count: usize,
    },
    HelloClientNameLength {
        format: u8,
        versions: Vec<u8>,
    },
    HelloClientName {
        format: u8,
        versions: Vec<u8>,
        length: usize,
    },
    HelloClientBuildLength {
        format: u8,
        versions: Vec<u8>,
        client_name: String,
    },
    HelloClientBuild {
        format: u8,
        versions: Vec<u8>,
        client_name: String,
        length: usize,
    },
    BindConnectorLength,
    BindConnector {
        length: usize,
    },
    BindMetadataCount {
        connector: String,
    },
    BindOverridesCount {
        connector: String,
        metadata: BTreeMap<String, String>,
    },
    BindPairKeyLength {
        pairs: Box<BindPairs>,
        target: PairTarget,
        remaining: u16,
    },
    BindPairKey {
        pairs: Box<BindPairs>,
        target: PairTarget,
        remaining: u16,
        length: usize,
    },
    BindPairValueLength {
        pairs: Box<BindPairs>,
        target: PairTarget,
        remaining: u16,
        key: String,
    },
    BindPairValue {
        pairs: Box<BindPairs>,
        target: PairTarget,
        remaining: u16,
        key: String,
        length: usize,
    },
    ProduceCorrelation {
        with_headers: bool,
        transactional: bool,
    },
    ProduceRouteLength {
        with_headers: bool,
        correlation_id: u32,
    },
    ProduceRoute {
        with_headers: bool,
        correlation_id: u32,
        length: usize,
    },
    ProduceHeadersCount {
        transactional: bool,
        correlation_id: u32,
        route: Bytes,
    },
    ProduceHeaderLength {
        transactional: bool,
        correlation_id: u32,
        fields: Box<ProduceHeaders>,
        remaining: u16,
        key: Option<Bytes>,
    },
    ProduceHeaderBytes {
        transactional: bool,
        correlation_id: u32,
        fields: Box<ProduceHeaders>,
        remaining: u16,
        key: Option<Bytes>,
        length: usize,
    },
    ProduceMessageLength {
        transactional: bool,
        correlation_id: u32,
        route: Bytes,
        headers: Option<Vec<Header>>,
    },
    ProduceMessage {
        transactional: bool,
        correlation_id: u32,
        route: Bytes,
        headers: Option<Vec<Header>>,
        length: usize,
    },
    BeginCorrelation,
    BeginRouteLength {
        correlation_id: u32,
    },
    BeginRoute {
        correlation_id: u32,
        length: usize,
    },
    ControlCorrelation {
        rollback: bool,
    },
    ReadCorrelation {
        with_headers: bool,
        fetch: bool,
    },
    ReadAutoSettle {
        with_headers: bool,
        fetch: bool,
        correlation_id: u32,
    },
    ReadRouteLength {
        with_headers: bool,
        fetch: bool,
        correlation_id: u32,
        auto_settle: bool,
    },
    ReadRoute {
        with_headers: bool,
        fetch: bool,
        correlation_id: u32,
        auto_settle: bool,
        length: usize,
    },
    ReadMaximum {
        with_headers: bool,
        correlation_id: u32,
        auto_settle: bool,
        route: String,
    },
    SettlementCorrelation {
        nack: bool,
    },
    SettlementSubscription {
        nack: bool,
        correlation_id: u32,
    },
    SettlementCount {
        nack: bool,
        correlation_id: u32,
        subscription_id: u8,
    },
    SettlementMessageLength {
        nack: bool,
        correlation_id: u32,
        subscription_id: u8,
        remaining: u32,
        message_ids: Vec<Bytes>,
    },
    SettlementMessage {
        nack: bool,
        correlation_id: u32,
        subscription_id: u8,
        remaining: u32,
        message_ids: Vec<Bytes>,
        length: usize,
    },
    UnsubscribeCorrelation,
    UnsubscribeSubscription {
        correlation_id: u32,
    },
}

#[derive(Clone, Debug)]
pub struct Decoder {
    max_frame_size: usize,
    frame_size: usize,
    state: State,
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_FRAME_SIZE)
    }
}

impl Decoder {
    #[must_use]
    pub const fn new(max_frame_size: usize) -> Self {
        Self {
            max_frame_size,
            frame_size: 0,
            state: State::Opcode,
        }
    }

    /// Incrementally decodes one request without rescanning bytes consumed by earlier calls.
    /// Complete byte fields are split from `source`; payload, headers, and message IDs retain
    /// zero-copy `Bytes` ownership.
    ///
    /// # Errors
    ///
    /// Returns [`NativeError::Malformed`] for invalid opcodes, booleans, UTF-8, header counts, or
    /// fields, and [`NativeError::FrameTooLarge`] before accepting a frame larger than the maximum.
    pub fn decode(&mut self, source: &mut BytesMut) -> Result<Option<Request>, NativeError> {
        let result = if matches!(self.state, State::Opcode) {
            match self.decode_complete_produce(source) {
                Ok(Some(request)) => Ok(Some(request)),
                Ok(None) => self.decode_inner(source),
                Err(error) => Err(error),
            }
        } else {
            self.decode_inner(source)
        };
        if result.is_err() {
            self.state = State::Opcode;
            self.frame_size = 0;
        }
        result
    }

    fn decode_complete_produce(
        &mut self,
        source: &mut BytesMut,
    ) -> Result<Option<Request>, NativeError> {
        const FIXED_PREFIX_BYTES: usize = 1 + 4 + 4;
        if source.first().copied() != Some(RequestCode::Produce as u8)
            || source.len() < FIXED_PREFIX_BYTES
        {
            return Ok(None);
        }

        let correlation_id = u32::from_be_bytes(source[1..5].try_into().expect("fixed prefix"));
        let route_length = usize::try_from(u32::from_be_bytes(
            source[5..9].try_into().expect("fixed prefix"),
        ))
        .map_err(|_| NativeError::FrameTooLarge)?;
        if route_length == 0 {
            return Err(NativeError::Malformed("route is empty"));
        }
        let route_end = FIXED_PREFIX_BYTES
            .checked_add(route_length)
            .ok_or(NativeError::FrameTooLarge)?;
        if route_end > self.max_frame_size {
            return Err(NativeError::FrameTooLarge);
        }
        if source.len() < route_end {
            return Ok(None);
        }
        std::str::from_utf8(&source[FIXED_PREFIX_BYTES..route_end])
            .map_err(|_| NativeError::Malformed("route is not UTF-8"))?;

        let message_length_end = route_end.checked_add(4).ok_or(NativeError::FrameTooLarge)?;
        if message_length_end > self.max_frame_size {
            return Err(NativeError::FrameTooLarge);
        }
        if source.len() < message_length_end {
            return Ok(None);
        }
        let message_length = usize::try_from(u32::from_be_bytes(
            source[route_end..message_length_end]
                .try_into()
                .expect("message length"),
        ))
        .map_err(|_| NativeError::FrameTooLarge)?;
        let frame_end = message_length_end
            .checked_add(message_length)
            .ok_or(NativeError::FrameTooLarge)?;
        if frame_end > self.max_frame_size {
            return Err(NativeError::FrameTooLarge);
        }
        if source.len() < frame_end {
            return Ok(None);
        }

        let frame = source.split_to(frame_end).freeze();
        let route = frame.slice(FIXED_PREFIX_BYTES..route_end);
        let message = frame.slice(message_length_end..frame_end);
        Ok(Some(Request::Produce {
            correlation_id,
            route,
            message,
            headers: None,
        }))
    }

    #[allow(clippy::too_many_lines)]
    fn decode_inner(&mut self, source: &mut BytesMut) -> Result<Option<Request>, NativeError> {
        loop {
            let state = std::mem::replace(&mut self.state, State::Opcode);
            match state {
                State::Opcode => {
                    let Some(code) = self.byte(source)? else {
                        self.state = State::Opcode;
                        return Ok(None);
                    };
                    let code = RequestCode::try_from(code)
                        .map_err(|()| NativeError::Malformed("unknown opcode"))?;
                    self.state = match code {
                        RequestCode::Hello => State::HelloFormat,
                        RequestCode::Bind => State::BindConnectorLength,
                        RequestCode::Produce => State::ProduceCorrelation {
                            with_headers: false,
                            transactional: false,
                        },
                        RequestCode::HProduce => State::ProduceCorrelation {
                            with_headers: true,
                            transactional: false,
                        },
                        RequestCode::TransactionProduce => State::ProduceCorrelation {
                            with_headers: false,
                            transactional: true,
                        },
                        RequestCode::TransactionHProduce => State::ProduceCorrelation {
                            with_headers: true,
                            transactional: true,
                        },
                        RequestCode::BeginTransaction => State::BeginCorrelation,
                        RequestCode::CommitTransaction => {
                            State::ControlCorrelation { rollback: false }
                        }
                        RequestCode::RollbackTransaction => {
                            State::ControlCorrelation { rollback: true }
                        }
                        RequestCode::Subscribe => State::ReadCorrelation {
                            with_headers: false,
                            fetch: false,
                        },
                        RequestCode::HSubscribe => State::ReadCorrelation {
                            with_headers: true,
                            fetch: false,
                        },
                        RequestCode::Fetch => State::ReadCorrelation {
                            with_headers: false,
                            fetch: true,
                        },
                        RequestCode::HFetch => State::ReadCorrelation {
                            with_headers: true,
                            fetch: true,
                        },
                        RequestCode::Ack => State::SettlementCorrelation { nack: false },
                        RequestCode::Nack => State::SettlementCorrelation { nack: true },
                        RequestCode::Unsubscribe => State::UnsubscribeCorrelation,
                        RequestCode::Disconnect => {
                            return Ok(Some(self.finish(Request::Disconnect)));
                        }
                        RequestCode::Pong => return Ok(Some(self.finish(Request::Pong))),
                    };
                }
                State::HelloFormat => {
                    let Some(format) = self.byte(source)? else {
                        self.state = State::HelloFormat;
                        return Ok(None);
                    };
                    self.state = State::HelloVersionCount { format };
                }
                State::HelloVersionCount { format } => {
                    let Some(count) = self.byte(source)? else {
                        self.state = State::HelloVersionCount { format };
                        return Ok(None);
                    };
                    self.state = State::HelloVersions {
                        format,
                        count: usize::from(count),
                    };
                }
                State::HelloVersions { format, count } => {
                    let Some(versions) = self.bytes(source, count)? else {
                        self.state = State::HelloVersions { format, count };
                        return Ok(None);
                    };
                    self.state = State::HelloClientNameLength {
                        format,
                        versions: versions.to_vec(),
                    };
                }
                State::HelloClientNameLength { format, versions } => {
                    let Some(length) = self.length(source)? else {
                        self.state = State::HelloClientNameLength { format, versions };
                        return Ok(None);
                    };
                    self.state = State::HelloClientName {
                        format,
                        versions,
                        length,
                    };
                }
                State::HelloClientName {
                    format,
                    versions,
                    length,
                } => {
                    let Some(value) = self.bytes(source, length)? else {
                        self.state = State::HelloClientName {
                            format,
                            versions,
                            length,
                        };
                        return Ok(None);
                    };
                    let client_name = string(&value)?;
                    self.state = State::HelloClientBuildLength {
                        format,
                        versions,
                        client_name,
                    };
                }
                State::HelloClientBuildLength {
                    format,
                    versions,
                    client_name,
                } => {
                    let Some(length) = self.length(source)? else {
                        self.state = State::HelloClientBuildLength {
                            format,
                            versions,
                            client_name,
                        };
                        return Ok(None);
                    };
                    self.state = State::HelloClientBuild {
                        format,
                        versions,
                        client_name,
                        length,
                    };
                }
                State::HelloClientBuild {
                    format,
                    versions,
                    client_name,
                    length,
                } => {
                    let Some(value) = self.bytes(source, length)? else {
                        self.state = State::HelloClientBuild {
                            format,
                            versions,
                            client_name,
                            length,
                        };
                        return Ok(None);
                    };
                    return Ok(Some(self.finish(Request::Hello(HelloRequest {
                        format,
                        versions,
                        client_name,
                        client_build: string(&value)?,
                    }))));
                }
                State::BindConnectorLength => {
                    let Some(length) = self.length(source)? else {
                        self.state = State::BindConnectorLength;
                        return Ok(None);
                    };
                    self.state = State::BindConnector { length };
                }
                State::BindConnector { length } => {
                    let Some(value) = self.bytes(source, length)? else {
                        self.state = State::BindConnector { length };
                        return Ok(None);
                    };
                    let connector = string(&value)?;
                    if connector.is_empty() {
                        return Err(NativeError::Malformed("connector name is empty"));
                    }
                    self.state = State::BindMetadataCount { connector };
                }
                State::BindMetadataCount { connector } => {
                    let Some(remaining) = self.u16(source)? else {
                        self.state = State::BindMetadataCount { connector };
                        return Ok(None);
                    };
                    if remaining == 0 {
                        self.state = State::BindOverridesCount {
                            connector,
                            metadata: BTreeMap::new(),
                        };
                    } else {
                        self.state = State::BindPairKeyLength {
                            pairs: Box::new(BindPairs {
                                connector,
                                metadata: BTreeMap::new(),
                                overrides: BTreeMap::new(),
                            }),
                            target: PairTarget::Metadata,
                            remaining,
                        };
                    }
                }
                State::BindOverridesCount {
                    connector,
                    metadata,
                } => {
                    let Some(remaining) = self.u16(source)? else {
                        self.state = State::BindOverridesCount {
                            connector,
                            metadata,
                        };
                        return Ok(None);
                    };
                    if remaining == 0 {
                        return Ok(Some(self.finish(Request::Bind {
                            connector,
                            metadata,
                            overrides: BTreeMap::new(),
                        })));
                    }
                    self.state = State::BindPairKeyLength {
                        pairs: Box::new(BindPairs {
                            connector,
                            metadata,
                            overrides: BTreeMap::new(),
                        }),
                        target: PairTarget::Overrides,
                        remaining,
                    };
                }
                State::BindPairKeyLength {
                    pairs,
                    target,
                    remaining,
                } => {
                    let Some(length) = self.length(source)? else {
                        self.state = State::BindPairKeyLength {
                            pairs,
                            target,
                            remaining,
                        };
                        return Ok(None);
                    };
                    self.state = State::BindPairKey {
                        pairs,
                        target,
                        remaining,
                        length,
                    };
                }
                State::BindPairKey {
                    pairs,
                    target,
                    remaining,
                    length,
                } => {
                    let Some(value) = self.bytes(source, length)? else {
                        self.state = State::BindPairKey {
                            pairs,
                            target,
                            remaining,
                            length,
                        };
                        return Ok(None);
                    };
                    self.state = State::BindPairValueLength {
                        pairs,
                        target,
                        remaining,
                        key: string(&value)?,
                    };
                }
                State::BindPairValueLength {
                    pairs,
                    target,
                    remaining,
                    key,
                } => {
                    let Some(length) = self.length(source)? else {
                        self.state = State::BindPairValueLength {
                            pairs,
                            target,
                            remaining,
                            key,
                        };
                        return Ok(None);
                    };
                    self.state = State::BindPairValue {
                        pairs,
                        target,
                        remaining,
                        key,
                        length,
                    };
                }
                State::BindPairValue {
                    mut pairs,
                    target,
                    remaining,
                    key,
                    length,
                } => {
                    let Some(value) = self.bytes(source, length)? else {
                        self.state = State::BindPairValue {
                            pairs,
                            target,
                            remaining,
                            key,
                            length,
                        };
                        return Ok(None);
                    };
                    let value = string(&value)?;
                    match target {
                        PairTarget::Metadata => {
                            pairs.metadata.insert(key, value);
                        }
                        PairTarget::Overrides => {
                            pairs.overrides.insert(key, value);
                        }
                    }
                    if remaining > 1 {
                        self.state = State::BindPairKeyLength {
                            pairs,
                            target,
                            remaining: remaining - 1,
                        };
                    } else {
                        match target {
                            PairTarget::Metadata => {
                                let BindPairs {
                                    connector,
                                    metadata,
                                    ..
                                } = *pairs;
                                self.state = State::BindOverridesCount {
                                    connector,
                                    metadata,
                                };
                            }
                            PairTarget::Overrides => {
                                let BindPairs {
                                    connector,
                                    metadata,
                                    overrides,
                                } = *pairs;
                                return Ok(Some(self.finish(Request::Bind {
                                    connector,
                                    metadata,
                                    overrides,
                                })));
                            }
                        }
                    }
                }
                State::ProduceCorrelation {
                    with_headers,
                    transactional,
                } => {
                    let Some(correlation_id) = self.u32(source)? else {
                        self.state = State::ProduceCorrelation {
                            with_headers,
                            transactional,
                        };
                        return Ok(None);
                    };
                    if transactional {
                        self.state =
                            next_produce_state(with_headers, true, correlation_id, Bytes::new());
                    } else {
                        self.state = State::ProduceRouteLength {
                            with_headers,
                            correlation_id,
                        };
                    }
                }
                State::ProduceRouteLength {
                    with_headers,
                    correlation_id,
                } => {
                    let Some(length) = self.length(source)? else {
                        self.state = State::ProduceRouteLength {
                            with_headers,
                            correlation_id,
                        };
                        return Ok(None);
                    };
                    if length == 0 {
                        return Err(NativeError::Malformed("route is empty"));
                    }
                    self.state = State::ProduceRoute {
                        with_headers,
                        correlation_id,
                        length,
                    };
                }
                State::ProduceRoute {
                    with_headers,
                    correlation_id,
                    length,
                } => {
                    let Some(route) = self.bytes(source, length)? else {
                        self.state = State::ProduceRoute {
                            with_headers,
                            correlation_id,
                            length,
                        };
                        return Ok(None);
                    };
                    std::str::from_utf8(&route)
                        .map_err(|_| NativeError::Malformed("route is not UTF-8"))?;
                    self.state = next_produce_state(with_headers, false, correlation_id, route);
                }
                State::ProduceHeadersCount {
                    transactional,
                    correlation_id,
                    route,
                } => {
                    let Some(remaining) = self.u16(source)? else {
                        self.state = State::ProduceHeadersCount {
                            transactional,
                            correlation_id,
                            route,
                        };
                        return Ok(None);
                    };
                    if remaining % 2 != 0 {
                        return Err(NativeError::Malformed("header string count must be even"));
                    }
                    if remaining == 0 {
                        self.state = State::ProduceMessageLength {
                            transactional,
                            correlation_id,
                            route,
                            headers: Some(Vec::new()),
                        };
                    } else {
                        self.state = State::ProduceHeaderLength {
                            transactional,
                            correlation_id,
                            fields: Box::new(ProduceHeaders {
                                route,
                                values: Vec::with_capacity(usize::from(remaining / 2)),
                            }),
                            remaining,
                            key: None,
                        };
                    }
                }
                State::ProduceHeaderLength {
                    transactional,
                    correlation_id,
                    fields,
                    remaining,
                    key,
                } => {
                    let Some(length) = self.length(source)? else {
                        self.state = State::ProduceHeaderLength {
                            transactional,
                            correlation_id,
                            fields,
                            remaining,
                            key,
                        };
                        return Ok(None);
                    };
                    self.state = State::ProduceHeaderBytes {
                        transactional,
                        correlation_id,
                        fields,
                        remaining,
                        key,
                        length,
                    };
                }
                State::ProduceHeaderBytes {
                    transactional,
                    correlation_id,
                    mut fields,
                    remaining,
                    key,
                    length,
                } => {
                    let Some(value) = self.bytes(source, length)? else {
                        self.state = State::ProduceHeaderBytes {
                            transactional,
                            correlation_id,
                            fields,
                            remaining,
                            key,
                            length,
                        };
                        return Ok(None);
                    };
                    let next_remaining = remaining - 1;
                    if let Some(key) = key {
                        fields.values.push(Header { key, value });
                        if next_remaining == 0 {
                            let ProduceHeaders { route, values } = *fields;
                            self.state = State::ProduceMessageLength {
                                transactional,
                                correlation_id,
                                route,
                                headers: Some(values),
                            };
                        } else {
                            self.state = State::ProduceHeaderLength {
                                transactional,
                                correlation_id,
                                fields,
                                remaining: next_remaining,
                                key: None,
                            };
                        }
                    } else {
                        if value.is_empty() {
                            return Err(NativeError::Malformed("header key is empty"));
                        }
                        std::str::from_utf8(&value)
                            .map_err(|_| NativeError::Malformed("header key is not UTF-8"))?;
                        self.state = State::ProduceHeaderLength {
                            transactional,
                            correlation_id,
                            fields,
                            remaining: next_remaining,
                            key: Some(value),
                        };
                    }
                }
                State::ProduceMessageLength {
                    transactional,
                    correlation_id,
                    route,
                    headers,
                } => {
                    let Some(length) = self.length(source)? else {
                        self.state = State::ProduceMessageLength {
                            transactional,
                            correlation_id,
                            route,
                            headers,
                        };
                        return Ok(None);
                    };
                    self.state = State::ProduceMessage {
                        transactional,
                        correlation_id,
                        route,
                        headers,
                        length,
                    };
                }
                State::ProduceMessage {
                    transactional,
                    correlation_id,
                    route,
                    headers,
                    length,
                } => {
                    let Some(message) = self.bytes(source, length)? else {
                        self.state = State::ProduceMessage {
                            transactional,
                            correlation_id,
                            route,
                            headers,
                            length,
                        };
                        return Ok(None);
                    };
                    let request = if transactional {
                        Request::TransactionProduce {
                            correlation_id,
                            message,
                            headers,
                        }
                    } else {
                        Request::Produce {
                            correlation_id,
                            route,
                            message,
                            headers,
                        }
                    };
                    return Ok(Some(self.finish(request)));
                }
                State::BeginCorrelation => {
                    let Some(correlation_id) = self.u32(source)? else {
                        self.state = State::BeginCorrelation;
                        return Ok(None);
                    };
                    self.state = State::BeginRouteLength { correlation_id };
                }
                State::BeginRouteLength { correlation_id } => {
                    let Some(length) = self.length(source)? else {
                        self.state = State::BeginRouteLength { correlation_id };
                        return Ok(None);
                    };
                    self.state = State::BeginRoute {
                        correlation_id,
                        length,
                    };
                }
                State::BeginRoute {
                    correlation_id,
                    length,
                } => {
                    let Some(route) = self.bytes(source, length)? else {
                        self.state = State::BeginRoute {
                            correlation_id,
                            length,
                        };
                        return Ok(None);
                    };
                    return Ok(Some(self.finish(Request::BeginTransaction {
                        correlation_id,
                        route: nonempty_string(&route, "route is empty")?,
                    })));
                }
                State::ControlCorrelation { rollback } => {
                    let Some(correlation_id) = self.u32(source)? else {
                        self.state = State::ControlCorrelation { rollback };
                        return Ok(None);
                    };
                    let request = if rollback {
                        Request::RollbackTransaction { correlation_id }
                    } else {
                        Request::CommitTransaction { correlation_id }
                    };
                    return Ok(Some(self.finish(request)));
                }
                State::ReadCorrelation {
                    with_headers,
                    fetch,
                } => {
                    let Some(correlation_id) = self.u32(source)? else {
                        self.state = State::ReadCorrelation {
                            with_headers,
                            fetch,
                        };
                        return Ok(None);
                    };
                    self.state = State::ReadAutoSettle {
                        with_headers,
                        fetch,
                        correlation_id,
                    };
                }
                State::ReadAutoSettle {
                    with_headers,
                    fetch,
                    correlation_id,
                } => {
                    let Some(auto_settle) = self.boolean(source)? else {
                        self.state = State::ReadAutoSettle {
                            with_headers,
                            fetch,
                            correlation_id,
                        };
                        return Ok(None);
                    };
                    self.state = State::ReadRouteLength {
                        with_headers,
                        fetch,
                        correlation_id,
                        auto_settle,
                    };
                }
                State::ReadRouteLength {
                    with_headers,
                    fetch,
                    correlation_id,
                    auto_settle,
                } => {
                    let Some(length) = self.length(source)? else {
                        self.state = State::ReadRouteLength {
                            with_headers,
                            fetch,
                            correlation_id,
                            auto_settle,
                        };
                        return Ok(None);
                    };
                    self.state = State::ReadRoute {
                        with_headers,
                        fetch,
                        correlation_id,
                        auto_settle,
                        length,
                    };
                }
                State::ReadRoute {
                    with_headers,
                    fetch,
                    correlation_id,
                    auto_settle,
                    length,
                } => {
                    let Some(route) = self.bytes(source, length)? else {
                        self.state = State::ReadRoute {
                            with_headers,
                            fetch,
                            correlation_id,
                            auto_settle,
                            length,
                        };
                        return Ok(None);
                    };
                    let route = nonempty_string(&route, "route is empty")?;
                    if fetch {
                        self.state = State::ReadMaximum {
                            with_headers,
                            correlation_id,
                            auto_settle,
                            route,
                        };
                    } else {
                        return Ok(Some(self.finish(Request::Subscribe {
                            correlation_id,
                            route,
                            auto_settle,
                            with_headers,
                        })));
                    }
                }
                State::ReadMaximum {
                    with_headers,
                    correlation_id,
                    auto_settle,
                    route,
                } => {
                    let Some(maximum) = self.u32(source)? else {
                        self.state = State::ReadMaximum {
                            with_headers,
                            correlation_id,
                            auto_settle,
                            route,
                        };
                        return Ok(None);
                    };
                    return Ok(Some(self.finish(Request::Fetch {
                        correlation_id,
                        route,
                        auto_settle,
                        with_headers,
                        maximum,
                    })));
                }
                State::SettlementCorrelation { nack } => {
                    let Some(correlation_id) = self.u32(source)? else {
                        self.state = State::SettlementCorrelation { nack };
                        return Ok(None);
                    };
                    self.state = State::SettlementSubscription {
                        nack,
                        correlation_id,
                    };
                }
                State::SettlementSubscription {
                    nack,
                    correlation_id,
                } => {
                    let Some(subscription_id) = self.byte(source)? else {
                        self.state = State::SettlementSubscription {
                            nack,
                            correlation_id,
                        };
                        return Ok(None);
                    };
                    self.state = State::SettlementCount {
                        nack,
                        correlation_id,
                        subscription_id,
                    };
                }
                State::SettlementCount {
                    nack,
                    correlation_id,
                    subscription_id,
                } => {
                    let Some(remaining) = self.u32(source)? else {
                        self.state = State::SettlementCount {
                            nack,
                            correlation_id,
                            subscription_id,
                        };
                        return Ok(None);
                    };
                    if remaining == 0 {
                        return Ok(Some(self.finish(settlement_request(
                            nack,
                            correlation_id,
                            subscription_id,
                            Vec::new(),
                        ))));
                    }
                    self.state = State::SettlementMessageLength {
                        nack,
                        correlation_id,
                        subscription_id,
                        remaining,
                        message_ids: Vec::with_capacity(
                            usize::try_from(remaining).map_err(|_| NativeError::FrameTooLarge)?,
                        ),
                    };
                }
                State::SettlementMessageLength {
                    nack,
                    correlation_id,
                    subscription_id,
                    remaining,
                    message_ids,
                } => {
                    let Some(length) = self.length(source)? else {
                        self.state = State::SettlementMessageLength {
                            nack,
                            correlation_id,
                            subscription_id,
                            remaining,
                            message_ids,
                        };
                        return Ok(None);
                    };
                    self.state = State::SettlementMessage {
                        nack,
                        correlation_id,
                        subscription_id,
                        remaining,
                        message_ids,
                        length,
                    };
                }
                State::SettlementMessage {
                    nack,
                    correlation_id,
                    subscription_id,
                    remaining,
                    mut message_ids,
                    length,
                } => {
                    let Some(message_id) = self.bytes(source, length)? else {
                        self.state = State::SettlementMessage {
                            nack,
                            correlation_id,
                            subscription_id,
                            remaining,
                            message_ids,
                            length,
                        };
                        return Ok(None);
                    };
                    message_ids.push(message_id);
                    if remaining == 1 {
                        return Ok(Some(self.finish(settlement_request(
                            nack,
                            correlation_id,
                            subscription_id,
                            message_ids,
                        ))));
                    }
                    self.state = State::SettlementMessageLength {
                        nack,
                        correlation_id,
                        subscription_id,
                        remaining: remaining - 1,
                        message_ids,
                    };
                }
                State::UnsubscribeCorrelation => {
                    let Some(correlation_id) = self.u32(source)? else {
                        self.state = State::UnsubscribeCorrelation;
                        return Ok(None);
                    };
                    self.state = State::UnsubscribeSubscription { correlation_id };
                }
                State::UnsubscribeSubscription { correlation_id } => {
                    let Some(subscription_id) = self.byte(source)? else {
                        self.state = State::UnsubscribeSubscription { correlation_id };
                        return Ok(None);
                    };
                    return Ok(Some(self.finish(Request::Unsubscribe {
                        correlation_id,
                        subscription_id,
                    })));
                }
            }
        }
    }

    fn finish(&mut self, request: Request) -> Request {
        self.frame_size = 0;
        self.state = State::Opcode;
        request
    }

    fn reserve(&self, length: usize) -> Result<(), NativeError> {
        let end = self
            .frame_size
            .checked_add(length)
            .ok_or(NativeError::FrameTooLarge)?;
        if end > self.max_frame_size {
            return Err(NativeError::FrameTooLarge);
        }
        Ok(())
    }

    fn bytes(
        &mut self,
        source: &mut BytesMut,
        length: usize,
    ) -> Result<Option<Bytes>, NativeError> {
        self.reserve(length)?;
        if source.len() < length {
            return Ok(None);
        }
        self.frame_size += length;
        Ok(Some(source.split_to(length).freeze()))
    }

    fn byte(&mut self, source: &mut BytesMut) -> Result<Option<u8>, NativeError> {
        self.reserve(1)?;
        if source.is_empty() {
            return Ok(None);
        }
        self.frame_size += 1;
        Ok(Some(source.get_u8()))
    }

    fn boolean(&mut self, source: &mut BytesMut) -> Result<Option<bool>, NativeError> {
        let Some(value) = self.byte(source)? else {
            return Ok(None);
        };
        match value {
            0 => Ok(Some(false)),
            1 => Ok(Some(true)),
            _ => Err(NativeError::Malformed("boolean must be 0 or 1")),
        }
    }

    fn u16(&mut self, source: &mut BytesMut) -> Result<Option<u16>, NativeError> {
        self.reserve(2)?;
        if source.len() < 2 {
            return Ok(None);
        }
        self.frame_size += 2;
        Ok(Some(source.get_u16()))
    }

    fn u32(&mut self, source: &mut BytesMut) -> Result<Option<u32>, NativeError> {
        self.reserve(4)?;
        if source.len() < 4 {
            return Ok(None);
        }
        self.frame_size += 4;
        Ok(Some(source.get_u32()))
    }

    fn length(&mut self, source: &mut BytesMut) -> Result<Option<usize>, NativeError> {
        self.u32(source)?
            .map(|value| usize::try_from(value).map_err(|_| NativeError::FrameTooLarge))
            .transpose()
    }
}

fn next_produce_state(
    with_headers: bool,
    transactional: bool,
    correlation_id: u32,
    route: Bytes,
) -> State {
    if with_headers {
        State::ProduceHeadersCount {
            transactional,
            correlation_id,
            route,
        }
    } else {
        State::ProduceMessageLength {
            transactional,
            correlation_id,
            route,
            headers: None,
        }
    }
}

fn string(value: &Bytes) -> Result<String, NativeError> {
    std::str::from_utf8(value)
        .map(str::to_owned)
        .map_err(|_| NativeError::Malformed("string is not UTF-8"))
}

fn nonempty_string(value: &Bytes, empty: &'static str) -> Result<String, NativeError> {
    if value.is_empty() {
        return Err(NativeError::Malformed(empty));
    }
    string(value)
}

fn settlement_request(
    nack: bool,
    correlation_id: u32,
    subscription_id: u8,
    message_ids: Vec<Bytes>,
) -> Request {
    if nack {
        Request::Nack {
            correlation_id,
            subscription_id,
            message_ids,
        }
    } else {
        Request::Ack {
            correlation_id,
            subscription_id,
            message_ids,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn append_bytes(buffer: &mut Vec<u8>, value: &[u8]) {
        buffer.extend_from_slice(
            &u32::try_from(value.len())
                .expect("test length")
                .to_be_bytes(),
        );
        buffer.extend_from_slice(value);
    }

    #[test]
    fn preserves_fragmented_frame_and_decodes_coalesced_frames() {
        let mut decoder = Decoder::default();
        let mut first = vec![
            RequestCode::Hello as u8,
            crate::HELLO_FORMAT,
            1,
            crate::WIRE_VERSION,
        ];
        append_bytes(&mut first, b"client");
        append_bytes(&mut first, b"build");
        let second = vec![RequestCode::Disconnect as u8];
        let mut source = BytesMut::new();

        for byte in &first[..first.len() - 1] {
            source.extend_from_slice(&[*byte]);
            assert_eq!(decoder.decode(&mut source).expect("partial decode"), None);
        }
        source.extend_from_slice(&first[first.len() - 1..]);
        source.extend_from_slice(&second);

        assert!(matches!(
            decoder.decode(&mut source).expect("HELLO decode"),
            Some(Request::Hello(_))
        ));
        assert_eq!(
            decoder.decode(&mut source).expect("disconnect decode"),
            Some(Request::Disconnect)
        );
        assert!(source.is_empty());
    }

    #[test]
    fn decodes_fragmented_and_coalesced_produce_on_single_pass_path() {
        let mut frame = vec![RequestCode::Produce as u8];
        frame.extend_from_slice(&7_u32.to_be_bytes());
        append_bytes(&mut frame, b"route");
        append_bytes(&mut frame, b"payload");

        for split in 0..frame.len() {
            let mut decoder = Decoder::default();
            let mut source = BytesMut::from(&frame[..split]);
            assert_eq!(decoder.decode(&mut source).expect("partial produce"), None);
            source.extend_from_slice(&frame[split..]);
            source.extend_from_slice(&[RequestCode::Disconnect as u8]);
            assert_eq!(
                decoder.decode(&mut source).expect("complete produce"),
                Some(Request::Produce {
                    correlation_id: 7,
                    route: Bytes::from_static(b"route"),
                    message: Bytes::from_static(b"payload"),
                    headers: None,
                })
            );
            assert_eq!(
                decoder.decode(&mut source).expect("coalesced disconnect"),
                Some(Request::Disconnect)
            );
        }
    }

    #[test]
    fn rejects_invalid_boolean_and_odd_header_count() {
        let mut decoder = Decoder::default();
        let mut invalid_bool = BytesMut::from(&[RequestCode::Subscribe as u8, 0, 0, 0, 1, 2][..]);
        assert!(matches!(
            decoder.decode(&mut invalid_bool),
            Err(NativeError::Malformed("boolean must be 0 or 1"))
        ));

        let mut odd_headers = BytesMut::from(
            &[
                RequestCode::HProduce as u8,
                0,
                0,
                0,
                1,
                0,
                0,
                0,
                1,
                b'r',
                0,
                1,
            ][..],
        );
        assert!(matches!(
            decoder.decode(&mut odd_headers),
            Err(NativeError::Malformed("header string count must be even"))
        ));
    }

    #[test]
    fn decodes_headered_produce_without_copying_payload_storage() {
        let mut decoder = Decoder::default();
        let mut frame = vec![RequestCode::HProduce as u8];
        frame.extend_from_slice(&7_u32.to_be_bytes());
        append_bytes(&mut frame, b"route");
        frame.extend_from_slice(&2_u16.to_be_bytes());
        append_bytes(&mut frame, b"key");
        append_bytes(&mut frame, b"value");
        append_bytes(&mut frame, b"payload");
        let mut source = BytesMut::from(frame.as_slice());

        let Some(Request::Produce {
            correlation_id,
            route,
            message,
            headers,
        }) = decoder.decode(&mut source).expect("decode produce")
        else {
            panic!("expected produce request");
        };
        assert_eq!(correlation_id, 7);
        assert_eq!(route, "route");
        assert_eq!(message, Bytes::from_static(b"payload"));
        assert_eq!(headers.expect("headers")[0].key, Bytes::from_static(b"key"));
    }

    #[test]
    fn every_opcode_decodes_under_byte_fragmentation() {
        let mut frames = Vec::new();
        let mut bind = vec![RequestCode::Bind as u8];
        append_bytes(&mut bind, b"connector");
        bind.extend_from_slice(&1_u16.to_be_bytes());
        append_bytes(&mut bind, b"meta");
        append_bytes(&mut bind, b"value");
        bind.extend_from_slice(&1_u16.to_be_bytes());
        append_bytes(&mut bind, b"override");
        append_bytes(&mut bind, b"setting");
        frames.push(bind);
        let mut hproduce = vec![RequestCode::HProduce as u8];
        hproduce.extend_from_slice(&1_u32.to_be_bytes());
        append_bytes(&mut hproduce, b"route");
        hproduce.extend_from_slice(&2_u16.to_be_bytes());
        append_bytes(&mut hproduce, b"key");
        append_bytes(&mut hproduce, b"value");
        append_bytes(&mut hproduce, b"message");
        frames.push(hproduce);
        let mut tx = vec![RequestCode::TransactionProduce as u8];
        tx.extend_from_slice(&2_u32.to_be_bytes());
        append_bytes(&mut tx, b"message");
        frames.push(tx);
        let mut txh = vec![RequestCode::TransactionHProduce as u8];
        txh.extend_from_slice(&3_u32.to_be_bytes());
        txh.extend_from_slice(&0_u16.to_be_bytes());
        append_bytes(&mut txh, b"message");
        frames.push(txh);
        let mut begin = vec![RequestCode::BeginTransaction as u8];
        begin.extend_from_slice(&4_u32.to_be_bytes());
        append_bytes(&mut begin, b"route");
        frames.push(begin);
        frames.push(
            [
                vec![RequestCode::CommitTransaction as u8],
                5_u32.to_be_bytes().to_vec(),
            ]
            .concat(),
        );
        frames.push(
            [
                vec![RequestCode::RollbackTransaction as u8],
                6_u32.to_be_bytes().to_vec(),
            ]
            .concat(),
        );
        for code in [RequestCode::Subscribe, RequestCode::HSubscribe] {
            let mut frame = vec![code as u8];
            frame.extend_from_slice(&7_u32.to_be_bytes());
            frame.push(1);
            append_bytes(&mut frame, b"route");
            frames.push(frame);
        }
        for code in [RequestCode::Fetch, RequestCode::HFetch] {
            let mut frame = vec![code as u8];
            frame.extend_from_slice(&8_u32.to_be_bytes());
            frame.push(0);
            append_bytes(&mut frame, b"route");
            frame.extend_from_slice(&32_u32.to_be_bytes());
            frames.push(frame);
        }
        for code in [RequestCode::Ack, RequestCode::Nack] {
            let mut frame = vec![code as u8];
            frame.extend_from_slice(&9_u32.to_be_bytes());
            frame.push(1);
            frame.extend_from_slice(&2_u32.to_be_bytes());
            append_bytes(&mut frame, b"one");
            append_bytes(&mut frame, b"two");
            frames.push(frame);
        }
        frames.push(
            [
                vec![RequestCode::Unsubscribe as u8],
                10_u32.to_be_bytes().to_vec(),
                vec![1],
            ]
            .concat(),
        );
        frames.push(vec![RequestCode::Disconnect as u8]);
        frames.push(vec![RequestCode::Pong as u8]);

        for frame in frames {
            let mut decoder = Decoder::default();
            let mut source = BytesMut::new();
            for (index, byte) in frame.iter().enumerate() {
                source.extend_from_slice(&[*byte]);
                let request = decoder.decode(&mut source).expect("fragment decode");
                if index + 1 == frame.len() {
                    assert!(request.is_some());
                } else {
                    assert!(request.is_none());
                }
            }
        }
    }
}
