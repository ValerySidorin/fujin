use std::collections::BTreeMap;

use bytes::Bytes;
use fujin_core::Header;

pub const HELLO_FORMAT: u8 = 1;
pub const WIRE_VERSION: u8 = 1;
pub const DEFAULT_MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum RequestCode {
    Hello = 0,
    Bind = 1,
    Produce = 2,
    HProduce = 3,
    BeginTransaction = 4,
    CommitTransaction = 5,
    RollbackTransaction = 6,
    Fetch = 7,
    HFetch = 8,
    Ack = 9,
    Nack = 10,
    Subscribe = 11,
    HSubscribe = 12,
    Unsubscribe = 13,
    Disconnect = 14,
    TransactionProduce = 15,
    TransactionHProduce = 16,
    Pong = 99,
}

impl TryFrom<u8> for RequestCode {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Hello),
            1 => Ok(Self::Bind),
            2 => Ok(Self::Produce),
            3 => Ok(Self::HProduce),
            4 => Ok(Self::BeginTransaction),
            5 => Ok(Self::CommitTransaction),
            6 => Ok(Self::RollbackTransaction),
            7 => Ok(Self::Fetch),
            8 => Ok(Self::HFetch),
            9 => Ok(Self::Ack),
            10 => Ok(Self::Nack),
            11 => Ok(Self::Subscribe),
            12 => Ok(Self::HSubscribe),
            13 => Ok(Self::Unsubscribe),
            14 => Ok(Self::Disconnect),
            15 => Ok(Self::TransactionProduce),
            16 => Ok(Self::TransactionHProduce),
            99 => Ok(Self::Pong),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ResponseCode {
    Subscribe = 1,
    HSubscribe = 2,
    Produce = 3,
    HProduce = 4,
    BeginTransaction = 5,
    CommitTransaction = 6,
    RollbackTransaction = 7,
    Message = 8,
    HMessage = 9,
    Fetch = 10,
    HFetch = 11,
    Ack = 12,
    Nack = 13,
    Unsubscribe = 14,
    Disconnect = 15,
    Bind = 16,
    TransactionProduce = 17,
    TransactionHProduce = 18,
    Hello = 19,
    Ping = 99,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HelloRequest {
    pub format: u8,
    pub versions: Vec<u8>,
    pub client_name: String,
    pub client_build: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Request {
    Hello(HelloRequest),
    Bind {
        connector: String,
        metadata: BTreeMap<String, String>,
        overrides: BTreeMap<String, String>,
    },
    Produce {
        correlation_id: u32,
        route: Bytes,
        message: Bytes,
        headers: Option<Vec<Header>>,
    },
    BeginTransaction {
        correlation_id: u32,
        route: String,
    },
    CommitTransaction {
        correlation_id: u32,
    },
    RollbackTransaction {
        correlation_id: u32,
    },
    TransactionProduce {
        correlation_id: u32,
        message: Bytes,
        headers: Option<Vec<Header>>,
    },
    Subscribe {
        correlation_id: u32,
        route: String,
        auto_settle: bool,
        with_headers: bool,
    },
    Fetch {
        correlation_id: u32,
        route: String,
        auto_settle: bool,
        with_headers: bool,
        maximum: u32,
    },
    Ack {
        correlation_id: u32,
        subscription_id: u8,
        message_ids: Vec<Bytes>,
    },
    Nack {
        correlation_id: u32,
        subscription_id: u8,
        message_ids: Vec<Bytes>,
    },
    Unsubscribe {
        correlation_id: u32,
        subscription_id: u8,
    },
    Disconnect,
    Pong,
}
