use bytes::{BufMut, Bytes, BytesMut};
use fujin_core::{
    BindResult, CoreError, Delivery, FetchResult, Header, OperationError, Result as CoreResult,
    RouteProfile, SettlementResult, StatusCode,
};

use crate::{HELLO_FORMAT, NativeError, ResponseCode, WIRE_VERSION};

pub(crate) fn hello_success(server_build: &str) -> Result<Bytes, NativeError> {
    let mut output = BytesMut::new();
    output.put_u8(ResponseCode::Hello as u8);
    output.put_u8(0);
    output.put_u8(HELLO_FORMAT);
    output.put_u8(WIRE_VERSION);
    put_string(&mut output, server_build)?;
    Ok(output.freeze())
}

pub(crate) fn hello_failure(
    code: StatusCode,
    reason: &str,
    message: &str,
) -> Result<Bytes, NativeError> {
    let mut output = BytesMut::new();
    output.put_u8(ResponseCode::Hello as u8);
    put_operation_error(
        &mut output,
        &OperationError::new(
            code,
            fujin_core::OperationOutcome::NotApplied,
            reason,
            message,
        ),
    )?;
    Ok(output.freeze())
}

pub(crate) fn bind(result: &CoreResult<BindResult>) -> Result<Bytes, NativeError> {
    let mut output = BytesMut::new();
    output.put_u8(ResponseCode::Bind as u8);
    match result {
        Ok(bound) => {
            output.put_u8(0);
            output.put_u32(checked_u32(bound.routes.len())?);
            for (route, profile) in &bound.routes {
                put_string(&mut output, route)?;
                put_profile(&mut output, *profile);
            }
        }
        Err(error) => put_core_error(&mut output, error)?,
    }
    Ok(output.freeze())
}

pub(crate) fn operation(
    code: ResponseCode,
    correlation_id: u32,
    result: &CoreResult<()>,
) -> Result<Bytes, NativeError> {
    let mut output = BytesMut::with_capacity(6);
    output.put_u8(code as u8);
    output.put_u32(correlation_id);
    put_result(&mut output, result)?;
    Ok(output.freeze())
}

pub(crate) fn subscribe(
    code: ResponseCode,
    correlation_id: u32,
    result: &CoreResult<u8>,
) -> Result<Bytes, NativeError> {
    let mut output = BytesMut::with_capacity(7);
    output.put_u8(code as u8);
    output.put_u32(correlation_id);
    match result {
        Ok(subscription_id) => {
            output.put_u8(0);
            output.put_u8(*subscription_id);
        }
        Err(error) => put_core_error(&mut output, error)?,
    }
    Ok(output.freeze())
}

pub(crate) fn fetch(
    code: ResponseCode,
    correlation_id: u32,
    result: &CoreResult<FetchResult>,
) -> Result<Bytes, NativeError> {
    let capacity = match result {
        Ok(fetched) => fetched
            .messages
            .iter()
            .try_fold(11_usize, |size, message| {
                checked_add(size, delivery_body_wire_len(message)?)
            })?,
        Err(_) => 6,
    };
    let mut output = BytesMut::with_capacity(capacity);
    output.put_u8(code as u8);
    output.put_u32(correlation_id);
    match result {
        Ok(fetched) => {
            output.put_u8(0);
            output.put_u8(fetched.subscription_id);
            output.put_u32(checked_u32(fetched.messages.len())?);
            for message in &fetched.messages {
                put_delivery_body(&mut output, message)?;
            }
        }
        Err(error) => put_core_error(&mut output, error)?,
    }
    Ok(output.freeze())
}

pub(crate) fn settlement(
    code: ResponseCode,
    correlation_id: u32,
    result: &CoreResult<Vec<SettlementResult>>,
) -> Result<Bytes, NativeError> {
    let capacity = match result {
        Ok(results) => results.iter().try_fold(10_usize, |size, message| {
            checked_add(size, checked_add(bytes_wire_len(&message.message_id)?, 1)?)
        })?,
        Err(_) => 6,
    };
    let mut output = BytesMut::with_capacity(capacity);
    output.put_u8(code as u8);
    output.put_u32(correlation_id);
    match result {
        Ok(results) => {
            output.put_u8(0);
            output.put_u32(checked_u32(results.len())?);
            for message in results {
                put_bytes(&mut output, &message.message_id)?;
                put_result(&mut output, &message.result)?;
            }
        }
        Err(error) => put_core_error(&mut output, error)?,
    }
    Ok(output.freeze())
}

pub(crate) fn delivery(subscription_id: u8, delivery: &Delivery) -> Result<Bytes, NativeError> {
    let code = if delivery.headers.is_some() {
        ResponseCode::HMessage
    } else {
        ResponseCode::Message
    };
    let mut output = BytesMut::with_capacity(checked_add(2, delivery_body_wire_len(delivery)?)?);
    output.put_u8(code as u8);
    output.put_u8(subscription_id);
    put_delivery_body(&mut output, delivery)?;
    Ok(output.freeze())
}

pub(crate) fn disconnect() -> Bytes {
    Bytes::from_static(&[ResponseCode::Disconnect as u8])
}

fn delivery_body_wire_len(delivery: &Delivery) -> Result<usize, NativeError> {
    let mut size = bytes_wire_len(&delivery.payload)?;
    if let Some(message_id) = &delivery.message_id {
        size = checked_add(size, bytes_wire_len(message_id)?)?;
    }
    if let Some(headers) = &delivery.headers {
        let strings = headers
            .len()
            .checked_mul(2)
            .ok_or(NativeError::FrameTooLarge)?;
        checked_u16(strings)?;
        size = checked_add(size, 2)?;
        for header in headers {
            size = checked_add(size, bytes_wire_len(&header.key)?)?;
            size = checked_add(size, bytes_wire_len(&header.value)?)?;
        }
    }
    Ok(size)
}

fn bytes_wire_len(value: &[u8]) -> Result<usize, NativeError> {
    checked_u32(value.len())?;
    checked_add(4, value.len())
}

fn checked_add(left: usize, right: usize) -> Result<usize, NativeError> {
    left.checked_add(right).ok_or(NativeError::FrameTooLarge)
}

fn put_delivery_body(output: &mut BytesMut, delivery: &Delivery) -> Result<(), NativeError> {
    if let Some(headers) = &delivery.headers {
        put_headers(output, headers)?;
    }
    if let Some(message_id) = &delivery.message_id {
        put_bytes(output, message_id)?;
    }
    put_bytes(output, &delivery.payload)
}

fn put_result(output: &mut BytesMut, result: &CoreResult<()>) -> Result<(), NativeError> {
    match result {
        Ok(()) => output.put_u8(0),
        Err(error) => put_core_error(output, error)?,
    }
    Ok(())
}

fn put_core_error(output: &mut BytesMut, error: &CoreError) -> Result<(), NativeError> {
    put_operation_error(output, &OperationError::from(error))
}

fn put_operation_error(output: &mut BytesMut, error: &OperationError) -> Result<(), NativeError> {
    output.put_u8(error.code as u8);
    output.put_u8(error.outcome as u8);
    put_string(output, &error.reason)?;
    put_string(output, &error.message)?;
    output.put_u16(checked_u16(error.details.len())?);
    for (key, value) in &error.details {
        put_string(output, key)?;
        put_string(output, value)?;
    }
    Ok(())
}

fn put_profile(output: &mut BytesMut, profile: RouteProfile) {
    output.put_u8(profile.capabilities.bits());
    output.put_u8(profile.produce_guarantee as u8);
    output.put_u8(profile.settlement.ack as u8);
    output.put_u8(profile.settlement.nack as u8);
}

fn put_headers(output: &mut BytesMut, headers: &[Header]) -> Result<(), NativeError> {
    let strings = headers
        .len()
        .checked_mul(2)
        .ok_or(NativeError::FrameTooLarge)?;
    output.put_u16(checked_u16(strings)?);
    for header in headers {
        put_bytes(output, &header.key)?;
        put_bytes(output, &header.value)?;
    }
    Ok(())
}

fn put_string(output: &mut BytesMut, value: &str) -> Result<(), NativeError> {
    put_bytes(output, value.as_bytes())
}

fn put_bytes(output: &mut BytesMut, value: &[u8]) -> Result<(), NativeError> {
    output.put_u32(checked_u32(value.len())?);
    output.extend_from_slice(value);
    Ok(())
}

fn checked_u32(value: usize) -> Result<u32, NativeError> {
    u32::try_from(value).map_err(|_| NativeError::FrameTooLarge)
}

fn checked_u16(value: usize) -> Result<u16, NativeError> {
    u16::try_from(value).map_err(|_| NativeError::FrameTooLarge)
}
