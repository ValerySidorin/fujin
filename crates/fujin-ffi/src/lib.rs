//! Versioned C ABI support for generated Fujin embedding libraries.

use std::{
    env,
    ffi::{CString, c_char},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
    sync::{Mutex, Once},
};

use anyhow::{Context, Result};
use fujin::{
    ApplicationBuilder, EmbeddedApplication, EmbeddedApplicationControl, EmbeddedRuntimeConfig,
    RuntimeConfig, configurator::ConnectorSnapshot,
};
use serde::Deserialize;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub const FUJIN_ABI_VERSION_V1: u32 = 1;
pub const FUJIN_STATUS_OK: i32 = 0;
pub const FUJIN_STATUS_INVALID_ARGUMENT: i32 = 1;
pub const FUJIN_STATUS_START_FAILED: i32 = 2;
pub const FUJIN_STATUS_RUNTIME_FAILED: i32 = 3;
pub const FUJIN_STATUS_PANIC: i32 = 255;
#[derive(Debug)]
struct InvalidArgument(String);

impl std::fmt::Display for InvalidArgument {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for InvalidArgument {}

fn invalid_argument(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(InvalidArgument(message.into()))
}

/// Opaque error owned by the caller until [`error_free_v1`].
#[derive(Debug)]
pub struct FujinError {
    message: CString,
}

/// Byte buffer owned by the caller until [`buffer_free_v1`].
#[derive(Debug)]
#[repr(C)]
pub struct FujinBuffer {
    pub data: *mut u8,
    pub length: usize,
}

/// Opaque application handle owned by a C caller.
#[derive(Debug)]
pub struct FujinHandle {
    control: EmbeddedApplicationControl,
    application: Mutex<Option<EmbeddedApplication>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct StartRequest {
    config: Option<RuntimeConfig>,
    runtime: RuntimeRequest,
    graceful_upgrade: bool,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RuntimeRequest {
    worker_threads: Option<usize>,
    thread_name: Option<String>,
}

impl RuntimeRequest {
    fn into_config(self) -> EmbeddedRuntimeConfig {
        let defaults = EmbeddedRuntimeConfig::default();
        EmbeddedRuntimeConfig {
            worker_threads: self.worker_threads,
            thread_name: self.thread_name.unwrap_or(defaults.thread_name),
        }
    }
}

fn initialize_logging() {
    static INITIALIZE: Once = Once::new();
    INITIALIZE.call_once(|| {
        let level = env::var("FUJIN_LOG_LEVEL").unwrap_or_else(|_| "INFO".into());
        let filter = if level.eq_ignore_ascii_case("OFF") {
            EnvFilter::new("off")
        } else {
            EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"))
        };
        let registry = tracing_subscriber::registry().with(filter);
        let _ = if env::var("FUJIN_LOG_TYPE").is_ok_and(|value| value.eq_ignore_ascii_case("json"))
        {
            registry
                .with(tracing_subscriber::fmt::layer().json())
                .try_init()
        } else {
            registry.with(tracing_subscriber::fmt::layer()).try_init()
        };
    });
}

/// Returns the ABI revision implemented by this library.
#[must_use]
pub const fn abi_version_v1() -> u32 {
    FUJIN_ABI_VERSION_V1
}

/// Starts a generated application and blocks until every listener is ready.
///
/// # Safety
/// `request` must reference `request_length` readable bytes. `output` must allow one pointer write.
/// `error` may be null or must allow one pointer write.
pub unsafe fn start_v1(
    mut builder: ApplicationBuilder,
    request: *const u8,
    request_length: usize,
    output: *mut *mut FujinHandle,
    error: *mut *mut FujinError,
) -> i32 {
    initialize_logging();
    unsafe {
        call(error, || {
            if output.is_null() {
                return Ok(failure(
                    error,
                    FUJIN_STATUS_INVALID_ARGUMENT,
                    "Fujin output handle pointer is null",
                ));
            }
            output.write(ptr::null_mut());
            let encoded = match input_bytes(request, request_length) {
                Ok(encoded) => encoded,
                Err(message) => {
                    return Ok(failure(error, FUJIN_STATUS_INVALID_ARGUMENT, message));
                }
            };
            let request: StartRequest = match serde_json::from_slice(encoded) {
                Ok(request) => request,
                Err(parse_error) => {
                    return Ok(failure(
                        error,
                        FUJIN_STATUS_INVALID_ARGUMENT,
                        format!("parse Fujin start request: {parse_error}"),
                    ));
                }
            };
            builder = builder.graceful_upgrade(request.graceful_upgrade);
            if let Some(config) = request.config {
                builder = builder.config(config);
            }
            let application =
                match EmbeddedApplication::start(builder, &request.runtime.into_config())
                    .context("start embedded Fujin application")
                {
                    Ok(application) => application,
                    Err(start_error) => {
                        return Ok(failure(error, FUJIN_STATUS_START_FAILED, start_error));
                    }
                };
            let control = application.control();
            output.write(Box::into_raw(Box::new(FujinHandle {
                control,
                application: Mutex::new(Some(application)),
            })));
            Ok(FUJIN_STATUS_OK)
        })
    }
}

/// Requests asynchronous application shutdown.
///
/// # Safety
/// `handle` must be a live pointer returned by [`start_v1`].
pub unsafe fn shutdown_v1(handle: *mut FujinHandle, error: *mut *mut FujinError) -> i32 {
    unsafe {
        with_handle(handle, error, |handle| {
            handle.control.request_shutdown();
            Ok(())
        })
    }
}

/// Waits for termination and joins the owned runtime thread.
///
/// # Safety
/// `handle` must be a live pointer returned by [`start_v1`].
pub unsafe fn wait_v1(handle: *mut FujinHandle, error: *mut *mut FujinError) -> i32 {
    unsafe {
        with_handle(handle, error, |handle| {
            let application = handle
                .application
                .lock()
                .map_err(|_| anyhow::anyhow!("Fujin handle lock poisoned"))?
                .take();
            application.map_or(Ok(()), EmbeddedApplication::wait)
        })
    }
}

/// Requests shutdown, joins the runtime, and releases the handle.
///
/// # Safety
/// `handle` must be null or a live pointer returned by [`start_v1`] and must not be reused.
pub unsafe fn free_v1(handle: *mut FujinHandle, error: *mut *mut FujinError) -> i32 {
    unsafe {
        call(error, || {
            if handle.is_null() {
                return Ok(FUJIN_STATUS_OK);
            }
            let handle = Box::from_raw(handle);
            let application = handle
                .application
                .into_inner()
                .map_err(|_| anyhow::anyhow!("Fujin handle lock poisoned"))?;
            if let Some(application) = application {
                application.shutdown()?;
            }
            Ok(FUJIN_STATUS_OK)
        })
    }
}

/// Writes the ready listener endpoints as JSON.
///
/// # Safety
/// Pointers follow the same rules as [`shutdown_v1`]; `output` must allow one structure write.
pub unsafe fn endpoints_json_v1(
    handle: *mut FujinHandle,
    output: *mut FujinBuffer,
    error: *mut *mut FujinError,
) -> i32 {
    unsafe {
        write_json(handle, output, error, |handle| {
            handle.control.endpoints().to_vec()
        })
    }
}

/// Writes the connector runtime and generation status as JSON.
///
/// # Safety
/// Pointers follow the same rules as [`endpoints_json_v1`].
pub unsafe fn runtime_status_json_v1(
    handle: *mut FujinHandle,
    output: *mut FujinBuffer,
    error: *mut *mut FujinError,
) -> i32 {
    unsafe {
        write_json(handle, output, error, |handle| {
            handle.control.catalog_status()
        })
    }
}

/// Reports whether a configurator watcher owns runtime connector state.
///
/// # Safety
/// Pointers follow the same rules as [`shutdown_v1`]; `output` must allow one byte write.
pub unsafe fn watches_connectors_v1(
    handle: *mut FujinHandle,
    output: *mut u8,
    error: *mut *mut FujinError,
) -> i32 {
    unsafe {
        with_handle(handle, error, |handle| {
            if output.is_null() {
                return Err(invalid_argument(
                    "Fujin watches-connectors output pointer is null",
                ));
            }
            output.write(u8::from(handle.control.watches_connectors()));
            Ok(())
        })
    }
}

/// Applies a complete connector snapshot encoded as JSON and writes the apply result as JSON.
///
/// # Safety
/// `request` and output pointers must be valid for their declared lengths and types.
pub unsafe fn reload_connectors_json_v1(
    handle: *mut FujinHandle,
    request: *const u8,
    request_length: usize,
    output: *mut FujinBuffer,
    error: *mut *mut FujinError,
) -> i32 {
    unsafe {
        with_handle(handle, error, |handle| {
            require_output(output)?;
            let encoded = input_bytes(request, request_length).map_err(invalid_argument)?;
            let snapshot: ConnectorSnapshot = serde_json::from_slice(encoded).map_err(|error| {
                invalid_argument(format!("parse Fujin connector snapshot: {error}"))
            })?;
            write_buffer(
                output,
                serde_json::to_vec(&handle.control.reload_connectors(snapshot))?,
            );
            Ok(())
        })
    }
}

/// Reloads connectors from the retained configurator and writes the apply result as JSON.
///
/// # Safety
/// Pointers follow the same rules as [`endpoints_json_v1`].
pub unsafe fn reload_from_configurator_json_v1(
    handle: *mut FujinHandle,
    output: *mut FujinBuffer,
    error: *mut *mut FujinError,
) -> i32 {
    unsafe {
        with_handle(handle, error, |handle| {
            require_output(output)?;
            let result = handle.control.reload_from_configurator()?;
            write_buffer(output, serde_json::to_vec(&result)?);
            Ok(())
        })
    }
}

/// Releases a buffer returned by this ABI.
///
/// # Safety
/// `buffer` must be untouched output from one successful Fujin ABI call or an empty buffer.
#[allow(clippy::needless_pass_by_value)] // C ABI owns and consumes this by-value structure.
pub unsafe fn buffer_free_v1(buffer: FujinBuffer) {
    if !buffer.data.is_null() {
        unsafe {
            drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
                buffer.data,
                buffer.length,
            )));
        }
    }
}

/// Returns an error message valid until [`error_free_v1`] is called.
///
/// # Safety
/// `error` must be null or a live pointer written by this ABI.
#[must_use]
pub unsafe fn error_message_v1(error: *const FujinError) -> *const c_char {
    unsafe { error.as_ref() }.map_or(ptr::null(), |error| error.message.as_ptr())
}

/// Releases an error returned by this ABI.
///
/// # Safety
/// `error` must be null or a live pointer written by this ABI and must not be reused.
pub unsafe fn error_free_v1(error: *mut FujinError) {
    if !error.is_null() {
        unsafe { drop(Box::from_raw(error)) };
    }
}

unsafe fn write_json<T: serde::Serialize>(
    handle: *mut FujinHandle,
    output: *mut FujinBuffer,
    error: *mut *mut FujinError,
    value: impl FnOnce(&FujinHandle) -> T,
) -> i32 {
    unsafe {
        with_handle(handle, error, |handle| {
            require_output(output)?;
            write_buffer(output, serde_json::to_vec(&value(handle))?);
            Ok(())
        })
    }
}

unsafe fn with_handle(
    handle: *mut FujinHandle,
    error: *mut *mut FujinError,
    operation: impl FnOnce(&FujinHandle) -> Result<()>,
) -> i32 {
    unsafe {
        call(error, || {
            let Some(handle) = handle.as_ref() else {
                return Ok(failure(
                    error,
                    FUJIN_STATUS_INVALID_ARGUMENT,
                    "Fujin handle pointer is null",
                ));
            };
            operation(handle)?;
            Ok(FUJIN_STATUS_OK)
        })
    }
}

unsafe fn call(error: *mut *mut FujinError, operation: impl FnOnce() -> Result<i32>) -> i32 {
    if !error.is_null() {
        unsafe { error.write(ptr::null_mut()) };
    }
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(status)) => status,
        Ok(Err(runtime_error)) => unsafe {
            let status = if runtime_error.downcast_ref::<InvalidArgument>().is_some() {
                FUJIN_STATUS_INVALID_ARGUMENT
            } else {
                FUJIN_STATUS_RUNTIME_FAILED
            };
            failure(error, status, runtime_error)
        },
        Err(_) => unsafe {
            failure(
                error,
                FUJIN_STATUS_PANIC,
                "panic crossed Fujin C ABI boundary",
            )
        },
    }
}

unsafe fn failure(
    output: *mut *mut FujinError,
    status: i32,
    message: impl std::fmt::Display,
) -> i32 {
    if !output.is_null() {
        let message = message.to_string().replace('\0', "\\0");
        let error = FujinError {
            message: CString::new(message).expect("NUL bytes replaced"),
        };
        unsafe { output.write(Box::into_raw(Box::new(error))) };
    }
    status
}

unsafe fn input_bytes<'a>(data: *const u8, length: usize) -> std::result::Result<&'a [u8], String> {
    if length == 0 {
        return Ok(&[]);
    }
    if data.is_null() {
        return Err("Fujin input data pointer is null".into());
    }
    Ok(unsafe { std::slice::from_raw_parts(data, length) })
}

fn require_output(output: *mut FujinBuffer) -> Result<()> {
    if output.is_null() {
        return Err(invalid_argument("Fujin output buffer pointer is null"));
    }
    Ok(())
}

unsafe fn write_buffer(output: *mut FujinBuffer, encoded: Vec<u8>) {
    let boxed = encoded.into_boxed_slice();
    let length = boxed.len();
    let data = Box::into_raw(boxed).cast::<u8>();
    unsafe { output.write(FujinBuffer { data, length }) };
}

/// Exports the stable Fujin C ABI around one generated [`ApplicationBuilder`] expression.
#[macro_export]
macro_rules! export_c_api {
    ($builder:expr) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn fujin_abi_version_v1() -> u32 {
            $crate::abi_version_v1()
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn fujin_build_version_v1() -> *const ::std::ffi::c_char {
            concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr().cast()
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn fujin_start_v1(
            request: *const u8,
            request_length: usize,
            output: *mut *mut $crate::FujinHandle,
            error: *mut *mut $crate::FujinError,
        ) -> i32 {
            unsafe { $crate::start_v1($builder, request, request_length, output, error) }
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn fujin_shutdown_v1(
            handle: *mut $crate::FujinHandle,
            error: *mut *mut $crate::FujinError,
        ) -> i32 {
            unsafe { $crate::shutdown_v1(handle, error) }
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn fujin_wait_v1(
            handle: *mut $crate::FujinHandle,
            error: *mut *mut $crate::FujinError,
        ) -> i32 {
            unsafe { $crate::wait_v1(handle, error) }
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn fujin_free_v1(
            handle: *mut $crate::FujinHandle,
            error: *mut *mut $crate::FujinError,
        ) -> i32 {
            unsafe { $crate::free_v1(handle, error) }
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn fujin_endpoints_json_v1(
            handle: *mut $crate::FujinHandle,
            output: *mut $crate::FujinBuffer,
            error: *mut *mut $crate::FujinError,
        ) -> i32 {
            unsafe { $crate::endpoints_json_v1(handle, output, error) }
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn fujin_runtime_status_json_v1(
            handle: *mut $crate::FujinHandle,
            output: *mut $crate::FujinBuffer,
            error: *mut *mut $crate::FujinError,
        ) -> i32 {
            unsafe { $crate::runtime_status_json_v1(handle, output, error) }
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn fujin_watches_connectors_v1(
            handle: *mut $crate::FujinHandle,
            output: *mut u8,
            error: *mut *mut $crate::FujinError,
        ) -> i32 {
            unsafe { $crate::watches_connectors_v1(handle, output, error) }
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn fujin_reload_connectors_json_v1(
            handle: *mut $crate::FujinHandle,
            request: *const u8,
            request_length: usize,
            output: *mut $crate::FujinBuffer,
            error: *mut *mut $crate::FujinError,
        ) -> i32 {
            unsafe {
                $crate::reload_connectors_json_v1(handle, request, request_length, output, error)
            }
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn fujin_reload_from_configurator_json_v1(
            handle: *mut $crate::FujinHandle,
            output: *mut $crate::FujinBuffer,
            error: *mut *mut $crate::FujinError,
        ) -> i32 {
            unsafe { $crate::reload_from_configurator_json_v1(handle, output, error) }
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn fujin_buffer_free_v1(buffer: $crate::FujinBuffer) {
            unsafe { $crate::buffer_free_v1(buffer) }
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn fujin_error_message_v1(
            error: *const $crate::FujinError,
        ) -> *const ::std::ffi::c_char {
            unsafe { $crate::error_message_v1(error) }
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn fujin_error_free_v1(error: *mut $crate::FujinError) {
            unsafe { $crate::error_free_v1(error) }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    #[test]
    fn malformed_start_request_returns_owned_error() {
        let mut handle = ptr::null_mut();
        let mut error = ptr::null_mut();
        let encoded = b"{";
        let status = unsafe {
            start_v1(
                fujin::Application::builder(),
                encoded.as_ptr(),
                encoded.len(),
                &raw mut handle,
                &raw mut error,
            )
        };
        assert_eq!(status, FUJIN_STATUS_INVALID_ARGUMENT);
        assert!(handle.is_null());
        assert!(!error.is_null());
        let message = unsafe { CStr::from_ptr(error_message_v1(error)) }
            .to_str()
            .expect("UTF-8 error message");
        assert!(message.contains("parse Fujin start request"));
        unsafe { error_free_v1(error) };
    }

    #[test]
    fn null_handle_returns_owned_error() {
        let mut error = ptr::null_mut();
        assert_eq!(
            unsafe { shutdown_v1(ptr::null_mut(), &raw mut error) },
            FUJIN_STATUS_INVALID_ARGUMENT
        );
        assert!(!error.is_null());
        unsafe { error_free_v1(error) };
    }

    #[test]
    fn lifecycle_exposes_endpoints_status_and_reload() {
        let request = serde_json::to_vec(&serde_json::json!({
            "config": {
                "fujin": {
                    "transports": [{"type": "tcp", "settings": {"addr": "127.0.0.1:0"}}]
                },
                "grpc": {"enabled": false},
                "connectors": {}
            },
            "runtime": {"worker_threads": 1}
        }))
        .expect("encode start request");
        let mut handle = ptr::null_mut();
        let mut error = ptr::null_mut();
        assert_eq!(
            unsafe {
                start_v1(
                    fujin::Application::builder().transport(fujin_transport_tcp::plugin()),
                    request.as_ptr(),
                    request.len(),
                    &raw mut handle,
                    &raw mut error,
                )
            },
            FUJIN_STATUS_OK
        );
        assert!(!handle.is_null());
        assert!(error.is_null());

        let endpoints =
            output_json(|output, error| unsafe { endpoints_json_v1(handle, output, error) });
        assert_eq!(endpoints[0]["interface"], "native");
        assert_eq!(endpoints[0]["transport"], "tcp");
        assert_ne!(endpoints[0]["address"], "127.0.0.1:0");

        let status =
            output_json(|output, error| unsafe { runtime_status_json_v1(handle, output, error) });
        assert_eq!(status["active_revision"], 0);
        let malformed = b"{";
        let mut ignored = FujinBuffer {
            data: ptr::null_mut(),
            length: 0,
        };
        assert_eq!(
            unsafe {
                reload_connectors_json_v1(
                    handle,
                    malformed.as_ptr(),
                    malformed.len(),
                    &raw mut ignored,
                    &raw mut error,
                )
            },
            FUJIN_STATUS_INVALID_ARGUMENT
        );
        assert!(!error.is_null());
        unsafe { error_free_v1(error) };
        error = ptr::null_mut();

        let snapshot = br#"{"revision":1,"connectors":{}}"#;
        let apply = output_json(|output, error| unsafe {
            reload_connectors_json_v1(handle, snapshot.as_ptr(), snapshot.len(), output, error)
        });
        assert_eq!(apply["state"], "accepted");
        assert_eq!(apply["revision"], 1);

        assert_eq!(
            unsafe { shutdown_v1(handle, &raw mut error) },
            FUJIN_STATUS_OK
        );
        assert_eq!(unsafe { wait_v1(handle, &raw mut error) }, FUJIN_STATUS_OK);
        assert_eq!(unsafe { free_v1(handle, &raw mut error) }, FUJIN_STATUS_OK);
    }

    fn output_json(
        call: impl FnOnce(*mut FujinBuffer, *mut *mut FujinError) -> i32,
    ) -> serde_json::Value {
        let mut output = FujinBuffer {
            data: ptr::null_mut(),
            length: 0,
        };
        let mut error = ptr::null_mut();
        assert_eq!(call(&raw mut output, &raw mut error), FUJIN_STATUS_OK);
        assert!(error.is_null());
        let encoded = unsafe { std::slice::from_raw_parts(output.data, output.length) };
        let value = serde_json::from_slice(encoded).expect("parse ABI JSON output");
        unsafe { buffer_free_v1(output) };
        value
    }

    #[test]
    fn input_bytes_accepts_empty_null_input() {
        assert_eq!(unsafe { input_bytes(ptr::null(), 0) }, Ok(&[][..]));
    }
}
