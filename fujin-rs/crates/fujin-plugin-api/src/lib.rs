#![allow(unsafe_code)]

//! Process-lifetime dynamic connector loading for Fujin's Rust runtime.
//!
//! Plugins and the host must be built from the same Fujin source revision and Rust toolchain. The
//! exported C entry point stabilizes symbol discovery, while descriptor trait objects retain Rust's
//! compiler-specific layout.

use std::{
    ffi::c_void,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use fujin_core::{ConnectorDescriptor, DescriptorRegistry};
use libloading::Library;

/// Host/plugin ABI version checked before registration.
pub const CONNECTOR_PLUGIN_ABI_VERSION: u32 = 1;
/// Exported plugin entry-point symbol, including its trailing NUL for `libloading`.
pub const CONNECTOR_PLUGIN_SYMBOL: &[u8] = b"fujin_connector_plugin_v1\0";

/// Host callback table passed to a connector plugin entry point.
#[derive(Debug)]
#[repr(C)]
pub struct ConnectorPluginRegistrar {
    pub abi_version: u32,
    pub context: *mut c_void,
    /// Consumes `descriptor` on every return path.
    pub register_descriptor: unsafe extern "C" fn(
        context: *mut c_void,
        name: *const u8,
        name_len: usize,
        descriptor: *mut c_void,
    ) -> i32,
}

/// Dynamic connector plugin entry-point signature.
pub type ConnectorPluginEntry = unsafe extern "C" fn(*mut ConnectorPluginRegistrar) -> i32;

/// Loaded process-lifetime libraries. Keep this value alive while any registered descriptor,
/// compiled connector, runtime, reader, or writer from those libraries may still be referenced.
#[derive(Debug)]
pub struct LoadedConnectorPlugins {
    libraries: Vec<Library>,
}

impl LoadedConnectorPlugins {
    #[must_use]
    pub fn len(&self) -> usize {
        self.libraries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.libraries.is_empty()
    }
}

/// Loads connector plugins and synchronously registers every exported descriptor.
///
/// # Errors
///
/// Returns an error when a library cannot be loaded, has no compatible entry point, exports no
/// descriptors, or registration fails.
///
/// Loading a library executes trusted operator-supplied code. Plugins must be built from the same
/// Fujin source revision and Rust toolchain as the host.
pub fn load_connector_plugins(
    paths: impl IntoIterator<Item = impl AsRef<Path>>,
    registry: &DescriptorRegistry,
) -> Result<LoadedConnectorPlugins> {
    let mut libraries = Vec::new();
    for path in paths {
        let path = path.as_ref();
        // SAFETY: The caller accepts plugin initialization and ABI requirements documented above.
        let library = unsafe { Library::new(path) }
            .with_context(|| format!("load connector plugin {}", path.display()))?;
        let mut state = RegistrationState {
            registry,
            error: None,
            registrations: 0,
        };
        let mut registrar = ConnectorPluginRegistrar {
            abi_version: CONNECTOR_PLUGIN_ABI_VERSION,
            context: std::ptr::from_mut(&mut state).cast(),
            register_descriptor,
        };
        let code = {
            // SAFETY: Symbol type and name are the versioned plugin ABI contract.
            let entry = unsafe { library.get::<ConnectorPluginEntry>(CONNECTOR_PLUGIN_SYMBOL) }
                .with_context(|| format!("resolve connector plugin entry in {}", path.display()))?;
            // SAFETY: The registrar remains valid for the synchronous entry-point call.
            unsafe { entry(std::ptr::from_mut(&mut registrar)) }
        };
        if let Some(error) = state.error {
            return Err(error)
                .with_context(|| format!("register connector plugin {}", path.display()));
        }
        if code != 0 {
            bail!("connector plugin {} returned status {code}", path.display());
        }
        if state.registrations == 0 {
            bail!(
                "connector plugin {} registered no descriptors",
                path.display()
            );
        }
        libraries.push(library);
    }
    Ok(LoadedConnectorPlugins { libraries })
}

/// Parses a platform-native path list from an environment variable.
#[must_use]
pub fn plugin_paths_from_env(variable: &str) -> Vec<PathBuf> {
    std::env::var_os(variable)
        .map(|value| std::env::split_paths(&value).collect())
        .unwrap_or_default()
}

struct RegistrationState<'a> {
    registry: &'a DescriptorRegistry,
    error: Option<anyhow::Error>,
    registrations: usize,
}

unsafe extern "C" fn register_descriptor(
    context: *mut c_void,
    name: *const u8,
    name_len: usize,
    descriptor: *mut c_void,
) -> i32 {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() {
            // SAFETY: The callback contract transfers ownership even when registration fails.
            unsafe { drop_erased_descriptor(descriptor) };
            return Err(anyhow!("plugin registrar context is null"));
        }
        // SAFETY: The loader passes a live RegistrationState for this synchronous callback.
        let state = unsafe { &mut *context.cast::<RegistrationState<'_>>() };
        if name.is_null() {
            // SAFETY: The callback contract transfers ownership even when registration fails.
            unsafe { drop_erased_descriptor(descriptor) };
            return Err(anyhow!("plugin descriptor name is null"));
        }
        // SAFETY: The plugin allocated this pointer with erase_descriptor using the matching API.
        let descriptor = unsafe { take_erased_descriptor(descriptor) }?;
        // SAFETY: The plugin guarantees `name_len` readable bytes for this callback.
        let name = unsafe { std::slice::from_raw_parts(name, name_len) };
        let name = std::str::from_utf8(name).context("plugin descriptor name is not UTF-8")?;
        if name.is_empty() {
            return Err(anyhow!("plugin descriptor name is empty"));
        }
        state
            .registry
            .register(name, Arc::from(descriptor))
            .with_context(|| format!("register connector descriptor {name:?}"))?;
        state.registrations += 1;
        Ok(())
    }));
    match result {
        Ok(Ok(())) => 0,
        Ok(Err(error)) => {
            if !context.is_null() {
                // SAFETY: The loader passes a live RegistrationState for this callback.
                unsafe { &mut *context.cast::<RegistrationState<'_>>() }.error = Some(error);
            }
            1
        }
        Err(_) => {
            if !context.is_null() {
                // SAFETY: The loader passes a live RegistrationState for this callback.
                unsafe { &mut *context.cast::<RegistrationState<'_>>() }.error =
                    Some(anyhow!("connector plugin registration panicked"));
            }
            2
        }
    }
}

#[doc(hidden)]
pub fn erase_descriptor<T>(descriptor: T) -> *mut c_void
where
    T: ConnectorDescriptor,
{
    let descriptor: Box<dyn ConnectorDescriptor> = Box::new(descriptor);
    Box::into_raw(Box::new(descriptor)).cast()
}

unsafe fn take_erased_descriptor(pointer: *mut c_void) -> Result<Box<dyn ConnectorDescriptor>> {
    if pointer.is_null() {
        bail!("plugin descriptor pointer is null");
    }
    // SAFETY: The pointer was returned by erase_descriptor and ownership is transferred once.
    let descriptor = unsafe { Box::from_raw(pointer.cast::<Box<dyn ConnectorDescriptor>>()) };
    Ok(*descriptor)
}

unsafe fn drop_erased_descriptor(pointer: *mut c_void) {
    if !pointer.is_null() {
        // SAFETY: The pointer was returned by erase_descriptor and ownership is transferred once.
        drop(unsafe { Box::from_raw(pointer.cast::<Box<dyn ConnectorDescriptor>>()) });
    }
}

/// Exports one connector descriptor through Fujin's versioned dynamic plugin entry point.
#[macro_export]
macro_rules! export_connector_plugin {
    ($name:literal, $descriptor:expr) => {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn fujin_connector_plugin_v1(
            registrar: *mut $crate::ConnectorPluginRegistrar,
        ) -> i32 {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if registrar.is_null() {
                    return 1;
                }
                // SAFETY: The host passes a valid registrar for the duration of this call.
                let registrar = unsafe { &mut *registrar };
                if registrar.abi_version != $crate::CONNECTOR_PLUGIN_ABI_VERSION {
                    return 2;
                }
                let name: &'static str = $name;
                let descriptor = $crate::erase_descriptor($descriptor);
                // SAFETY: All pointers remain valid for this synchronous callback; ownership of the
                // descriptor pointer transfers to the host on every return path.
                unsafe {
                    (registrar.register_descriptor)(
                        registrar.context,
                        name.as_ptr(),
                        name.len(),
                        descriptor,
                    )
                }
            }));
            result.unwrap_or(3)
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_plugin_path_list_loads_nothing() {
        let registry = DescriptorRegistry::default();
        let loaded = load_connector_plugins(Vec::<PathBuf>::new(), &registry)
            .expect("load empty plugin list");
        assert!(loaded.is_empty());
    }

    #[test]
    fn missing_plugin_path_is_contextual() {
        let registry = DescriptorRegistry::default();
        let error =
            load_connector_plugins([Path::new("definitely-missing-fujin-plugin")], &registry)
                .expect_err("missing plugin error");
        assert!(error.to_string().contains("load connector plugin"));
    }
}
