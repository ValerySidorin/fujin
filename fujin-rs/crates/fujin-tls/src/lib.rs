//! TLS material loading shared by optional listeners.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use fujin_runtime::fujin_server_config::TlsConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;

pub fn install_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Loads a PEM certificate chain and private key.
///
/// # Errors
///
/// Returns file I/O or PEM parsing errors, including empty certificate or key files.
pub async fn load_identity(
    config: &TlsConfig,
    listener: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    install_provider();
    let certificate = tokio::fs::read(&config.certificate)
        .await
        .with_context(|| format!("read {listener} certificate {:?}", config.certificate))?;
    let private_key = tokio::fs::read(&config.private_key)
        .await
        .with_context(|| format!("read {listener} private key {:?}", config.private_key))?;
    let certificates = rustls_pemfile::certs(&mut certificate.as_slice())
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("parse {listener} certificate"))?;
    if certificates.is_empty() {
        bail!("{listener} certificate contains no certificates");
    }
    let private_key = rustls_pemfile::private_key(&mut private_key.as_slice())
        .with_context(|| format!("parse {listener} private key"))?
        .with_context(|| format!("{listener} private key file contains no key"))?;
    Ok((certificates, private_key))
}

/// Builds a Rustls acceptor with optional mutual-TLS roots.
///
/// # Errors
///
/// Returns identity, root certificate, or Rustls configuration errors.
pub async fn load_acceptor(config: &TlsConfig, listener: &str) -> Result<TlsAcceptor> {
    let (certificates, private_key) = load_identity(config, listener).await?;
    let builder = rustls::ServerConfig::builder();
    let server = if config.require_client_certificate {
        let directory = config
            .client_certificates
            .as_ref()
            .context("client certificate directory missing")?;
        let roots_pem = load_pem_directory(directory).await?;
        let mut roots = rustls::RootCertStore::empty();
        for certificate in rustls_pemfile::certs(&mut roots_pem.as_slice()) {
            roots
                .add(certificate.context("parse client CA certificate")?)
                .context("add client CA certificate")?;
        }
        if roots.is_empty() {
            bail!("client certificate directory contains no certificates");
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .context("build client certificate verifier")?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates, private_key)
            .context("configure TLS identity")?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)
            .context("configure TLS identity")?
    };
    Ok(TlsAcceptor::from(Arc::new(server)))
}

/// Reads regular PEM files from a directory in stable path order.
///
/// # Errors
///
/// Returns directory traversal or file I/O errors.
pub async fn load_pem_directory(directory: &str) -> Result<Vec<u8>> {
    let mut entries = tokio::fs::read_dir(directory)
        .await
        .with_context(|| format!("read certificate directory {directory:?}"))?;
    let mut paths = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            paths.push(entry.path());
        }
    }
    paths.sort();
    let mut output = Vec::new();
    for path in paths {
        let bytes = tokio::fs::read(&path)
            .await
            .with_context(|| format!("read certificate {}", path.display()))?;
        output.extend_from_slice(&bytes);
        output.push(b'\n');
    }
    Ok(output)
}
