use std::sync::Arc;

use anyhow::{Context, Result};
use fujin_configurator::server_config::GrpcListenerConfig;
use fujin_connector::Catalog;
use fujin_middleware::BindMiddlewareRunner;

use fujin_grpc_proto::fujin::v1 as pb;
use fujin_transport::{Endpoint, ListenerMetadata, TransportContext, listener::bind_tcp};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Identity, Server, ServerTlsConfig};

use crate::GrpcService;

pub(super) async fn serve(
    config: GrpcListenerConfig,
    context: TransportContext,
    catalog: Arc<Catalog>,
    bind_middlewares: Arc<dyn BindMiddlewareRunner>,
) -> Result<()> {
    let shutdown = context.shutdown();
    let listener = bind_tcp(
        &config.listen,
        ListenerMetadata::grpc(config.listen.clone()),
        context.listener_registry(),
        context.inherited_listeners(),
    )
    .await?;
    let mut builder = Server::builder()
        .max_concurrent_streams(config.max_concurrent_streams)
        .initial_stream_window_size(config.initial_stream_window_size)
        .initial_connection_window_size(config.initial_connection_window_size)
        .http2_keepalive_interval(config.http2_keepalive_interval)
        .http2_keepalive_timeout(config.http2_keepalive_timeout)
        .http2_adaptive_window(config.http2_adaptive_window);
    if let Some(deadline) = config.timeout {
        builder = builder.timeout(deadline);
    }
    if let Some(age) = config.max_connection_age {
        builder = builder.max_connection_age(age);
    }
    if let Some(grace) = config.max_connection_age_grace {
        builder = builder.max_connection_age_grace(grace);
    }
    if let Some(tls) = config.tls.as_ref() {
        let certificate = tokio::fs::read(&tls.certificate)
            .await
            .with_context(|| format!("read gRPC certificate {:?}", tls.certificate))?;
        let private_key = tokio::fs::read(&tls.private_key)
            .await
            .with_context(|| format!("read gRPC private key {:?}", tls.private_key))?;
        let mut tls_config =
            ServerTlsConfig::new().identity(Identity::from_pem(certificate, private_key));
        if let Some(directory) = tls.client_certificates.as_ref() {
            tls_config = tls_config.client_ca_root(tonic::transport::Certificate::from_pem(
                fujin_transport::tls::load_pem_directory(directory).await?,
            ));
        }
        builder = builder
            .tls_config(tls_config)
            .context("configure gRPC TLS")?;
    }
    let mut service = pb::fujin_service_server::FujinServiceServer::new(GrpcService::new(
        catalog,
        bind_middlewares,
    ));
    if let Some(limit) = config.max_decoding_message_size {
        service = service.max_decoding_message_size(limit);
    }
    if let Some(limit) = config.max_encoding_message_size {
        service = service.max_encoding_message_size(limit);
    }
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<pb::fujin_service_server::FujinServiceServer<GrpcService>>()
        .await;
    context.signal_ready(Endpoint::grpc(
        listener
            .local_addr()
            .context("read gRPC listener address")?
            .to_string(),
        config.tls.is_some(),
    ));
    let shutdown_health = health_reporter.clone();
    builder
        .add_service(health_service)
        .add_service(service)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            shutdown.cancelled().await;
            shutdown_health
                .set_not_serving::<pb::fujin_service_server::FujinServiceServer<GrpcService>>()
                .await;
            shutdown_health
                .set_service_status("", tonic_health::ServingStatus::NotServing)
                .await;
        })
        .await
        .context("serve gRPC")
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use fujin_connector::{Catalog, ConnectorRegistry, GenerationCompiler, NoConnectorMiddleware};
    use fujin_middleware::NoBindMiddleware;
    use fujin_transport::{InheritedListeners, ListenerRegistry, TransportContext};
    use tokio::{
        sync::mpsc,
        time::{Duration, timeout},
    };
    use tokio_util::sync::CancellationToken;
    use tonic_health::pb::{
        HealthCheckRequest, health_check_response::ServingStatus, health_client::HealthClient,
    };

    use super::*;
    use crate::native::NativeSessions;

    #[tokio::test]
    async fn reports_fujin_service_serving() {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve gRPC address");
        let address = probe.local_addr().expect("gRPC address");
        drop(probe);
        let registry = Arc::new(ConnectorRegistry::default());
        let compiler = Arc::new(GenerationCompiler::new(
            registry,
            Arc::new(NoConnectorMiddleware),
        ));
        let catalog = Arc::new(
            Catalog::compile(&BTreeMap::new(), compiler)
                .await
                .expect("compile empty catalog"),
        );
        let shutdown = CancellationToken::new();
        let (ready_tx, mut ready_rx) = mpsc::unbounded_channel();
        let server_catalog = Arc::clone(&catalog);
        let server_bind_middlewares: Arc<dyn BindMiddlewareRunner> = Arc::new(NoBindMiddleware);
        let server_shutdown = shutdown.clone();
        let server = tokio::spawn(async move {
            let native_sessions = Arc::new(NativeSessions::new(
                Arc::clone(&server_catalog),
                Arc::clone(&server_bind_middlewares),
                "test".into(),
                server_shutdown.clone(),
            ));
            serve(
                GrpcListenerConfig {
                    listen: address.to_string(),
                    timeout: None,
                    max_concurrent_streams: None,
                    max_decoding_message_size: None,
                    max_encoding_message_size: None,
                    initial_stream_window_size: None,
                    initial_connection_window_size: None,
                    http2_keepalive_interval: None,
                    http2_keepalive_timeout: None,
                    http2_adaptive_window: None,
                    max_connection_age: None,
                    max_connection_age_grace: None,
                    tls: None,
                },
                TransportContext::new(
                    native_sessions,
                    server_shutdown,
                    ready_tx,
                    ListenerRegistry::new(1),
                    InheritedListeners::default(),
                ),
                server_catalog,
                server_bind_middlewares,
            )
            .await
        });
        timeout(Duration::from_secs(5), ready_rx.recv())
            .await
            .expect("gRPC listener readiness timeout")
            .expect("gRPC listener readiness");
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .expect("health endpoint")
            .connect()
            .await
            .expect("connect health channel");
        let mut client = HealthClient::new(channel);
        let response = client
            .check(HealthCheckRequest {
                service: "fujin.v1.FujinService".into(),
            })
            .await
            .expect("check Fujin gRPC health")
            .into_inner();
        assert_eq!(response.status, ServingStatus::Serving as i32);
        drop(client);
        shutdown.cancel();
        timeout(Duration::from_secs(5), server)
            .await
            .expect("gRPC shutdown timeout")
            .expect("gRPC server task")
            .expect("gRPC server");
        catalog.close().await.expect("close catalog");
    }
}
