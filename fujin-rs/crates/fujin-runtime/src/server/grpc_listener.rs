use anyhow::{Context, Result};
use fujin_proto::fujin::v1 as pb;
use fujin_transport::{Endpoint, TransportContext, listener::bind_tcp};
use fujin_upgrade::ListenerMetadata;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Identity, Server, ServerTlsConfig};

use crate::{GrpcService, server_config::GrpcListenerConfig};

pub(super) async fn serve(config: GrpcListenerConfig, context: TransportContext) -> Result<()> {
    let catalog = context.catalog();
    let bind_middlewares = context.bind_middlewares();
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
        .initial_stream_window_size(config.initial_window_size)
        .initial_connection_window_size(config.initial_connection_window_size)
        .http2_keepalive_interval(config.server_keepalive.time)
        .http2_keepalive_timeout(config.server_keepalive.timeout);
    if let Some(age) = config.server_keepalive.max_connection_age {
        builder = builder.max_connection_age(age);
    }
    if let Some(grace) = config.server_keepalive.max_connection_age_grace {
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
    if let Some(limit) = config.max_recv_message_size {
        service = service.max_decoding_message_size(limit);
    }
    if let Some(limit) = config.max_send_message_size {
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

    use fujin_core::{Catalog, ConnectorRegistry, GenerationCompiler, NoConnectorMiddleware};
    use fujin_transport::TransportContext;
    use fujin_upgrade::{InheritedListeners, ListenerRegistry};
    use tokio::{
        sync::mpsc,
        time::{Duration, timeout},
    };
    use tokio_util::sync::CancellationToken;
    use tonic_health::pb::{
        HealthCheckRequest, health_check_response::ServingStatus, health_client::HealthClient,
    };

    use super::*;

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
        let server_shutdown = shutdown.clone();
        let server = tokio::spawn(async move {
            serve(
                GrpcListenerConfig {
                    listen: address.to_string(),
                    max_concurrent_streams: None,
                    max_recv_message_size: None,
                    max_send_message_size: None,
                    initial_window_size: None,
                    initial_connection_window_size: None,
                    server_keepalive: crate::server_config::ServerKeepAliveConfig::default(),
                    tls: None,
                },
                TransportContext::new(
                    server_catalog,
                    Arc::new(fujin_core::NoBindMiddleware),
                    "test".into(),
                    server_shutdown,
                    ready_tx,
                    ListenerRegistry::new(1),
                    InheritedListeners::default(),
                ),
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
