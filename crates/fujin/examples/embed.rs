use fujin::{Application, FujinConfig, GrpcConfig, RuntimeConfig, TransportConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = RuntimeConfig {
        fujin: FujinConfig {
            transports: vec![TransportConfig {
                transport_type: "tcp".into(),
                enabled: true,
                settings: serde_json::json!({"addr": "127.0.0.1:4850"}),
            }],
        },
        grpc: GrpcConfig {
            enabled: false,
            ..GrpcConfig::default()
        },
        ..RuntimeConfig::default()
    };

    let application = Application::builder()
        .graceful_upgrade(false)
        .config(config)
        .transport(fujin_transport_tcp::plugin())
        .build()
        .await?;
    let running = application.start().await?;

    println!("Fujin listening on {:?}", running.endpoints());
    tokio::signal::ctrl_c().await?;
    running.shutdown().await
}
