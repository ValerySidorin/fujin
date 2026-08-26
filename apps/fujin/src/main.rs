use anyhow::Result;
use fujin::Application;

mod plugins;

const BUILD_VERSION: &str = env!("FUJIN_BUILD_VERSION");

#[tokio::main]
async fn main() -> Result<()> {
    fujin::run_cli(
        plugins::full(Application::builder().build_version(BUILD_VERSION)),
        BUILD_VERSION,
    )
    .await
}
