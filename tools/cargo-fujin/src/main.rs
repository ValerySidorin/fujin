use std::{env, ffi::OsString, path::PathBuf};

use anyhow::{Result, bail};
use cargo_fujin::{
    BuildOptions, DEFAULT_MANIFEST, Dependency, Plugin, PluginFamily, add_plugin, build_project,
    clean_project, format_plugin, generate_project, initialize_manifest,
    is_cargo_subcommand_argument, load_manifest, remove_plugin,
};
use clap::{ArgAction, ArgGroup, Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "cargo fujin",
    version,
    about = "Build Fujin binaries from Cargo plugins"
)]
struct Cli {
    /// Fujin composition manifest.
    #[arg(long, global = true, default_value = DEFAULT_MANIFEST)]
    manifest: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Creates a new Fujin composition manifest.
    Init {
        /// Replace an existing manifest.
        #[arg(long)]
        force: bool,
        #[command(flatten)]
        fujin: OptionalDependencyArgs,
    },
    /// Adds, removes, or lists plugin crates.
    Plugin {
        #[command(subcommand)]
        command: PluginCommand,
    },
    /// Generates the Cargo composition project without building it.
    Generate,
    /// Removes generated sources and the Cargo build cache, preserving the final artifact.
    Clean,
    /// Generates and builds the final Fujin binary.
    Build {
        /// Cargo profile used for the generated project.
        #[arg(long, default_value = "release")]
        profile: String,
        /// Rust target triple.
        #[arg(long)]
        target: Option<String>,
        /// Require the generated Cargo.lock to remain unchanged.
        #[arg(long)]
        locked: bool,
        /// Seed the generated project with this Cargo.lock and require it to remain unchanged.
        #[arg(long)]
        lockfile: Option<PathBuf>,
        /// Prevent Cargo network access.
        #[arg(long)]
        offline: bool,
        /// Override application.output from the manifest.
        #[arg(long)]
        output: Option<PathBuf>,
        /// Remove generated sources and the Cargo build cache after installing the final artifact.
        #[arg(long)]
        clean_after: bool,
    },
}

#[derive(Debug, Subcommand)]
enum PluginCommand {
    /// Adds one plugin crate to the composition.
    Add {
        #[arg(value_enum)]
        family: Family,
        /// Stable plugin identifier used by the composition manifest.
        #[arg(long)]
        name: String,
        /// Cargo package name.
        #[arg(long)]
        package: String,
        /// Plugin factory function within the crate.
        #[arg(long, default_value = "plugin")]
        factory: String,
        /// Optional Rust cfg expression, for example `unix`.
        #[arg(long)]
        cfg: Option<String>,
        #[command(flatten)]
        dependency: Box<DependencyArgs>,
    },
    /// Removes one plugin from the composition.
    Remove {
        #[arg(value_enum)]
        family: Family,
        name: String,
    },
    /// Lists plugins in deterministic manifest order.
    List,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Family {
    Configurator,
    Connector,
    Transport,
    BindMiddleware,
    ConnectorMiddleware,
}

impl From<Family> for PluginFamily {
    fn from(value: Family) -> Self {
        match value {
            Family::Configurator => Self::Configurator,
            Family::Connector => Self::Connector,
            Family::Transport => Self::Transport,
            Family::BindMiddleware => Self::BindMiddleware,
            Family::ConnectorMiddleware => Self::ConnectorMiddleware,
        }
    }
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("source")
        .required(true)
        .multiple(false)
        .args(["version", "git", "path"])
))]
struct DependencyArgs {
    #[arg(long)]
    version: Option<String>,
    #[arg(long)]
    git: Option<String>,
    #[arg(long)]
    path: Option<PathBuf>,
    #[arg(long, requires = "version")]
    registry: Option<String>,
    #[arg(long, requires = "git", conflicts_with_all = ["tag", "branch"])]
    rev: Option<String>,
    #[arg(long, requires = "git", conflicts_with_all = ["rev", "branch"])]
    tag: Option<String>,
    #[arg(long, requires = "git", conflicts_with_all = ["rev", "tag"])]
    branch: Option<String>,
    #[arg(long, action = ArgAction::Set)]
    default_features: Option<bool>,
    #[arg(long = "feature", value_delimiter = ',')]
    features: Vec<String>,
}

impl From<DependencyArgs> for Dependency {
    fn from(value: DependencyArgs) -> Self {
        Self {
            version: value.version,
            git: value.git,
            path: value.path,
            registry: value.registry,
            rev: value.rev,
            tag: value.tag,
            branch: value.branch,
            default_features: value.default_features,
            features: value.features,
        }
    }
}

#[derive(Debug, Args, Default)]
struct OptionalDependencyArgs {
    #[arg(long = "fujin-version")]
    version: Option<String>,
    #[arg(long = "fujin-git")]
    git: Option<String>,
    #[arg(long = "fujin-path")]
    path: Option<PathBuf>,
    #[arg(long = "fujin-registry", requires = "version")]
    registry: Option<String>,
    #[arg(
        long = "fujin-rev",
        requires = "git",
        conflicts_with_all = ["tag", "branch"]
    )]
    rev: Option<String>,
    #[arg(
        long = "fujin-tag",
        requires = "git",
        conflicts_with_all = ["rev", "branch"]
    )]
    tag: Option<String>,
    #[arg(
        long = "fujin-branch",
        requires = "git",
        conflicts_with_all = ["rev", "tag"]
    )]
    branch: Option<String>,
    #[arg(long = "fujin-default-features", action = ArgAction::Set)]
    default_features: Option<bool>,
    #[arg(long = "fujin-feature", value_delimiter = ',')]
    features: Vec<String>,
}

impl OptionalDependencyArgs {
    fn into_dependency(self) -> Result<Dependency> {
        let sources = usize::from(self.version.is_some())
            + usize::from(self.git.is_some())
            + usize::from(self.path.is_some());
        if sources > 1 {
            bail!("specify at most one of --fujin-version, --fujin-git, or --fujin-path");
        }
        Ok(Dependency {
            version: self
                .version
                .or_else(|| (sources == 0).then(|| env!("CARGO_PKG_VERSION").into())),
            git: self.git,
            path: self.path,
            registry: self.registry,
            rev: self.rev,
            tag: self.tag,
            branch: self.branch,
            default_features: self.default_features,
            features: self.features,
        })
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse_from(arguments());
    match cli.command {
        Command::Init { force, fujin } => {
            initialize_manifest(&cli.manifest, force, fujin.into_dependency()?)?;
            println!("created {}", cli.manifest.display());
        }
        Command::Plugin { command } => match command {
            PluginCommand::Add {
                family,
                name,
                package,
                factory,
                cfg,
                dependency,
            } => {
                let family = PluginFamily::from(family);
                add_plugin(
                    &cli.manifest,
                    Plugin {
                        family,
                        name: name.clone(),
                        package,
                        factory,
                        cfg,
                        dependency: (*dependency).into(),
                    },
                )?;
                println!("added {family} plugin {name}");
            }
            PluginCommand::Remove { family, name } => {
                let family = PluginFamily::from(family);
                remove_plugin(&cli.manifest, family, &name)?;
                println!("removed {family} plugin {name}");
            }
            PluginCommand::List => {
                for plugin in load_manifest(&cli.manifest)?.plugins {
                    println!("{}", format_plugin(&plugin));
                }
            }
        },
        Command::Generate => {
            let generated = generate_project(&cli.manifest)?;
            println!("generated {}", generated.directory.display());
        }
        Command::Clean => {
            let directory = clean_project(&cli.manifest)?;
            println!("cleaned {}", directory.display());
        }
        Command::Build {
            profile,
            target,
            locked,
            lockfile,
            offline,
            output,
            clean_after,
        } => {
            let output = build_project(
                &cli.manifest,
                &BuildOptions {
                    profile,
                    target,
                    locked,
                    lockfile,
                    offline,
                    output,
                    clean_after,
                },
            )?;
            println!("built {}", output.display());
        }
    }
    Ok(())
}

fn arguments() -> impl Iterator<Item = OsString> {
    let mut arguments = env::args_os();
    let executable = arguments
        .next()
        .unwrap_or_else(|| OsString::from("cargo-fujin"));
    let mut normalized = vec![executable];
    if let Some(argument) = arguments.next()
        && !is_cargo_subcommand_argument(&argument)
    {
        normalized.push(argument);
    }
    normalized.extend(arguments);
    normalized.into_iter()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cargo_injected_subcommand_parses() {
        let cli = Cli::try_parse_from([
            "cargo-fujin",
            "--manifest",
            "custom.toml",
            "plugin",
            "add",
            "connector",
            "--name",
            "acme",
            "--package",
            "acme-connector",
            "--git",
            "https://example.invalid/acme",
            "--rev",
            "deadbeef",
        ])
        .expect("parse plugin add");
        assert_eq!(cli.manifest, PathBuf::from("custom.toml"));
        let Command::Plugin {
            command: PluginCommand::Add { dependency, .. },
        } = cli.command
        else {
            panic!("unexpected command");
        };
        assert_eq!(dependency.rev.as_deref(), Some("deadbeef"));
    }

    #[test]
    fn plugin_add_requires_exactly_one_source() {
        assert!(
            Cli::try_parse_from([
                "cargo-fujin",
                "plugin",
                "add",
                "connector",
                "--name",
                "acme",
                "--package",
                "acme-connector",
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "cargo-fujin",
                "plugin",
                "add",
                "connector",
                "--name",
                "acme",
                "--package",
                "acme-connector",
                "--version",
                "1",
                "--git",
                "https://example.invalid/acme",
            ])
            .is_err()
        );
    }

    #[test]
    fn clean_and_build_cache_options_parse() {
        let clean = Cli::try_parse_from(["cargo-fujin", "clean"]).expect("parse clean");
        assert!(matches!(clean.command, Command::Clean));

        let build = Cli::try_parse_from([
            "cargo-fujin",
            "build",
            "--clean-after",
            "--lockfile",
            "Cargo.lock",
        ])
        .expect("parse build cache options");
        assert!(matches!(
            build.command,
            Command::Build {
                clean_after: true,
                lockfile: Some(path),
                ..
            } if path == std::path::Path::new("Cargo.lock")
        ));
    }
}
