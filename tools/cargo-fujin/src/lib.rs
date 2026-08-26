use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsStr,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

pub const DEFAULT_MANIFEST: &str = "fujin.build.toml";
const DEFAULT_GENERATED_DIRECTORY: &str = ".fujin/generated";
const GENERATED_DIRECTORY_MARKER: &str = ".cargo-fujin-generated";
const GENERATED_DIRECTORY_MARKER_CONTENT: &str = "managed by cargo-fujin\n";

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BuildManifest {
    pub application: Application,
    #[serde(default, rename = "plugin")]
    pub plugins: Vec<Plugin>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArtifactKind {
    #[default]
    Binary,
    Cdylib,
    Staticlib,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Application {
    pub name: String,
    pub version: String,
    pub output: PathBuf,
    pub artifact: ArtifactKind,
    pub generated_directory: PathBuf,
    pub fujin: Dependency,
}

impl Default for Application {
    fn default() -> Self {
        Self {
            name: "fujin-custom".into(),
            version: "0.1.0".into(),
            output: PathBuf::from("fujin"),
            artifact: ArtifactKind::Binary,
            generated_directory: PathBuf::from(DEFAULT_GENERATED_DIRECTORY),
            fujin: Dependency {
                version: Some(env!("CARGO_PKG_VERSION").into()),
                ..Dependency::default()
            },
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Dependency {
    pub version: Option<String>,
    pub git: Option<String>,
    pub path: Option<PathBuf>,
    pub registry: Option<String>,
    pub rev: Option<String>,
    pub tag: Option<String>,
    pub branch: Option<String>,
    pub default_features: Option<bool>,
    pub features: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PluginFamily {
    Configurator,
    Connector,
    Transport,
    BindMiddleware,
    ConnectorMiddleware,
}

impl PluginFamily {
    #[must_use]
    pub const fn builder_method(self) -> &'static str {
        match self {
            Self::Configurator => "configurator",
            Self::Connector => "connector",
            Self::Transport => "transport",
            Self::BindMiddleware => "bind_middleware",
            Self::ConnectorMiddleware => "connector_middleware",
        }
    }
}

impl std::fmt::Display for PluginFamily {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Configurator => "configurator",
            Self::Connector => "connector",
            Self::Transport => "transport",
            Self::BindMiddleware => "bind-middleware",
            Self::ConnectorMiddleware => "connector-middleware",
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Plugin {
    pub family: PluginFamily,
    pub name: String,
    pub package: String,
    #[serde(default = "default_factory")]
    pub factory: String,
    #[serde(default)]
    pub cfg: Option<String>,
    #[serde(flatten)]
    pub dependency: Dependency,
}

fn default_factory() -> String {
    "plugin".into()
}

#[derive(Clone, Debug)]
pub struct GeneratedProject {
    pub directory: PathBuf,
    pub manifest: PathBuf,
    pub source: PathBuf,
}

#[derive(Clone, Debug)]
pub struct BuildOptions {
    pub profile: String,
    pub target: Option<String>,
    pub locked: bool,
    pub offline: bool,
    pub output: Option<PathBuf>,
    pub lockfile: Option<PathBuf>,
    pub clean_after: bool,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            profile: "release".into(),
            target: None,
            locked: false,
            offline: false,
            output: None,
            lockfile: None,
            clean_after: false,
        }
    }
}

/// Loads and validates a build manifest.
///
/// # Errors
/// Returns an error when the file cannot be read, parsed, or validated.
pub fn load_manifest(path: &Path) -> Result<BuildManifest> {
    let encoded = fs::read_to_string(path)
        .with_context(|| format!("read Fujin build manifest {}", path.display()))?;
    let manifest: BuildManifest = toml::from_str(&encoded)
        .with_context(|| format!("parse Fujin build manifest {}", path.display()))?;
    validate_manifest_at(&manifest, manifest_root(path), false)?;
    Ok(manifest)
}

/// Validates and writes a build manifest.
///
/// # Errors
/// Returns an error when validation, serialization, directory creation, or writing fails.
pub fn save_manifest(path: &Path, manifest: &BuildManifest) -> Result<()> {
    validate_manifest_at(manifest, manifest_root(path), false)?;
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create manifest directory {}", parent.display()))?;
    }
    let mut encoded = toml::to_string_pretty(manifest).context("encode Fujin build manifest")?;
    if !encoded.ends_with('\n') {
        encoded.push('\n');
    }
    fs::write(path, encoded)
        .with_context(|| format!("write Fujin build manifest {}", path.display()))
}

/// Initializes a new build manifest.
///
/// # Errors
/// Returns an error when the destination exists without `force`, or writing fails.
pub fn initialize_manifest(path: &Path, force: bool, fujin: Dependency) -> Result<()> {
    if path.exists() && !force {
        bail!(
            "Fujin build manifest {} already exists; use --force to replace it",
            path.display()
        );
    }
    let mut manifest = BuildManifest::default();
    manifest.application.fujin = fujin;
    save_manifest(path, &manifest)
}

/// Adds one plugin to a build manifest.
///
/// # Errors
/// Returns an error when the plugin is invalid, duplicates an alias, or persistence fails.
pub fn add_plugin(path: &Path, plugin: Plugin) -> Result<()> {
    validate_plugin(&plugin, manifest_root(path))?;
    let mut manifest = load_manifest(path)?;
    ensure!(
        !manifest
            .plugins
            .iter()
            .any(|existing| existing.family == plugin.family && existing.name == plugin.name),
        "{} plugin {:?} already exists",
        plugin.family,
        plugin.name
    );
    ensure!(
        !manifest
            .plugins
            .iter()
            .any(|existing| existing.name == plugin.name),
        "plugin Cargo alias {:?} is already in use",
        plugin.name
    );
    manifest.plugins.push(plugin);
    save_manifest(path, &manifest)
}

/// Removes one plugin from a build manifest.
///
/// # Errors
/// Returns an error when the plugin is absent or the manifest cannot be loaded or saved.
pub fn remove_plugin(path: &Path, family: PluginFamily, name: &str) -> Result<()> {
    let mut manifest = load_manifest(path)?;
    let original = manifest.plugins.len();
    manifest
        .plugins
        .retain(|plugin| plugin.family != family || plugin.name != name);
    ensure!(
        manifest.plugins.len() != original,
        "{family} plugin {name:?} is not present"
    );
    save_manifest(path, &manifest)
}

/// Generates the Cargo project for a custom Fujin binary.
///
/// # Errors
/// Returns an error when the manifest is not runnable or generated files cannot be written.
pub fn generate_project(path: &Path) -> Result<GeneratedProject> {
    let manifest = load_manifest(path)?;
    validate_manifest_at(&manifest, manifest_root(path), true)?;
    let root = manifest_root(path);
    let directory = resolve_path(root, &manifest.application.generated_directory);
    let source_directory = directory.join("src");
    fs::create_dir_all(&source_directory).with_context(|| {
        format!(
            "create generated source directory {}",
            source_directory.display()
        )
    })?;
    write_if_changed(
        &directory.join(GENERATED_DIRECTORY_MARKER),
        GENERATED_DIRECTORY_MARKER_CONTENT,
    )
    .with_context(|| format!("write generated directory marker {}", directory.display()))?;
    let cargo_manifest = directory.join("Cargo.toml");
    let source = source_directory.join(match manifest.application.artifact {
        ArtifactKind::Binary => "main.rs",
        ArtifactKind::Cdylib | ArtifactKind::Staticlib => "lib.rs",
    });
    write_if_changed(&cargo_manifest, &generate_cargo_toml(&manifest, root)?).with_context(
        || {
            format!(
                "write generated Cargo manifest {}",
                cargo_manifest.display()
            )
        },
    )?;
    let source_code = match manifest.application.artifact {
        ArtifactKind::Binary => generate_main(&manifest),
        ArtifactKind::Cdylib | ArtifactKind::Staticlib => generate_library(&manifest),
    };
    write_if_changed(&source, &source_code)
        .with_context(|| format!("write generated Fujin source {}", source.display()))?;
    Ok(GeneratedProject {
        directory,
        manifest: cargo_manifest,
        source,
    })
}

/// Removes the generated Cargo project and its build cache.
///
/// The final artifact configured by `application.output` is not removed.
///
/// # Errors
/// Returns an error when the manifest cannot be loaded, the generated path is unsafe, or removal
/// fails.
pub fn clean_project(path: &Path) -> Result<PathBuf> {
    let manifest = load_manifest(path)?;
    let root = manifest_root(path);
    let directory = resolve_path(root, &manifest.application.generated_directory);
    if !directory.exists() {
        return Ok(directory);
    }

    let canonical_root = fs::canonicalize(root)
        .with_context(|| format!("resolve manifest directory {}", root.display()))?;
    let canonical_directory = fs::canonicalize(&directory)
        .with_context(|| format!("resolve generated directory {}", directory.display()))?;
    ensure!(
        canonical_directory != canonical_root,
        "refuse to clean manifest directory {}",
        canonical_directory.display()
    );
    ensure!(
        canonical_directory.is_dir(),
        "generated path {} is not a directory",
        canonical_directory.display()
    );
    let marker = canonical_directory.join(GENERATED_DIRECTORY_MARKER);
    let managed = fs::read_to_string(&marker)
        .is_ok_and(|content| content == GENERATED_DIRECTORY_MARKER_CONTENT);
    let legacy_generated = canonical_directory.starts_with(&canonical_root)
        && canonical_directory.join("Cargo.toml").is_file()
        && (canonical_directory.join("src/main.rs").is_file()
            || canonical_directory.join("src/lib.rs").is_file());
    ensure!(
        managed || legacy_generated,
        "refuse to clean unrecognized generated directory {}",
        canonical_directory.display()
    );
    fs::remove_dir_all(&canonical_directory).with_context(|| {
        format!(
            "remove generated directory {}",
            canonical_directory.display()
        )
    })?;
    Ok(directory)
}

fn write_if_changed(path: &Path, content: &str) -> Result<bool> {
    match fs::read(path) {
        Ok(existing) if existing == content.as_bytes() => return Ok(false),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    fs::write(path, content)?;
    Ok(true)
}

/// Generates and builds a custom Fujin binary.
///
/// # Errors
/// Returns an error when generation, Cargo execution, or artifact installation fails.
pub fn build_project(path: &Path, options: &BuildOptions) -> Result<PathBuf> {
    ensure!(!options.profile.is_empty(), "Cargo profile is empty");
    let manifest = load_manifest(path)?;
    validate_manifest_at(&manifest, manifest_root(path), true)?;
    let root = manifest_root(path);
    let generated = generate_project(path)?;
    let target_directory = generated.directory.join("target");
    let mut command = Command::new("cargo");
    if let Some(lockfile) = &options.lockfile {
        install_lockfile(root, &generated, lockfile)?;
    }
    command
        .arg("build")
        .arg("--manifest-path")
        .arg(&generated.manifest)
        .arg("--profile")
        .arg(&options.profile)
        .env("CARGO_TARGET_DIR", &target_directory);
    if options.locked || options.lockfile.is_some() {
        command.arg("--locked");
    }
    if options.offline {
        command.arg("--offline");
    }
    if let Some(target) = &options.target {
        command.arg("--target").arg(target);
    }
    let status = command
        .status()
        .context("run Cargo for generated Fujin application")?;
    ensure!(status.success(), "generated Fujin Cargo build failed");

    let mut artifact = target_directory;
    if let Some(target) = &options.target {
        artifact.push(target);
    }
    artifact.push(profile_directory(&options.profile));
    artifact.push(artifact_name(
        &manifest.application.name,
        manifest.application.artifact,
        options.target.as_deref(),
    ));
    ensure!(
        artifact.is_file(),
        "generated Fujin binary {} was not produced",
        artifact.display()
    );

    let output = options.output.as_ref().map_or_else(
        || resolve_path(root, &manifest.application.output),
        |output| resolve_path(root, output),
    );
    if let Some(parent) = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }
    fs::copy(&artifact, &output)
        .with_context(|| format!("copy generated Fujin binary to {}", output.display()))?;
    if options.clean_after {
        let canonical_output = fs::canonicalize(&output)
            .with_context(|| format!("resolve generated Fujin output {}", output.display()))?;
        let canonical_generated = fs::canonicalize(&generated.directory).with_context(|| {
            format!(
                "resolve generated directory {}",
                generated.directory.display()
            )
        })?;
        ensure!(
            !canonical_output.starts_with(&canonical_generated),
            "--clean-after cannot preserve output {} inside generated directory {}",
            output.display(),
            generated.directory.display()
        );
        clean_project(path)?;
    }
    Ok(output)
}

fn install_lockfile(root: &Path, generated: &GeneratedProject, lockfile: &Path) -> Result<()> {
    let source = resolve_path(root, lockfile);
    let destination = generated.directory.join("Cargo.lock");
    ensure!(
        source.is_file(),
        "Cargo lock file {} does not exist",
        source.display()
    );
    let same_file = destination.exists()
        && fs::canonicalize(&source).ok() == fs::canonicalize(&destination).ok();
    if !same_file {
        fs::copy(&source, &destination).with_context(|| {
            format!(
                "copy Cargo lock file {} to {}",
                source.display(),
                destination.display()
            )
        })?;
    }
    Ok(())
}

/// Validates an in-memory build manifest.
///
/// # Errors
/// Returns an error when names, dependency sources, paths, or runnable composition are invalid.
pub fn validate_manifest(manifest: &BuildManifest, require_runnable: bool) -> Result<()> {
    validate_manifest_at(manifest, Path::new("."), require_runnable)
}

fn validate_manifest_at(
    manifest: &BuildManifest,
    root: &Path,
    require_runnable: bool,
) -> Result<()> {
    validate_package_name(&manifest.application.name, "application name")?;
    ensure!(
        !manifest.application.version.is_empty(),
        "application version is empty"
    );
    ensure!(
        !manifest.application.output.as_os_str().is_empty(),
        "application output is empty"
    );
    ensure!(
        !manifest
            .application
            .generated_directory
            .as_os_str()
            .is_empty(),
        "generated directory is empty"
    );
    validate_dependency(&manifest.application.fujin, root)?;
    let mut names = BTreeSet::new();
    let mut aliases = BTreeSet::new();
    for plugin in &manifest.plugins {
        validate_plugin(plugin, root)?;
        ensure!(
            names.insert((plugin.family, plugin.name.as_str())),
            "duplicate {} plugin {:?}",
            plugin.family,
            plugin.name
        );
        ensure!(
            aliases.insert(plugin.name.as_str()),
            "plugin Cargo alias {:?} is used by more than one plugin",
            plugin.name
        );
    }
    if require_runnable {
        ensure!(
            manifest
                .plugins
                .iter()
                .any(|plugin| plugin.family == PluginFamily::Configurator),
            "at least one configurator plugin is required"
        );
        ensure!(
            manifest
                .plugins
                .iter()
                .any(|plugin| plugin.family == PluginFamily::Connector),
            "at least one connector plugin is required"
        );
    }
    Ok(())
}

fn validate_plugin(plugin: &Plugin, root: &Path) -> Result<()> {
    validate_package_name(&plugin.name, "plugin name")?;
    validate_package_name(&plugin.package, "plugin package")?;
    validate_rust_path(&plugin.factory)?;
    if let Some(cfg) = &plugin.cfg {
        ensure!(!cfg.trim().is_empty(), "plugin cfg expression is empty");
        ensure!(
            !cfg.contains(['\n', '\r']),
            "plugin cfg expression contains a newline"
        );
    }
    validate_dependency(&plugin.dependency, root)
}

fn validate_dependency(dependency: &Dependency, root: &Path) -> Result<()> {
    let sources = usize::from(dependency.version.is_some())
        + usize::from(dependency.git.is_some())
        + usize::from(dependency.path.is_some());
    ensure!(
        sources == 1,
        "dependency must specify exactly one of version, git, or path"
    );
    if dependency.registry.is_some() {
        ensure!(
            dependency.version.is_some(),
            "registry requires a version dependency"
        );
    }
    let selectors = usize::from(dependency.rev.is_some())
        + usize::from(dependency.tag.is_some())
        + usize::from(dependency.branch.is_some());
    ensure!(
        selectors <= 1,
        "git dependency may specify only one of rev, tag, or branch"
    );
    if selectors != 0 {
        ensure!(
            dependency.git.is_some(),
            "rev, tag, and branch require a git dependency"
        );
    }
    for feature in &dependency.features {
        ensure!(!feature.trim().is_empty(), "dependency feature is empty");
    }
    if let Some(path) = &dependency.path {
        let resolved = resolve_path(root, path);
        ensure!(
            resolved.exists(),
            "dependency path {} does not exist",
            resolved.display()
        );
    }
    Ok(())
}

fn validate_package_name(value: &str, label: &str) -> Result<()> {
    ensure!(!value.is_empty(), "{label} is empty");
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'),
        "{label} {value:?} may contain only ASCII letters, digits, '-' and '_'"
    );
    ensure!(
        value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric),
        "{label} {value:?} must start with an ASCII letter or digit"
    );
    Ok(())
}

fn validate_rust_path(value: &str) -> Result<()> {
    ensure!(!value.is_empty(), "plugin factory path is empty");
    ensure!(
        value.split("::").all(|segment| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                && segment
                    .as_bytes()
                    .first()
                    .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_')
        }),
        "plugin factory path {value:?} is not a Rust path"
    );
    Ok(())
}

fn generate_cargo_toml(manifest: &BuildManifest, root: &Path) -> Result<String> {
    let mut output = String::new();
    writeln!(output, "[package]")?;
    writeln!(output, "name = {}", toml_string(&manifest.application.name))?;
    writeln!(
        output,
        "version = {}",
        toml_string(&manifest.application.version)
    )?;
    writeln!(output, "edition = \"2024\"")?;
    writeln!(output, "publish = false")?;
    if manifest.application.artifact != ArtifactKind::Binary {
        writeln!(output)?;
        writeln!(output, "[lib]")?;
        writeln!(
            output,
            "crate-type = [{}]",
            toml_string(match manifest.application.artifact {
                ArtifactKind::Cdylib => "cdylib",
                ArtifactKind::Staticlib => "staticlib",
                ArtifactKind::Binary => unreachable!("binary has no lib target"),
            })
        )?;
    }
    writeln!(output)?;
    writeln!(output, "[workspace]")?;
    writeln!(output)?;
    writeln!(output, "[dependencies]")?;
    writeln!(output, "anyhow = \"1\"")?;
    writeln!(
        output,
        "tokio = {{ version = \"1\", features = [\"macros\", \"rt-multi-thread\"] }}"
    )?;
    write_dependency(
        &mut output,
        "fujin",
        "fujin",
        &manifest.application.fujin,
        root,
    )?;
    if manifest.application.artifact != ArtifactKind::Binary {
        let ffi = ffi_dependency(&manifest.application.fujin)?;
        write_dependency(&mut output, "fujin-ffi", "fujin-ffi", &ffi, root)?;
    }
    for plugin in manifest
        .plugins
        .iter()
        .filter(|plugin| plugin.cfg.is_none())
    {
        write_dependency(
            &mut output,
            &plugin_alias(&plugin.name),
            &plugin.package,
            &plugin.dependency,
            root,
        )?;
    }

    let mut targets = BTreeMap::<&str, Vec<&Plugin>>::new();
    for plugin in manifest
        .plugins
        .iter()
        .filter(|plugin| plugin.cfg.is_some())
    {
        targets
            .entry(plugin.cfg.as_deref().expect("filtered cfg"))
            .or_default()
            .push(plugin);
    }
    for (cfg, plugins) in targets {
        writeln!(output)?;
        writeln!(
            output,
            "[target.{}.dependencies]",
            toml_key(&format!("cfg({cfg})"))
        )?;
        for plugin in plugins {
            write_dependency(
                &mut output,
                &plugin_alias(&plugin.name),
                &plugin.package,
                &plugin.dependency,
                root,
            )?;
        }
    }
    Ok(output)
}

fn write_dependency(
    output: &mut String,
    alias: &str,
    package: &str,
    dependency: &Dependency,
    root: &Path,
) -> Result<()> {
    let mut fields = Vec::new();
    if alias != package {
        fields.push(format!("package = {}", toml_string(package)));
    }
    if let Some(version) = &dependency.version {
        fields.push(format!("version = {}", toml_string(version)));
    }
    if let Some(git) = &dependency.git {
        fields.push(format!("git = {}", toml_string(git)));
    }
    if let Some(path) = &dependency.path {
        let resolved = resolve_path(root, path)
            .canonicalize()
            .with_context(|| format!("resolve dependency path {}", path.display()))?;
        fields.push(format!(
            "path = {}",
            toml_string(&resolved.to_string_lossy())
        ));
    }
    if let Some(registry) = &dependency.registry {
        fields.push(format!("registry = {}", toml_string(registry)));
    }
    for (key, value) in [
        ("rev", dependency.rev.as_ref()),
        ("tag", dependency.tag.as_ref()),
        ("branch", dependency.branch.as_ref()),
    ] {
        if let Some(value) = value {
            fields.push(format!("{key} = {}", toml_string(value)));
        }
    }
    if let Some(default_features) = dependency.default_features {
        fields.push(format!("default-features = {default_features}"));
    }
    if !dependency.features.is_empty() {
        let features = dependency
            .features
            .iter()
            .map(|feature| toml_string(feature))
            .collect::<Vec<_>>()
            .join(", ");
        fields.push(format!("features = [{features}]"));
    }
    writeln!(output, "{} = {{ {} }}", toml_key(alias), fields.join(", "))?;
    Ok(())
}

fn ffi_dependency(fujin: &Dependency) -> Result<Dependency> {
    let mut ffi = fujin.clone();
    if let Some(path) = &fujin.path {
        let parent = path
            .parent()
            .context("Fujin dependency path has no parent directory")?;
        ffi.path = Some(parent.join("fujin-ffi"));
    }
    Ok(ffi)
}

fn generate_main(manifest: &BuildManifest) -> String {
    let mut output = String::from(
        "use fujin::Application;\n\nconst BUILD_VERSION: &str = match option_env!(\"FUJIN_BUILD_VERSION\") {\n    Some(version) => version,\n    None => env!(\"CARGO_PKG_VERSION\"),\n};\n\n#[tokio::main]\nasync fn main() -> anyhow::Result<()> {\n    let builder = Application::builder().build_version(BUILD_VERSION);\n",
    );
    for plugin in &manifest.plugins {
        if let Some(cfg) = &plugin.cfg {
            let _ = writeln!(output, "    #[cfg({cfg})]");
        }
        let crate_name = plugin_alias(&plugin.name).replace('-', "_");
        let _ = writeln!(
            output,
            "    let builder = builder.{}({crate_name}::{}());",
            plugin.family.builder_method(),
            plugin.factory
        );
    }
    output.push_str("    fujin::run_cli(builder, BUILD_VERSION).await\n}\n");
    output
}

fn generate_library(manifest: &BuildManifest) -> String {
    let mut output = String::from(
        "const BUILD_VERSION: &str = match option_env!(\"FUJIN_BUILD_VERSION\") {\n    Some(version) => version,\n    None => env!(\"CARGO_PKG_VERSION\"),\n};\n\nfn application_builder() -> fujin::ApplicationBuilder {\n    let builder = fujin::Application::builder().build_version(BUILD_VERSION).graceful_upgrade(false);\n",
    );
    for plugin in &manifest.plugins {
        if let Some(cfg) = &plugin.cfg {
            let _ = writeln!(output, "    #[cfg({cfg})]");
        }
        let crate_name = plugin_alias(&plugin.name).replace('-', "_");
        let _ = writeln!(
            output,
            "    let builder = builder.{}({crate_name}::{}());",
            plugin.family.builder_method(),
            plugin.factory
        );
    }
    output.push_str("    builder\n}\n\nfujin_ffi::export_c_api!(application_builder());\n");
    output
}

fn toml_string(value: &str) -> String {
    toml::Value::String(value.to_owned()).to_string()
}

fn toml_key(value: &str) -> String {
    toml_string(value)
}

fn manifest_root(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn resolve_path(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_owned()
    } else {
        root.join(path)
    }
}

fn profile_directory(profile: &str) -> &str {
    if profile == "dev" { "debug" } else { profile }
}

fn executable_name(name: &str, target: Option<&str>) -> String {
    if target.map_or(cfg!(windows), |target| target.contains("windows")) {
        format!("{name}.exe")
    } else {
        name.into()
    }
}

fn artifact_name(name: &str, artifact: ArtifactKind, target: Option<&str>) -> String {
    let crate_name = name.replace('-', "_");
    match artifact {
        ArtifactKind::Binary => executable_name(name, target),
        ArtifactKind::Staticlib if target.is_some_and(|target| target.contains("windows")) => {
            format!("{crate_name}.lib")
        }
        ArtifactKind::Staticlib => format!("lib{crate_name}.a"),
        ArtifactKind::Cdylib if target.is_some_and(|target| target.contains("windows")) => {
            format!("{crate_name}.dll")
        }
        ArtifactKind::Cdylib
            if target.map_or(cfg!(target_os = "macos"), |target| target.contains("apple")) =>
        {
            format!("lib{crate_name}.dylib")
        }
        ArtifactKind::Cdylib => format!("lib{crate_name}.so"),
    }
}

#[must_use]
pub fn format_plugin(plugin: &Plugin) -> String {
    let source = if let Some(path) = &plugin.dependency.path {
        format!("path:{}", path.display())
    } else if let Some(git) = &plugin.dependency.git {
        let selector = plugin
            .dependency
            .rev
            .as_ref()
            .map(|value| format!("rev={value}"))
            .or_else(|| {
                plugin
                    .dependency
                    .tag
                    .as_ref()
                    .map(|value| format!("tag={value}"))
            })
            .or_else(|| {
                plugin
                    .dependency
                    .branch
                    .as_ref()
                    .map(|value| format!("branch={value}"))
            });
        selector.map_or_else(
            || format!("git:{git}"),
            |selector| format!("git:{git}#{selector}"),
        )
    } else {
        plugin.dependency.version.as_ref().map_or_else(
            || "unknown".into(),
            |version| {
                plugin.dependency.registry.as_ref().map_or_else(
                    || format!("crates.io:{version}"),
                    |registry| format!("{registry}:{version}"),
                )
            },
        )
    };
    format!(
        "{}\t{}\t{}\t{}\t{}\t{}",
        plugin.family,
        plugin.name,
        plugin.package,
        plugin.factory,
        source,
        plugin.cfg.as_deref().unwrap_or("-")
    )
}

#[must_use]
fn plugin_alias(name: &str) -> String {
    format!("fujin-plugin-{name}")
}

pub fn is_cargo_subcommand_argument(argument: &OsStr) -> bool {
    argument == OsStr::new("fujin")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn path_dependency(path: &Path) -> Dependency {
        Dependency {
            path: Some(path.to_owned()),
            ..Dependency::default()
        }
    }

    fn fixture() -> (TempDir, BuildManifest) {
        let directory = tempfile::tempdir().expect("temporary directory");
        let fujin = directory.path().join("fujin");
        let fujin_ffi = directory.path().join("fujin-ffi");
        let configurator = directory.path().join("configurator");
        let connector = directory.path().join("connector");
        let unix = directory.path().join("unix");
        for path in [&fujin, &fujin_ffi, &configurator, &connector, &unix] {
            fs::create_dir(path).expect("dependency directory");
        }
        let manifest = BuildManifest {
            application: Application {
                name: "acme-fujin".into(),
                version: "2.3.4".into(),
                output: PathBuf::from("bin/acme-fujin"),
                artifact: ArtifactKind::Binary,
                generated_directory: PathBuf::from(".fujin/generated"),
                fujin: path_dependency(&fujin),
            },
            plugins: vec![
                Plugin {
                    family: PluginFamily::Configurator,
                    name: "yaml-source".into(),
                    package: "acme-configurator".into(),
                    factory: "factory::plugin".into(),
                    cfg: None,
                    dependency: path_dependency(&configurator),
                },
                Plugin {
                    family: PluginFamily::Connector,
                    name: "broker".into(),
                    package: "acme-connector".into(),
                    factory: "plugin".into(),
                    cfg: None,
                    dependency: path_dependency(&connector),
                },
                Plugin {
                    family: PluginFamily::Transport,
                    name: "unix".into(),
                    package: "acme-unix".into(),
                    factory: "plugin".into(),
                    cfg: Some("unix".into()),
                    dependency: path_dependency(&unix),
                },
            ],
        };
        (directory, manifest)
    }

    #[test]
    fn manifest_round_trip_preserves_plugin_sources() {
        let (directory, manifest) = fixture();
        let path = directory.path().join(DEFAULT_MANIFEST);
        save_manifest(&path, &manifest).expect("save manifest");
        let loaded = load_manifest(&path).expect("load manifest");
        assert_eq!(loaded.application.name, "acme-fujin");
        assert_eq!(loaded.plugins.len(), 3);
        assert_eq!(loaded.plugins[0].factory, "factory::plugin");
        assert_eq!(loaded.plugins[2].cfg.as_deref(), Some("unix"));
    }

    #[test]
    fn generated_project_uses_explicit_registrations_and_target_dependencies() {
        let (directory, manifest) = fixture();
        let path = directory.path().join(DEFAULT_MANIFEST);
        save_manifest(&path, &manifest).expect("save manifest");
        let generated = generate_project(&path).expect("generate project");
        let cargo = fs::read_to_string(generated.manifest).expect("generated Cargo.toml");
        let source = fs::read_to_string(generated.source).expect("generated main.rs");
        assert!(cargo.contains("[target.\"cfg(unix)\".dependencies]"));
        assert!(cargo.contains("\"fujin-plugin-broker\""));
        assert!(
            source.contains("builder.configurator(fujin_plugin_yaml_source::factory::plugin())")
        );
        assert!(source.contains("builder.connector(fujin_plugin_broker::plugin())"));
        assert!(source.contains("#[cfg(unix)]"));
        assert!(source.contains("option_env!(\"FUJIN_BUILD_VERSION\")"));
        assert!(source.contains("fujin::run_cli(builder, BUILD_VERSION)"));
    }

    #[test]
    fn generated_files_are_written_only_when_content_changes() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("generated.rs");
        assert!(write_if_changed(&path, "first\n").expect("initial write"));
        assert!(!write_if_changed(&path, "first\n").expect("unchanged write"));
        assert!(write_if_changed(&path, "second\n").expect("changed write"));
        assert_eq!(
            fs::read_to_string(path).expect("generated contents"),
            "second\n"
        );
    }

    #[test]
    fn external_lockfile_seeds_generated_project() {
        let (directory, manifest) = fixture();
        let path = directory.path().join(DEFAULT_MANIFEST);
        save_manifest(&path, &manifest).expect("save manifest");
        let generated = generate_project(&path).expect("generate project");
        let lockfile = directory.path().join("locks/Cargo.lock");
        fs::create_dir_all(lockfile.parent().expect("lock parent")).expect("lock directory");
        fs::write(&lockfile, "version = 4\n").expect("external lock");

        install_lockfile(directory.path(), &generated, Path::new("locks/Cargo.lock"))
            .expect("install lock");
        install_lockfile(directory.path(), &generated, Path::new("locks/Cargo.lock"))
            .expect("install same lock");

        assert_eq!(
            fs::read_to_string(generated.directory.join("Cargo.lock"))
                .expect("generated Cargo.lock"),
            "version = 4\n"
        );
    }

    #[test]
    fn clean_removes_generated_project_and_preserves_installed_artifact() {
        let (directory, manifest) = fixture();
        let path = directory.path().join(DEFAULT_MANIFEST);
        save_manifest(&path, &manifest).expect("save manifest");
        let generated = generate_project(&path).expect("generate project");
        fs::create_dir_all(generated.directory.join("target")).expect("target directory");
        fs::write(generated.directory.join("target/cache"), "cache").expect("target cache");
        fs::remove_file(generated.directory.join(GENERATED_DIRECTORY_MARKER))
            .expect("simulate pre-marker generated project");
        let output = directory.path().join(&manifest.application.output);
        fs::create_dir_all(output.parent().expect("output parent")).expect("output directory");
        fs::write(&output, "artifact").expect("installed artifact");

        let cleaned = clean_project(&path).expect("clean generated project");

        assert_eq!(cleaned, generated.directory);
        assert!(!cleaned.exists());
        assert_eq!(
            fs::read_to_string(output).expect("preserved artifact"),
            "artifact"
        );
        assert_eq!(clean_project(&path).expect("idempotent clean"), cleaned);
    }

    #[test]
    fn clean_refuses_unmanaged_generated_directory() {
        let (directory, manifest) = fixture();
        let path = directory.path().join(DEFAULT_MANIFEST);
        save_manifest(&path, &manifest).expect("save manifest");
        let generated = directory
            .path()
            .join(&manifest.application.generated_directory);
        fs::create_dir_all(&generated).expect("unmanaged directory");
        let sentinel = generated.join("sentinel");
        fs::write(&sentinel, "keep").expect("unmanaged content");

        assert!(clean_project(&path).is_err());
        assert!(sentinel.exists());
    }

    #[test]
    fn generated_cdylib_exports_the_stable_c_api() {
        let (directory, mut manifest) = fixture();
        manifest.application.artifact = ArtifactKind::Cdylib;
        let path = directory.path().join(DEFAULT_MANIFEST);
        save_manifest(&path, &manifest).expect("save manifest");
        let generated = generate_project(&path).expect("generate library project");
        let cargo = fs::read_to_string(generated.manifest).expect("generated Cargo.toml");
        let source = fs::read_to_string(&generated.source).expect("generated lib.rs");
        assert!(cargo.contains("crate-type = [\"cdylib\"]"));
        assert!(cargo.contains("\"fujin-ffi\""));
        assert!(source.contains("fujin_ffi::export_c_api!(application_builder())"));
        assert!(source.contains("graceful_upgrade(false)"));
        assert!(source.contains("option_env!(\"FUJIN_BUILD_VERSION\")"));
        assert!(source.contains("build_version(BUILD_VERSION)"));
        assert_eq!(
            generated.source.file_name().and_then(OsStr::to_str),
            Some("lib.rs")
        );
    }

    #[test]
    fn manifest_management_rejects_duplicates_and_removes_exact_family() {
        let (directory, mut manifest) = fixture();
        manifest.plugins.truncate(1);
        let path = directory.path().join(DEFAULT_MANIFEST);
        save_manifest(&path, &manifest).expect("save manifest");
        let connector = Plugin {
            family: PluginFamily::Connector,
            name: "broker".into(),
            package: "acme-connector".into(),
            factory: "plugin".into(),
            cfg: None,
            dependency: path_dependency(&directory.path().join("connector")),
        };
        add_plugin(&path, connector.clone()).expect("add connector");
        assert!(add_plugin(&path, connector).is_err());
        remove_plugin(&path, PluginFamily::Connector, "broker").expect("remove connector");
        assert_eq!(
            load_manifest(&path).expect("load manifest").plugins.len(),
            1
        );
    }

    #[test]
    fn runnable_manifest_requires_configurator_and_connector() {
        let (directory, mut manifest) = fixture();
        manifest
            .plugins
            .retain(|plugin| plugin.family != PluginFamily::Connector);
        assert!(
            validate_manifest_at(&manifest, directory.path(), true)
                .expect_err("missing connector")
                .to_string()
                .contains("connector plugin")
        );
    }

    #[test]
    fn dependency_rejects_ambiguous_sources() {
        let dependency = Dependency {
            version: Some("1".into()),
            git: Some("https://example.invalid/plugin".into()),
            ..Dependency::default()
        };
        assert!(validate_dependency(&dependency, Path::new(".")).is_err());
    }

    #[test]
    fn cargo_subcommand_argument_is_recognized() {
        assert!(is_cargo_subcommand_argument(OsStr::new("fujin")));
        assert!(!is_cargo_subcommand_argument(OsStr::new("build")));
    }
}
