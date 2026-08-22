package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	fujinService = "github.com/fujin-io/fujin/public/service"
	fujinCABI    = "github.com/fujin-io/fujin/public/embedded/cabi"
	moduleName   = "tmpfujin"
)

var (
	configurators   stringSlice
	connectors      stringSlice
	transports      stringSlice
	bindMiddlewares stringSlice
	connMiddlewares stringSlice
	replacements    stringSlice
	output          = flag.String("output", "fujin", "Output binary path")
	buildTags       = flag.String("tags", "netgo,osusergo", "Build tags for the final binary (e.g. fujin,grpc for transports)")
	extraLdflags    = flag.String("ldflags", "", "Extra ldflags (e.g. -X main.Version=1.0.0)")
	cgoEnabled      = flag.Bool("cgo", false, "Enable CGO (required by some plugins)")
	localModule     = flag.Bool("local", false, "Use local fujin module (for builds from source)")
	outputKind      = flag.String("buildmode", "executable", "Output kind: executable, c-shared, or c-archive")
)

type stringSlice []string

func (s *stringSlice) String() string { return strings.Join(*s, ",") }
func (s *stringSlice) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func init() {
	flag.Var(&configurators, "configurator", "Configurator plugins")
	flag.Var(&transports, "transport", "Transport plugins")
	flag.Var(&connectors, "connector", "Connector plugins")
	flag.Var(&bindMiddlewares, "bind-middleware", "Bind middleware plugins")
	flag.Var(&connMiddlewares, "connector-middleware", "Connector middleware plugins")
	flag.Var(&replacements, "replace", "Local module replacement module=path (repeatable)")
}

func main() {
	flag.Parse()

	kind, err := parseBuildKind(*outputKind)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	if err := validateInputs(kind); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if err := runBuild(buildOpts{
		outputPath:   *output,
		plugins:      collectPlugins(),
		replacements: replacements,
		tags:         *buildTags,
		extraLdflags: *extraLdflags,
		cgoEnabled:   *cgoEnabled || kind != buildExecutable,
		localModule:  *localModule,
		kind:         kind,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Fujin %s built successfully: %s\n", kind, *output)
}

type buildKind string

const (
	buildExecutable buildKind = "executable"
	buildCShared    buildKind = "c-shared"
	buildCArchive   buildKind = "c-archive"
)

func parseBuildKind(value string) (buildKind, error) {
	kind := buildKind(strings.TrimSpace(value))
	switch kind {
	case buildExecutable, buildCShared, buildCArchive:
		return kind, nil
	default:
		return "", fmt.Errorf("invalid buildmode %q: expected executable, c-shared, or c-archive", value)
	}
}

type buildOpts struct {
	outputPath   string
	plugins      []string
	replacements []string
	tags         string
	extraLdflags string
	cgoEnabled   bool
	localModule  bool
	kind         buildKind
}

func runBuild(opts buildOpts) error {
	tmpDir, err := os.MkdirTemp("", "fujin-builder-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	if err := runGo(tmpDir, "mod", "init", moduleName); err != nil {
		return err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get working dir: %w", err)
	}
	if opts.localModule {
		if err := runGo(tmpDir, "mod", "edit", "-replace", "github.com/fujin-io/fujin="+cwd); err != nil {
			return fmt.Errorf("add Fujin replace directive: %w", err)
		}
	}
	for _, replacement := range opts.replacements {
		normalized, err := normalizeReplacement(replacement, cwd)
		if err != nil {
			return err
		}
		if err := runGo(tmpDir, "mod", "edit", "-replace", normalized); err != nil {
			return fmt.Errorf("add replace directive %q: %w", replacement, err)
		}
	}
	rootPackage := fujinService
	if opts.kind != buildExecutable {
		rootPackage = fujinCABI
	}
	if err := runGo(tmpDir, "get", rootPackage); err != nil {
		return err
	}

	for _, pkg := range opts.plugins {
		if err := runGo(tmpDir, "get", pkg); err != nil {
			return fmt.Errorf("go get %s: %w", pkg, err)
		}
	}

	plugins := pluginsByType{
		configurators:   configurators,
		connectors:      connectors,
		transports:      transports,
		bindMiddlewares: bindMiddlewares,
		connMiddlewares: connMiddlewares,
	}
	mainContent := generateMain(plugins)
	if opts.kind != buildExecutable {
		mainContent = generateLibraryMain(plugins)
	}
	mainPath := filepath.Join(tmpDir, "main.go")
	if err := os.WriteFile(mainPath, []byte(mainContent), 0644); err != nil {
		return fmt.Errorf("write main.go: %w", err)
	}

	outPath, err := filepath.Abs(opts.outputPath)
	if err != nil {
		return fmt.Errorf("output path: %w", err)
	}

	ldflags := "-s -w"
	if opts.extraLdflags != "" {
		ldflags = ldflags + " " + opts.extraLdflags
	}
	cgo := "0"
	if opts.cgoEnabled {
		cgo = "1"
	}
	env := append(os.Environ(), "CGO_ENABLED="+cgo)
	args := []string{"build"}
	if opts.kind != buildExecutable {
		args = append(args, "-buildmode="+string(opts.kind))
	}
	args = append(args, "-ldflags", ldflags, "-tags", opts.tags, "-o", outPath, ".")
	if err := runGoWithEnv(tmpDir, env, args...); err != nil {
		return err
	}

	return nil
}

func validateInputs(kind buildKind) error {
	if kind == buildExecutable && len(configurators) == 0 {
		return fmt.Errorf("at least one configurator is required for executable builds (e.g. -configurator github.com/fujin-io/fujin/public/plugins/configurator/yaml)")
	}
	if len(connectors) == 0 {
		return fmt.Errorf("at least one connector is required (e.g. -connector github.com/fujin-io/fujin/public/plugins/connector/kafka/franz)")
	}
	if strings.TrimSpace(*output) == "" {
		return fmt.Errorf("output path cannot be empty")
	}
	seen := make(map[string]bool)
	for _, pkg := range collectPlugins() {
		if seen[pkg] {
			return fmt.Errorf("duplicate plugin: %s", pkg)
		}
		seen[pkg] = true
		if strings.TrimSpace(pkg) == "" {
			return fmt.Errorf("plugin package path cannot be empty")
		}
	}
	effectiveCGO := *cgoEnabled || kind != buildExecutable
	if err := validatePluginRequirements(connectors, *buildTags, effectiveCGO); err != nil {
		return err
	}
	for _, replacement := range replacements {
		if _, err := normalizeReplacement(replacement, "."); err != nil {
			return err
		}
	}
	return nil
}

func validatePluginRequirements(connectorPackages []string, tags string, cgo bool) error {
	const zeromqPebbe = "github.com/fujin-io/fujin/public/plugins/connector/zeromq/pebbe"
	selected := false
	for _, pkg := range connectorPackages {
		if pkg == zeromqPebbe {
			selected = true
			break
		}
	}
	if !selected {
		return nil
	}
	if !cgo {
		return fmt.Errorf("connector %s requires -cgo", zeromqPebbe)
	}
	for _, tag := range strings.FieldsFunc(tags, func(r rune) bool { return r == ',' || r == ' ' }) {
		if tag == "zeromq_pebbe" {
			return nil
		}
	}
	return fmt.Errorf("connector %s requires build tag zeromq_pebbe", zeromqPebbe)
}

func normalizeReplacement(replacement, baseDir string) (string, error) {
	parts := strings.SplitN(replacement, "=", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", fmt.Errorf("invalid replacement %q: expected module=path", replacement)
	}
	module := strings.TrimSpace(parts[0])
	path := strings.TrimSpace(parts[1])
	if !filepath.IsAbs(path) {
		absolute, err := filepath.Abs(filepath.Join(baseDir, path))
		if err != nil {
			return "", fmt.Errorf("resolve replacement %q: %w", replacement, err)
		}
		path = absolute
	}
	return module + "=" + path, nil
}

func collectPlugins() []string {
	var all []string
	all = append(all, configurators...)
	all = append(all, connectors...)
	all = append(all, transports...)
	all = append(all, bindMiddlewares...)
	all = append(all, connMiddlewares...)
	return all
}

type pluginsByType struct {
	configurators   []string
	connectors      []string
	transports      []string
	bindMiddlewares []string
	connMiddlewares []string
}

func generateMain(p pluginsByType) string {
	var imports []string
	imports = append(imports,
		`"context"`,
		`"os/signal"`,
		fmt.Sprintf(`"%s"`, fujinService),
	)
	for _, imp := range p.configurators {
		imports = append(imports, fmt.Sprintf(`_ "%s"`, imp))
	}
	for _, imp := range p.connectors {
		imports = append(imports, fmt.Sprintf(`_ "%s"`, imp))
	}
	for _, imp := range p.transports {
		imports = append(imports, fmt.Sprintf(`_ "%s"`, imp))
	}
	for _, imp := range p.bindMiddlewares {
		imports = append(imports, fmt.Sprintf(`_ "%s"`, imp))
	}
	for _, imp := range p.connMiddlewares {
		imports = append(imports, fmt.Sprintf(`_ "%s"`, imp))
	}

	sb := strings.Builder{}
	sb.WriteString("package main\n\n")
	sb.WriteString("import (\n")
	for _, imp := range imports {
		sb.WriteString("\t" + imp + "\n")
	}
	sb.WriteString(")\n\n")
	sb.WriteString("var Version string\n\n")
	sb.WriteString(`func main() {
	service.Version = Version
	ctx, cancel := signal.NotifyContext(context.Background(), service.ShutdownSignals()...)
	defer cancel()
	service.RunCLI(ctx)
}
`)
	return sb.String()
}

func runGo(dir string, args ...string) error {
	return runGoWithEnv(dir, os.Environ(), args...)
}

func runGoWithEnv(dir string, env []string, args ...string) error {
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("go %s: %w\n%s", strings.Join(args, " "), err, out)
	}
	return nil
}
