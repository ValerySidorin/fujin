#!/usr/bin/env bash
set -euo pipefail

CRATES_USER_AGENT="fujin-release/1.0 (https://github.com/fujin-io/fujin)"
VERSION="${VERSION#v}"
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "VERSION must be an unprefixed semantic version" >&2
  exit 1
fi

workspace_version=$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\([^"]*\)"/\1/p' Cargo.toml)
if [[ "$workspace_version" != "$VERSION" ]]; then
  echo "workspace version $workspace_version does not match VERSION $VERSION" >&2
  exit 1
fi
if [[ "${DRY_RUN:-0}" == 1 ]]; then
  metadata=$(cargo metadata --no-deps --format-version 1)
  patch_args=()
  while IFS= read -r wave; do
    read -r -a crates <<<"$wave"
    for crate in "${crates[@]}"; do
      manifest=$(jq -er --arg crate "$crate" '.packages[] | select(.name == $crate) | .manifest_path' <<<"$metadata")
      patch_args+=(--config "patch.crates-io.$crate.path=\"$(dirname "$manifest")\"")
    done
  done < <(sed '/^[[:space:]]*#/d; /^[[:space:]]*$/d' scripts/release_crates.txt)

  while IFS= read -r wave; do
    read -r -a crates <<<"$wave"
    for crate in "${crates[@]}"; do
      cargo package --quiet --locked --allow-dirty -p "$crate" "${patch_args[@]}"
      echo "package verified: $crate $VERSION"
    done
  done < <(sed '/^[[:space:]]*#/d; /^[[:space:]]*$/d' scripts/release_crates.txt)
  exit 0
fi

published() {
  local crate=$1
  local status
  status=$(curl --silent --show-error --location --user-agent "$CRATES_USER_AGENT" --output /dev/null --write-out '%{http_code}' \
    "https://crates.io/api/v1/crates/$crate/$VERSION") || {
    echo "failed to query crates.io for $crate $VERSION" >&2
    exit 1
  }
  case "$status" in
    200) return 0 ;;
    404) return 1 ;;
    *)
      echo "crates.io returned HTTP $status for $crate $VERSION" >&2
      exit 1
      ;;
  esac
}

verify_published() {
  local crate=$1
  local expected actual archive
  expected=$(curl --fail --silent --show-error --user-agent "$CRATES_USER_AGENT" \
    "https://crates.io/api/v1/crates/$crate/$VERSION" | jq -r '.version.checksum')
  cargo package --locked --no-verify -p "$crate" >/dev/null
  archive="target/package/$crate-$VERSION.crate"
  actual=$(sha256sum "$archive" | cut -d ' ' -f 1)
  if [[ "$actual" != "$expected" ]]; then
    echo "published $crate $VERSION checksum does not match this release commit" >&2
    exit 1
  fi
}

wait_until_published() {
  local crate=$1
  for attempt in $(seq 1 30); do
    if published "$crate" && cargo info "$crate@$VERSION" --registry crates-io >/dev/null 2>&1; then
      return 0
    fi
    echo "waiting for $crate $VERSION in crates.io API and index ($attempt/30)"
    sleep 10
  done
  echo "$crate $VERSION was uploaded but did not become observable" >&2
  return 1
}

while IFS= read -r wave; do
  read -r -a crates <<<"$wave"
  for crate in "${crates[@]}"; do
    if published "$crate"; then
      echo "verifying existing publication: $crate $VERSION"
      verify_published "$crate"
    fi
  done
done < <(sed '/^[[:space:]]*#/d; /^[[:space:]]*$/d' scripts/release_crates.txt)

while IFS= read -r wave; do
  read -r -a crates <<<"$wave"
  for crate in "${crates[@]}"; do
    if published "$crate"; then
      echo "already published: $crate $VERSION"
      continue
    fi
    echo "publishing: $crate $VERSION"
    cargo publish --locked -p "$crate"
  done
  for crate in "${crates[@]}"; do
    wait_until_published "$crate"
  done
  for crate in "${crates[@]}"; do
    verify_published "$crate"
  done
done < <(sed '/^[[:space:]]*#/d; /^[[:space:]]*$/d' scripts/release_crates.txt)
