#!/usr/bin/env sh

set -eu

flink_line=2.2
if [ "${1:-}" = "--flink-line" ]; then
  [ "$#" -ge 2 ] || { echo "usage: $0 [--flink-line <line>] <FLINK_HOME>" >&2; exit 64; }
  flink_line=$2
  shift 2
fi

if [ "$#" -ne 1 ]; then
  echo "usage: $0 [--flink-line <line>] <FLINK_HOME>" >&2
  exit 64
fi

flink_home=$1
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
artifact_version=$(cd "$repo_root" && mvn -q -DforceStdout help:evaluate -Dexpression=project.version)
loader_jar=$repo_root/streamfusion-loader/target/streamfusion-loader-flink$flink_line-$artifact_version.jar
core_jar=$repo_root/streamfusion-core/target/streamfusion-core-flink$flink_line-$artifact_version-runtime.jar

if [ ! -d "$flink_home/lib" ]; then
  echo "Flink lib directory does not exist: $flink_home/lib" >&2
  exit 66
fi

if [ ! -f "$loader_jar" ] || [ ! -f "$core_jar" ]; then
  echo "Build the deployment bundle first: bin/build-release.sh" >&2
  exit 66
fi

cp "$loader_jar" "$flink_home/lib/00-streamfusion-loader.jar"
cp "$core_jar" "$flink_home/lib/streamfusion-core.jar"

echo "Installed StreamFusion into $flink_home/lib. Restart Flink before submitting jobs."
