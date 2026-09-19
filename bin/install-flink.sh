#!/usr/bin/env sh

set -eu

flink_line=2.2
if [ "${1:-}" = --flink-line ]; then
  if [ "$#" -lt 3 ]; then echo "usage: $0 [--flink-line 2.2|1.18] <FLINK_HOME>" >&2; exit 64; fi
  flink_line=$2
  shift 2
fi
if [ "$#" -ne 1 ]; then
  echo "usage: $0 [--flink-line 2.2|1.18] <FLINK_HOME>" >&2
  exit 64
fi
flink_home=$1
artifact_suffix=
case "$flink_line" in
  2.2) ;;
  1.18) artifact_suffix=-flink1.18 ;;
  *) echo "unsupported Flink line: $flink_line" >&2; exit 64 ;;
esac
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
artifact_version=$(cd "$repo_root" && mvn -q -DforceStdout help:evaluate -Dexpression=project.version)
loader_jar=$repo_root/streamfusion-loader/target/streamfusion-loader$artifact_suffix-$artifact_version.jar
core_jar=$repo_root/streamfusion-core/target/streamfusion-core$artifact_suffix-$artifact_version-runtime.jar

if [ ! -d "$flink_home/lib" ]; then
  echo "Flink lib directory does not exist: $flink_home/lib" >&2
  exit 66
fi

if [ ! -f "$loader_jar" ] || [ ! -f "$core_jar" ]; then
  echo "Build the deployment bundle first: bin/build-release.sh" >&2
  exit 66
fi

for jar_file in "$loader_jar" "$core_jar"; do
  payload_line=$(unzip -p "$jar_file" META-INF/MANIFEST.MF | tr -d '\r' | sed -n 's/^StreamFusion-Flink-Line: //p')
  if [ "$payload_line" != "$flink_line" ]; then
    echo "Payload $jar_file targets Flink $payload_line, expected $flink_line" >&2
    exit 65
  fi
done
set -- "$flink_home"/lib/flink-dist-*.jar
if [ "$#" -ne 1 ]; then
  echo "Expected exactly one Flink distribution JAR in $flink_home/lib" >&2
  exit 65
fi
case "$flink_line:$(basename "$1")" in
  1.18:flink-dist-1.18.1.jar|2.2:flink-dist-2.2.0.jar|2.2:flink-dist-2.2.1.jar) ;;
  *) echo "Flink distribution $(basename "$1") does not match the selected supported line $flink_line" >&2; exit 65 ;;
esac

cp "$loader_jar" "$flink_home/lib/00-streamfusion-loader.jar"
cp "$core_jar" "$flink_home/lib/streamfusion-core.jar"

echo "Installed StreamFusion into $flink_home/lib. Restart Flink before submitting jobs."
