#!/bin/bash
set -e

# TODO: This would be much nicer to do in SBT, once SBT supports Maven-style
# resolution.

MVN="build/mvn --force"
# NOTE: These should match those in the release publishing script
INSTALL_PROFILES="-Phive-thriftserver -Pyarn -Phive -Phadoop-2.2"
LOCAL_REPO="mvn-tmp"

if [ -n "$AMPLAB_JENKINS" ]; then
  # To speed up Maven install process we remove source files
  # Maven dependency list only works once installed
  find . -name *.scala | xargs rm
  find . -name *.java | xargs rm
fi

# Use custom version to avoid Maven contention
spark_version="spark-$(date +%s | tail -c6)"
$MVN -q versions:set -DnewVersion=$spark_version > /dev/null

echo "Performing Maven install"
$MVN install -q \
  -pl '!assembly' \
  -pl '!examples' \
  -pl '!external/flume-assembly' \
  -pl '!external/kafka-assembly' \
  -pl '!external/twitter' \
  -pl '!external/flume' \
  -pl '!external/mqtt' \
  -pl '!external/mqtt-assembly' \
  -pl '!external/zeromq' \
  -pl '!external/kafka' \
  -DskipTests

echo "Generating dependency manifest"
$MVN dependency:build-classpath \
  | grep "Building Spark Project Assembly" -A 5 \
  | tail -n 1 | tr ":" "\n" | rev | cut -d "/" -f 1 | rev | sort \
  > dev/pr-deps

if [ -n "$AMPLAB_JENKINS" ]; then
  git reset --hard HEAD
fi

if [[ $@ == **replace-manifest** ]]; then
  echo "Replacing manifest and creating new file at dev/spark-deps"
  mv dev/pr-deps dev/spark-deps
  exit 0
fi

set +e
dep_diff="$(diff dev/pr-deps dev/spark-deps)"
set -e

if [ "$dep_diff" != "" ]; then
  echo "Spark's published dependencies DO NOT MATCH the manifest file (dev/spark-deps)."
  echo "To update the manifest file, run './dev/test-dependencies --replace-manifest'."
  echo "$dep_diff"
  exit 1
fi
