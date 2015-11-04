#!/usr/bin/env sh
set -e
sbt publishLocal createVersionFile
cd e2e
sbt clean test
echo "Clean up the loaclly published artifacts!"
