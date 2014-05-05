#!/bin/bash

# echo "$$" > gush.pid

set -e

GUSH_DIR=/home/simao/w/gush
GUSH_JAR=$(ls -t $GUSH_DIR/target/scala-2.*/gush-assembly-*.jar | head -n 1)

cd $GUSH_DIR

exec java -jar $GUSH_JAR
