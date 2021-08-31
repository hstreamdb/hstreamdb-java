#! /usr/bin/env bash

JAVA_FILES=$(find src/ -name "*.java")

for JAVA_FILE in ${JAVA_FILES}
do
 echo "formatting ${JAVA_FILE}"
 google-java-format -r ${JAVA_FILE}
done
