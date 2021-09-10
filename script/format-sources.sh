#! /usr/bin/env bash

CLIENT_JAVA_FILES=$(find client/src/ -name "*.java")
for CLIENT_JAVA_FILE in ${CLIENT_JAVA_FILES}
do
 echo "formatting ${CLIENT_JAVA_FILE}"
 google-java-format -r ${CLIENT_JAVA_FILE}
done

EXAMPLE_JAVA_FILES=$(find examples/src/ -name "*.java")
for EXAMPLE_JAVA_FILE in ${EXAMPLE_JAVA_FILES}
do
 echo "formatting ${EXAMPLE_JAVA_FILE}"
 google-java-format -r ${EXAMPLE_JAVA_FILE}
done
