#!/usr/bin/env bash
mvn clean package dependency:copy-dependencies -DoutputDirectory=target/lib
zip -u target/gcp-bigquery-api-test-1.0-SNAPSHOT.zip target/lib/* target/gcp-bigquery-api-test-1.0-SNAPSHOT.jar
mv target/gcp-bigquery-api-test-1.0-SNAPSHOT.zip ~/Downloads/gcp-bigquery-api-test-1.0-SNAPSHOT.zip