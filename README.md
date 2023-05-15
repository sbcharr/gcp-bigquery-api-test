A repository to assess the performance of GCP Big Query Storage APIs

If you want to build the repo, please follow the steps in a linux machine,

1. Make sure to install atleast java version 11.
2. Make sure to install `git`, `zip`, `unzip` and `maven`.
3. Clone the repo.
4. cd to the project top folder and execute

```
./mkbuild.sh.
```

5. This will create 2 files, gcp-bigquery-api-test-1.0-SNAPSHOT.jar and gcp-bigquery-api-test-1.0-SNAPSHOT.zip. If you
want to distribute, then .zip file has the required executable and libraries bundled.
6. Before executing, set the `GOOGLE_APPLICATION_CREDENTIALS` env variable.
7. Run the command to execute,

```
java -DBQ_WRITE_API_STREAM_TYPE=default_async -jar target/gcp-bigquery-api-test-1.0-SNAPSHOT.jar
```

Here, `BQ_WRITE_API_STREAM_TYPE` is the stream type, various values are `default_sync`, `default_async` and `insert_all`.
