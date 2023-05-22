package com.github.sbcharr.bigquery.handler.defaultstream;

import com.github.sbcharr.Constants;
import com.github.sbcharr.Utils;
import com.github.sbcharr.bigquery.BqTestTableDTO;
import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class BqDataWriterDefaultStreamSync {
  private static final String PROJECT_ID = "molten-optics-378412";
  private static final String DATA_SET = "test_dataset";
  private static final String TABLE_NAME = "test_bq_api";

  private static long totalTransferredBytes = 0;

  public static void runWriteToDefaultStream()
      throws DescriptorValidationException, InterruptedException, IOException {
    long t1 = System.currentTimeMillis();
    writeToDefaultStream();
    System.out.println(
        "Took "
            + (System.currentTimeMillis() - t1) / 1000
            + " seconds to write "
            + Constants.APPEND_BATCH_SIZE * Constants.TOTAL_BATCHES
            + " records with total size "
            + totalTransferredBytes / 1024
            + " kilo bytes to BQ in SYNC mode");
  }

  public static void writeToDefaultStream()
      throws DescriptorValidationException, InterruptedException, IOException {
    TableName parentTable = TableName.of(PROJECT_ID, DATA_SET, TABLE_NAME);
    System.out.println("table name " + parentTable);

    DataWriter writer = new DataWriter();
    // initialize writer
    writer.initialize(parentTable);

    List<Integer> tempList = new ArrayList<>();
    for (int i = 0; i < Constants.TOTAL_BATCHES; i++) {
      tempList.add(i);
    }

    tempList.parallelStream()
        .forEach(
            idx -> {
              // Create a JSON object that is compatible with the table schema.
              JSONArray jsonArr = new JSONArray(1);
              BqTestTableDTO apiDTO = new BqTestTableDTO();
              for (int j = 0; j < Constants.APPEND_BATCH_SIZE; j++) {
                apiDTO.field1 = Utils.generateRandomText(400);
                apiDTO.field2 = Utils.generateRandomText(450);
                apiDTO.field3 = Utils.generateRandomText(350);
                apiDTO.field4 = Utils.generateRandomText(550);
                apiDTO.field5 = Utils.generateRandomText(500);
                apiDTO.field6 = Utils.generateRandomText(350);

                JSONObject record = new JSONObject();
                record.put("field1", apiDTO.field1);
                record.put("field2", apiDTO.field2);
                record.put("field3", apiDTO.field3);
                record.put("field4", apiDTO.field4);
                record.put("field5", apiDTO.field5);
                record.put("field6", apiDTO.field6);

                synchronized ("lock") {
                  totalTransferredBytes += Utils.getByteSizeOfJsonString(record.toString());
                }
                jsonArr.put(record);
              }

              try {
                writer.append(new AppendContext(jsonArr, 0));
              } catch (DescriptorValidationException | IOException | InterruptedException e) {
                e.printStackTrace();
              }
            });

    // Final cleanup for the stream during worker teardown.
    writer.cleanup();
    System.out.println("Appended records successfully.");
  }

  private static class AppendContext {

    JSONArray data;
    int retryCount = 0;

    AppendContext(JSONArray data, int retryCount) {
      this.data = data;
      this.retryCount = retryCount;
    }
  }

  private static class DataWriter {
    private static final int MAX_RECREATE_COUNT = 3;

    private final Object lock = new Object();
    private JsonStreamWriter streamWriter;

    @GuardedBy("lock")
    private RuntimeException error = null;

    private AtomicInteger recreateCount = new AtomicInteger(0);

    public void initialize(TableName parentTable)
        throws DescriptorValidationException, IOException, InterruptedException {
      streamWriter =
          JsonStreamWriter.newBuilder(parentTable.toString(), BigQueryWriteClient.create()).build();
    }

    public void append(AppendContext appendContext)
        throws DescriptorValidationException, IOException, InterruptedException {
      synchronized (this.lock) {
        if (!streamWriter.isUserClosed()
            && streamWriter.isClosed()
            && recreateCount.getAndIncrement() < MAX_RECREATE_COUNT) {
          streamWriter =
              JsonStreamWriter.newBuilder(
                      streamWriter.getStreamName(), BigQueryWriteClient.create())
                  .build();
          this.error = null;
        }
        // If earlier appends have failed, we need to reset before continuing.
        if (this.error != null) {
          throw this.error;
        }
      }
      long t1 = System.currentTimeMillis();
      ApiFuture<AppendRowsResponse> future = streamWriter.append(appendContext.data);
      //      System.out.println(
      //          "took " + (System.currentTimeMillis() - t1) + " millis to get the append future
      // object");

      try {
        AppendRowsResponse response = future.get();
        if (response.hasError()) {
          System.out.println("Failed to append records for the batch");
        }
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }

    public void cleanup() {
      streamWriter.close();

      // Verify that no error occurred in the stream.
      synchronized (this.lock) {
        if (this.error != null) {
          throw this.error;
        }
      }
    }
  }
}
