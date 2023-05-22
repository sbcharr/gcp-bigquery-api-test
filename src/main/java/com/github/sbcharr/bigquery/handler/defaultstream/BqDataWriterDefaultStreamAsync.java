package com.github.sbcharr.bigquery.handler.defaultstream;

import com.github.sbcharr.Constants;
import com.github.sbcharr.Utils;
import com.github.sbcharr.bigquery.BqTestTableDTO;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.cloud.bigquery.storage.v1.Exceptions.AppendSerializationError;
import com.google.cloud.bigquery.storage.v1.Exceptions.StorageException;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import io.grpc.Status;
import io.grpc.Status.Code;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;




public class BqDataWriterDefaultStreamAsync {
  private static final String PROJECT_ID = "molten-optics-378412";
  private static final String DATA_SET = "test_dataset";
  private static final String TABLE_NAME = "test_bq_api";

  private static long totalTransferredBytes = 0;

  public static void runWriteToDefaultStream()
      throws DescriptorValidationException, InterruptedException, IOException {
    long t1 = System.currentTimeMillis();
    writeToDefaultStream(PROJECT_ID, DATA_SET, TABLE_NAME);
    System.out.println(
        "Took "
            + (System.currentTimeMillis() - t1) / 1000
            + " seconds to write "
            + Constants.APPEND_BATCH_SIZE * Constants.TOTAL_BATCHES
            + " records with total size " + totalTransferredBytes/1024 + " kilo bytes to BQ in ASYNC mode");
  }

  public static void writeToDefaultStream(String projectId, String datasetName, String tableName)
      throws DescriptorValidationException, InterruptedException, IOException {
    TableName parentTable = TableName.of(projectId, datasetName, tableName);
    System.out.println("table name " + parentTable);

    DataWriter writer = new DataWriter();
    // initialize writer
    writer.initialize(parentTable);

    List<Integer> tempList = new ArrayList<>();
    for (int i = 0; i < Constants.TOTAL_BATCHES; i++) {
      tempList.add(i);
    }
    // System.out.println("Record size: " + 500 + " Bytes");
    tempList.parallelStream()
        .forEach(
            idx -> {
              // Create a JSON object that is compatible with the table schema.
              JSONArray jsonArr = new JSONArray();
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
                //        StringBuilder sbSuffix = new StringBuilder();
                //        for (int k = 0; k < j; k++) {
                //          sbSuffix.append(k);
                //        }
                //        record.put("test_string", String.format("record %03d-%03d %s", i, j,
                // sbSuffix.toString()));
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
    // verifyExpectedRowCount(parentTable, 12);
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

    private static final int MAX_RETRY_COUNT = 3;
    private static final int MAX_RECREATE_COUNT = 3;
    private static final ImmutableList<Code> RETRIABLE_ERROR_CODES =
        ImmutableList.of(
            Code.INTERNAL,
            Code.ABORTED,
            Code.CANCELLED,
            Code.FAILED_PRECONDITION,
            Code.DEADLINE_EXCEEDED,
            Code.UNAVAILABLE);

    // Track the number of in-flight requests to wait for all responses before shutting down.
    private final Phaser inflightRequestCount = new Phaser(1);
    private final Object lock = new Object();
    private JsonStreamWriter streamWriter1;
    private JsonStreamWriter streamWriter2;
    private JsonStreamWriter streamWriter3;
    private List<JsonStreamWriter> streamWriters;
    private int streamWriterIndex;

    @GuardedBy("lock")
    private RuntimeException error = null;

    private AtomicInteger recreateCount = new AtomicInteger(0);

    public void initialize(TableName parentTable)
        throws DescriptorValidationException, IOException, InterruptedException {
      streamWriter1 =
          JsonStreamWriter.newBuilder(parentTable.toString(), BigQueryWriteClient.create()).build();
      streamWriter2 =
          JsonStreamWriter.newBuilder(parentTable.toString(), BigQueryWriteClient.create()).build();
      streamWriter3 =
          JsonStreamWriter.newBuilder(parentTable.toString(), BigQueryWriteClient.create()).build();
      addStreamWriters();
    }

    private List<JsonStreamWriter> getStreamWriters() {
      return streamWriters;
    }

    private void addStreamWriters() {
      streamWriters = new ArrayList<>();
      streamWriters.add(streamWriter1);
      streamWriters.add(streamWriter2);
      streamWriters.add(streamWriter3);
    }

    //    public boolean isClosedStreamWriter() {
    //      return streamWriter == null;
    //    }

    public void append(AppendContext appendContext)
        throws DescriptorValidationException, IOException, InterruptedException {
      synchronized (this.lock) {
        if (streamWriterIndex == streamWriters.size()) {
          streamWriterIndex = 0;
        }
        if (!streamWriters.get(streamWriterIndex).isUserClosed()
            && streamWriters.get(streamWriterIndex).isClosed()
            && recreateCount.getAndIncrement() < MAX_RECREATE_COUNT) {
          JsonStreamWriter streamWriter =
              JsonStreamWriter.newBuilder(
                      streamWriters.get(streamWriterIndex).getStreamName(),
                      BigQueryWriteClient.create())
                  .build();
          streamWriters.set(streamWriterIndex, streamWriter);
          this.error = null;
        }
        // If earlier appends have failed, we need to reset before continuing.
        if (this.error != null) {
          throw this.error;
        }
        streamWriterIndex++;
      }
      long t1 = System.currentTimeMillis();
      ApiFuture<AppendRowsResponse> future =
          streamWriters.get(streamWriterIndex - 1).append(appendContext.data);
//      System.out.println("stream writer at index " + (streamWriterIndex - 1) + " has writer " + streamWriters.get(streamWriterIndex - 1));
      //      System.out.println(
      //          "took " + (System.currentTimeMillis() - t1) + " millis to get the append future
      // object");
      ApiFutures.addCallback(
          future, new AppendCompleteCallback(this, appendContext), MoreExecutors.directExecutor());

      inflightRequestCount.register();
    }

    public void cleanup() {
      // Wait for all in-flight requests to complete.
      inflightRequestCount.arriveAndAwaitAdvance();
      for (JsonStreamWriter streamWriter : streamWriters) {
        streamWriter.close();
      }

      // Verify that no error occurred in the stream.
      synchronized (this.lock) {
        if (this.error != null) {
          throw this.error;
        }
      }
    }

    static class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {

      private final DataWriter parent;
      private final AppendContext appendContext;

      public AppendCompleteCallback(DataWriter parent, AppendContext appendContext) {
        this.parent = parent;
        this.appendContext = appendContext;
      }

      public void onSuccess(AppendRowsResponse response) {
        // System.out.format("Append success\n");
        this.parent.recreateCount.set(0);
        done();
      }

      public void onFailure(Throwable throwable) {
        System.out.println("Append failed");
        Status status = Status.fromThrowable(throwable);
        if (appendContext.retryCount < MAX_RETRY_COUNT
            && RETRIABLE_ERROR_CODES.contains(status.getCode())) {
          appendContext.retryCount++;
          try {
            this.parent.append(appendContext);
            done();
            return;
          } catch (Exception e) {
            System.out.format("Failed to retry append: %s\n", e);
          }
        }

        if (throwable instanceof AppendSerializationError) {
          AppendSerializationError ase = (AppendSerializationError) throwable;
          Map<Integer, String> rowIndexToErrorMessage = ase.getRowIndexToErrorMessage();
          if (rowIndexToErrorMessage.size() > 0) {
            // Omit the faulty rows
            JSONArray dataNew = new JSONArray();
            for (int i = 0; i < appendContext.data.length(); i++) {
              if (!rowIndexToErrorMessage.containsKey(i)) {
                dataNew.put(appendContext.data.get(i));
              } else {
                // TODO: process faulty rows by placing them on a dead-letter-queue, for instance
              }
            }

            // Retry the remaining valid rows, but using a separate thread to
            // avoid potentially blocking while we are in a callback.
            if (dataNew.length() > 0) {
              try {
                this.parent.append(new AppendContext(dataNew, 0));
              } catch (DescriptorValidationException | IOException | InterruptedException e) {
                throw new RuntimeException(e);
              }
            }
            done();
            return;
          }
        }

        synchronized (this.parent.lock) {
          if (this.parent.error == null) {
            StorageException storageException = Exceptions.toStorageException(throwable);
            this.parent.error =
                (storageException != null) ? storageException : new RuntimeException(throwable);
          }
        }
        done();
      }

      private void done() {
        this.parent.inflightRequestCount.arriveAndDeregister();
      }
    }
  }
}
