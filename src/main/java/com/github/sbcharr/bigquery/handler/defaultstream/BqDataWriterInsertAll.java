package com.github.sbcharr.bigquery.handler.defaultstream;

import com.github.sbcharr.Constants;
import com.github.sbcharr.Utils;
import com.github.sbcharr.bigquery.BqTestTableDTO;
import com.google.cloud.bigquery.*;
import com.google.protobuf.Descriptors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BqDataWriterInsertAll {
  private static final String PROJECT_ID = "molten-optics-378412";
  private static final String DATA_SET = "test_dataset";
  private static final String TABLE_NAME = "test_bq_api";

  public static void runWriteToDefaultStream()
      throws Descriptors.DescriptorValidationException, InterruptedException, IOException {
    long t1 = System.currentTimeMillis();
    writeToDefaultStream();
    System.out.println(
        "Took "
            + (System.currentTimeMillis() - t1) / 1000
            + " seconds to write "
            + Constants.APPEND_BATCH_SIZE * Constants.TOTAL_BATCHES
            + " records to BQ in INSERT ALL mode");
  }

  private static void writeToDefaultStream() {
    BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();
    TableId tableId = TableId.of(PROJECT_ID, DATA_SET, TABLE_NAME);
    List<Integer> tempList = new ArrayList<>();
    for (int i = 0; i < Constants.TOTAL_BATCHES; i++) {
      tempList.add(i);
    }
    System.out.println("Record size: " + 500 + " Bytes");
    tempList.parallelStream()
        .forEach(
            idx -> {
              List<InsertAllRequest.RowToInsert> rowToInsert = new ArrayList<>();
              Map<String, Object> rowContent = new HashMap<>();
              BqTestTableDTO apiDTO = new BqTestTableDTO();
              for (int i = 0; i < Constants.APPEND_BATCH_SIZE; i++) {
                apiDTO.field1 = Utils.generateRandomText(80);
                apiDTO.field2 = Utils.generateRandomText(60);
                apiDTO.field3 = Utils.generateRandomText(90);
                apiDTO.field4 = Utils.generateRandomText(100);
                apiDTO.field5 = Utils.generateRandomText(50);
                apiDTO.field6 = Utils.generateRandomText(120);
                rowContent.put("field1", apiDTO.field1);
                rowContent.put("field2", apiDTO.field2);
                rowContent.put("field3", apiDTO.field3);
                rowContent.put("field4", apiDTO.field4);
                rowContent.put("field5", apiDTO.field5);
                rowContent.put("field6", apiDTO.field6);
                rowToInsert.add(InsertAllRequest.RowToInsert.of(rowContent));
              }

              InsertAllResponse response =
                  bigquery.insertAll(
                      InsertAllRequest.newBuilder(tableId).setRows(rowToInsert).build());
              if (response.hasErrors()) {
                // Handle errors
                System.out.println("Error in inserting the batch to BQ");
              }
              rowContent.clear();
            });
  }
}
