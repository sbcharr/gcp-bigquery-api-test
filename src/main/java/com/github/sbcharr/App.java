package com.github.sbcharr;

import com.github.sbcharr.bigquery.StreamType;
import com.github.sbcharr.bigquery.handler.defaultstream.BqDataWriterDefaultStreamAsync;
import com.github.sbcharr.bigquery.handler.defaultstream.BqDataWriterDefaultStreamSync;
import com.github.sbcharr.bigquery.handler.defaultstream.BqDataWriterInsertAll;
import com.google.protobuf.Descriptors;

import java.io.IOException;

public class App {
  public static void main(String[] args) {
    String streamType = System.getProperty("BQ_WRITE_API_STREAM_TYPE");
    System.out.println("Stream type: " + streamType);
    try {
      if (StreamType.DEFAULT_SYNC.value().equals(streamType)) {
        BqDataWriterDefaultStreamSync.runWriteToDefaultStream();
      } else if (StreamType.DEFAULT_ASYNC.value().equals(streamType)) {
        BqDataWriterDefaultStreamAsync.runWriteToDefaultStream();
      } else if (StreamType.INSERT_ALL.value().equals(streamType)) {
        BqDataWriterInsertAll.runWriteToDefaultStream();
      } else {
        System.out.println("stream type does not match, exiting...");
      }
    } catch (Descriptors.DescriptorValidationException | InterruptedException | IOException e) {
      e.printStackTrace();
    }
  }
}
