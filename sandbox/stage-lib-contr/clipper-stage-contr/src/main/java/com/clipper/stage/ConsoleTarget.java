/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.clipper.stage;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

@StageDef(description = "Prints the travel log, fare and the cumulative sum on the Console",
  label = "Console Target",
  version = "1.0")
public class ConsoleTarget implements Target {

  int batchCounter = 0;
  @Override
  public void init(Info info, Target.Context context) {

  }

  @Override
  public void write(Batch batch) {
    PrintWriter pw = null;
    try {
      pw = new PrintWriter(new BufferedOutputStream(
          new FileOutputStream("/Users/harikiran/Documents/workspace/streamsets/"
              + "dev/dist/target/streamsets-pipeline-0.0.1-SNAPSHOT/streamsets-pipeline-0.0.1-SNAPSHOT/clipperOut.txt"
          , true /*append*/)));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    Iterator<Record> it = batch.getRecords();
    pw.write(String.format("******************* Batch %d Start *********************", batchCounter++));

    while (it.hasNext()) {
      Record record = it.next();
      Field field = record.get();
      Map<String, Field> valueAsMap = field.getValueAsMap();

      pw.write("------------------------------------------------------------------------------------");
      for(Map.Entry<String, Field> e : valueAsMap.entrySet()) {
        String fieldName = e.getKey();
        pw.write(fieldName);
        pw.write(" : ");
        pw.write(e.getValue().toString());
        pw.println();
      }
      pw.write("------------------------------------------------------------------------------------");
      pw.println();
    }
    pw.write("******************* Batch end *********************");
    pw.flush();
    pw.close();
  }

  @Override
  public void destroy() {

  }
}
