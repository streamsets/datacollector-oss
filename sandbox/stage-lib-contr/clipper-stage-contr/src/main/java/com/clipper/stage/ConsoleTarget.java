/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.clipper.stage;

import com.streamsets.pipeline.api.*;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

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
      Iterator<String> filedNames = record.getFieldNames();
      pw.write("------------------------------------------------------------------------------------");
      while(filedNames.hasNext()) {
        String fieldName = filedNames.next();
        pw.write(fieldName);
        pw.write(" : ");
        pw.write(record.getField(fieldName).getValue().toString());
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
