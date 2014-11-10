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

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;

import java.io.PrintWriter;
import java.util.Iterator;

@StageDef(description = "Prints the travel log, fare and the cumulative sum on the Console",
  label = "Console Target",
  name = "ConsoleTarget",
  version = "1.0")
public class ConsoleTarget implements Target {

  @Override
  public void init(Info info, Target.Context context) {

  }

  @Override
  public void write(Batch batch) {
    PrintWriter pw = new PrintWriter(System.out);
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      Iterator<String> filedNames = record.getFieldNames();
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
    pw.flush();
    pw.close();
  }

  @Override
  public void destroy() {

  }
}
