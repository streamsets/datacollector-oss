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
package com.clipper.stage.test;

import com.clipper.stage.PDFLineProducer;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.testharness.SourceRunner;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TestClipper {

  @Test
  public void testClipper() throws StageException {
    Map<String, List<Record>> run = new SourceRunner.Builder<PDFLineProducer>()
      .addSource(PDFLineProducer.class)
      .maxBatchSize(10)
      .outputLanes(ImmutableSet.of("lane"))
      .configure("pdfLocation", "/Users/harikiran/Downloads/madhuridehistory.pdf")
      .build()
      .run();

    System.out.println("Fin. Number of lines read :" + run.get("lane").size());
    for(Record r : run.get("lane")) {
      System.out.println(r.toString());
    }
  }
}
