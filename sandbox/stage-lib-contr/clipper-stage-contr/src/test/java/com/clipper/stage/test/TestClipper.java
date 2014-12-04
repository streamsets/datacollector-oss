/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
