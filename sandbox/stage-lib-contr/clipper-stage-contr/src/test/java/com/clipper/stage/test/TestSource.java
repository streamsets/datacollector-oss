/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.clipper.stage.test;

import com.clipper.stage.PDFLineProducer;
import com.clipper.stage.RandomSource;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.testharness.SourceRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TestSource {

  @Test
  public void testPdfLineProducer() {
    try {
      Map<String, List<Record>> run = new SourceRunner.Builder<PDFLineProducer>()
        .addSource(PDFLineProducer.class)
        .configure("pdfLocation", "/Users/Harikiran/Downloads/madhuridehistory.pdf")
        .build().run();
      System.out.println("Fin. Number of lines read :" + run.get("lane").size());
      for(Record r : run.get("lane")) {
        System.out.println(r.toString());
      }
    } catch (StageException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testRandomSource() {
    try {

      Map<String, List<Record>> run = new SourceRunner.Builder<RandomSource>()
        .addSource(RandomSource.class)
        .sourceOffset(null)
        .maxBatchSize(25)
        .configure("fields", "name,age,employer,city,state,country")
        .outputLanes(ImmutableSet.of("lane"))
        .build().run();

      Assert.assertNotNull(run);
      Assert.assertNotNull(run.get("lane"));
      Assert.assertTrue(run.get("lane").size() == 25);
    } catch (StageException e) {
      e.printStackTrace();
    }
  }
}
