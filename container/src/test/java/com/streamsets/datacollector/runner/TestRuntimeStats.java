/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class TestRuntimeStats {

  @Test
  public void testSerialize() throws IOException {
    long startTime = System.currentTimeMillis();
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    JsonGenerator jsonGenerator = new JsonFactory().createGenerator(bOut);
    RuntimeStats runtimeStats = new RuntimeStats();
    runtimeStats.setBatchCount(1);
    runtimeStats.setBatchStartTime(startTime);
    runtimeStats.setCurrentBatchAge(1);
    runtimeStats.setCurrentSourceOffset("1::0");
    runtimeStats.setTimeInCurrentStage(100);
    runtimeStats.setTimeOfLastReceivedRecord(startTime);

    runtimeStats.serialize(jsonGenerator);
    jsonGenerator.close();

    String ser = new String(bOut.toByteArray());
    ObjectMapper m = ObjectMapperFactory.get();
    RuntimeStats runtimeStats1 = m.readValue(ser, RuntimeStats.class);

    Assert.assertEquals(runtimeStats.getBatchCount(), runtimeStats1.getBatchCount());
    Assert.assertEquals(runtimeStats.getBatchStartTime(), runtimeStats1.getBatchStartTime());
    Assert.assertEquals(runtimeStats.getCurrentBatchAge(), runtimeStats1.getCurrentBatchAge());
    Assert.assertEquals(runtimeStats.getCurrentSourceOffset(), runtimeStats1.getCurrentSourceOffset());
    Assert.assertEquals(runtimeStats.getCurrentStage(), runtimeStats1.getCurrentStage());
    Assert.assertEquals(runtimeStats.getTimeInCurrentStage(), runtimeStats1.getTimeInCurrentStage());
    Assert.assertEquals(runtimeStats.getTimeOfLastReceivedRecord(), runtimeStats1.getTimeOfLastReceivedRecord());

  }

}
