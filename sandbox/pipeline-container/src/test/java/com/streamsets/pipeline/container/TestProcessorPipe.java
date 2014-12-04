/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.api.Module;
import com.streamsets.pipeline.api.Module.Info;
import com.streamsets.pipeline.api.Processor;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class TestProcessorPipe {

  @Test(expected = NullPointerException.class)
  public void testInvalidConstructor() {
    Module.Info info = new ModuleInfo("n", "v", "d", "in");
    Set<String> input = new HashSet<String>();
    input.add("a");
    Set<String> output = new HashSet<String>();
    output.add("a");
    new ProcessorPipe(new ArrayList<Info>(), new MetricRegistry(), info, null, input, output);
  }

  @Test
  public void testConstructor() {
    Module.Info info = new ModuleInfo("n", "v", "d", "in");
    Processor Processor = Mockito.mock(Processor.class);
    Set<String> input = new HashSet<String>();
    input.add("a");
    Set<String> output = new HashSet<String>();
    output.add("a");
    new ProcessorPipe(new ArrayList<Info>(), new MetricRegistry(), info, Processor, input, output);
  }

}
