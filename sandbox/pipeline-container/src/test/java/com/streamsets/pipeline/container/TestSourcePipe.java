/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.api.Module;
import com.streamsets.pipeline.api.Module.Info;
import com.streamsets.pipeline.api.Source;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class TestSourcePipe {

  @Test(expected = NullPointerException.class)
  public void testInvalidConstructor() {
    Module.Info info = new ModuleInfo("n", "v", "d", "in");
    Set<String> output = new HashSet<String>();
    output.add("a");
    new SourcePipe(new ArrayList<Info>(), new MetricRegistry(), info, null, output);
  }

  @Test
  public void testConstructor() {
    Module.Info info = new ModuleInfo("n", "v", "d", "in");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("a");
    new SourcePipe(new ArrayList<Info>(), new MetricRegistry(), info, source, output);
  }

}
