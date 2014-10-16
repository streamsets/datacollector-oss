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
package com.streamsets.pipeline.container;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.api.Module;
import com.streamsets.pipeline.api.Module.Info;
import com.streamsets.pipeline.config.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class TestPipe {

  public static class TPipe extends Pipe {

    public TPipe(Module.Info info, Set<String> inputLines, Set<String> outputLines) {
      super(new ArrayList<Info>(), new MetricRegistry(), info, inputLines, outputLines);
    }

    @Override
    public void init() {
    }

    @Override
    public void destroy() {
    }

    @Override
    public void configure(Configuration conf) {
    }

    @Override
    public void processBatch(PipeBatch batch) {
    }
  }

  @Test(expected = NullPointerException.class)
  public void testInvalidConstructor1() {
    new TPipe(null, null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testInvalidConstructor2() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "i");
    new TPipe(info, null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testInvalidConstructor3() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "i");
    new TPipe(info, new HashSet<String>(), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidConstructor6() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "i");
    new TPipe(info, new HashSet<String>(), new HashSet<String>());
  }

  @Test
  public void testName() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "i");
    Set<String> input = new HashSet<String>();
    input.add("i");
    Set<String> output = new HashSet<String>();
    Pipe pipe = new TPipe(info, input, output);
    Assert.assertEquals(info, pipe.getModuleInfo());
  }

  @Test
  public void testWithInput() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "i");
    Set<String> input = new HashSet<String>();
    input.add("i");
    Set<String> output = new HashSet<String>();
    Pipe pipe = new TPipe(info, input, output);
    Assert.assertEquals(input, pipe.getInputLanes());
    Assert.assertEquals(output, pipe.getOutputLanes());
    Assert.assertEquals(input, pipe.getConsumedLanes());
    Assert.assertTrue(pipe.getProducedLanes().isEmpty());
  }

  @Test
  public void testWithOutput() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "i");
    Set<String> input = new HashSet<String>();
    Set<String> output = new HashSet<String>();
    output.add("o");
    Pipe pipe = new TPipe(info, input, output);
    Assert.assertEquals(input, pipe.getInputLanes());
    Assert.assertEquals(output, pipe.getOutputLanes());
    Assert.assertTrue(pipe.getConsumedLanes().isEmpty());
    Assert.assertEquals(output, pipe.getProducedLanes());
  }

  @Test
  public void testWithSameInputOutput() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "i");
    Set<String> input = new HashSet<String>();
    input.add("l");
    Set<String> output = new HashSet<String>();
    output.add("l");
    Pipe pipe = new TPipe(info, input, output);
    Assert.assertEquals(input, pipe.getInputLanes());
    Assert.assertEquals(output, pipe.getOutputLanes());
    Assert.assertTrue(pipe.getProducedLanes().isEmpty());
    Assert.assertTrue(pipe.getConsumedLanes().isEmpty());
  }

  @Test
  public void testWithDiffInputOutput() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "i");
    Set<String> input = new HashSet<String>();
    input.add("a");
    input.add("b");
    Set<String> output = new HashSet<String>();
    output.add("b");
    output.add("c");
    Pipe pipe = new TPipe(info, input, output);
    Assert.assertEquals(input, pipe.getInputLanes());
    Assert.assertEquals(output, pipe.getOutputLanes());
    Assert.assertEquals(1, pipe.getProducedLanes().size());
    Assert.assertTrue(pipe.getProducedLanes().contains("c"));
    Assert.assertEquals(1, pipe.getConsumedLanes().size());
    Assert.assertTrue(pipe.getConsumedLanes().contains("a"));
  }

}
