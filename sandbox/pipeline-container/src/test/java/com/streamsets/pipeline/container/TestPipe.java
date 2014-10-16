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

import com.streamsets.pipeline.config.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class TestPipe {

  public static class TPipe extends Pipe {
    public TPipe(String name, Set<String> inputLines, Set<String> outputLines) {
      super(name, inputLines, outputLines);
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
    new TPipe("name", null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testInvalidConstructor3() {
    new TPipe("name", new HashSet<String>(), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidConstructor4() {
    new TPipe(Pipe.INVALID_NAME, new HashSet<String>(), new HashSet<String>());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidConstructor5() {
    new TPipe(Pipe.INVALID_NAME, new HashSet<String>(), new HashSet<String>());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidConstructor6() {
    new TPipe("name", new HashSet<String>(), new HashSet<String>());
  }

  @Test
  public void testName() {
    Set<String> input = new HashSet<String>();
    input.add("i");
    Set<String> output = new HashSet<String>();
    Pipe pipe = new TPipe("name", input, output);
    Assert.assertEquals("name", pipe.getName());
  }

  @Test
  public void testWithInput() {
    Set<String> input = new HashSet<String>();
    input.add("i");
    Set<String> output = new HashSet<String>();
    Pipe pipe = new TPipe("name", input, output);
    Assert.assertEquals(input, pipe.getInputLanes());
    Assert.assertEquals(output, pipe.getOutputLanes());
    Assert.assertEquals(input, pipe.getConsumedLanes());
    Assert.assertTrue(pipe.getProducedLanes().isEmpty());
  }

  @Test
  public void testWithOutput() {
    Set<String> input = new HashSet<String>();
    Set<String> output = new HashSet<String>();
    output.add("o");
    Pipe pipe = new TPipe("name", input, output);
    Assert.assertEquals(input, pipe.getInputLanes());
    Assert.assertEquals(output, pipe.getOutputLanes());
    Assert.assertTrue(pipe.getConsumedLanes().isEmpty());
    Assert.assertEquals(output, pipe.getProducedLanes());
  }

  @Test
  public void testWithSameInputOutput() {
    Set<String> input = new HashSet<String>();
    input.add("l");
    Set<String> output = new HashSet<String>();
    output.add("l");
    Pipe pipe = new TPipe("name", input, output);
    Assert.assertEquals(input, pipe.getInputLanes());
    Assert.assertEquals(output, pipe.getOutputLanes());
    Assert.assertTrue(pipe.getProducedLanes().isEmpty());
    Assert.assertTrue(pipe.getConsumedLanes().isEmpty());
  }

  @Test
  public void testWithDiffInputOutput() {
    Set<String> input = new HashSet<String>();
    input.add("a");
    input.add("b");
    Set<String> output = new HashSet<String>();
    output.add("b");
    output.add("c");
    Pipe pipe = new TPipe("name", input, output);
    Assert.assertEquals(input, pipe.getInputLanes());
    Assert.assertEquals(output, pipe.getOutputLanes());
    Assert.assertEquals(1, pipe.getProducedLanes().size());
    Assert.assertTrue(pipe.getProducedLanes().contains("c"));
    Assert.assertEquals(1, pipe.getConsumedLanes().size());
    Assert.assertTrue(pipe.getConsumedLanes().contains("a"));
  }

}
