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
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Stage.Info;
import com.streamsets.pipeline.api.Processor;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class TestProcessorPipe {

  @Test(expected = NullPointerException.class)
  public void testInvalidConstructor() {
    Stage.Info info = new ModuleInfo("n", "v", "d", "in");
    Set<String> input = new HashSet<String>();
    input.add("a");
    Set<String> output = new HashSet<String>();
    output.add("a");
    new ProcessorPipe(new ArrayList<Info>(), new MetricRegistry(), info, null, input, output);
  }

  @Test
  public void testConstructor() {
    Stage.Info info = new ModuleInfo("n", "v", "d", "in");
    Processor Processor = Mockito.mock(Processor.class);
    Set<String> input = new HashSet<String>();
    input.add("a");
    Set<String> output = new HashSet<String>();
    output.add("a");
    new ProcessorPipe(new ArrayList<Info>(), new MetricRegistry(), info, Processor, input, output);
  }

}
