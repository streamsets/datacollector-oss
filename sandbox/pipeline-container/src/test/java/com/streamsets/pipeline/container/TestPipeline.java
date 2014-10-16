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

import com.streamsets.pipeline.api.Module;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.Configuration;
import com.streamsets.pipeline.container.Pipeline.Builder;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Set;

public class TestPipeline {

  @Test(expected = NullPointerException.class)
  public void testBuilderConstructorInvalid1() {
    new Builder(null, null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testBuilderConstructorInvalid2() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("name");
    new Builder(info, null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testBuilderConstructorInvalid3() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("name");
    Source source = Mockito.mock(Source.class);
    new Builder(info, source, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderConstructorInvalid4() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("name");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    new Builder(info, source, output);
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderInvalidPipelineOnlySource() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("name");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Builder builder = new Builder(info, source, output);

    builder.validate();
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderInvalidPipelinePipesWithSameName() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("name");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Builder builder = new Builder(info, source, output);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("name");
    Target target = Mockito.mock(Target.class);
    Set<String> input = new HashSet<String>();
    input.add("o");
    builder.add(info, target, input);

    builder.validate();
  }

  @Test
  public void testBuilderValidMinimalPipeline() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Builder builder = new Builder(info, source, output);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("target");
    Target target = Mockito.mock(Target.class);
    Set<String> input = new HashSet<String>();
    input.add("o");
    builder.add(info, target, input);

    builder.validate();
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderInvalidPipelineUnconsumedInput() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o1");
    output.add("o2");
    Builder builder = new Builder(info, source, output);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("target");
    Target target = Mockito.mock(Target.class);
    Set<String> input = new HashSet<String>();
    input.add("o1");
    builder.add(info, target, input);

    builder.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderInvalidPipelineTargetWithNoInput() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Builder builder = new Builder(info, source, output);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("target");
    Target target = Mockito.mock(Target.class);
    Set<String> input = new HashSet<String>();
    builder.add(info, target, input);

    builder.validate();
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderInvalidPipelineUnknownInput() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o1");
    Builder builder = new Builder(info, source, output);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("target");
    Target target = Mockito.mock(Target.class);
    Set<String> input = new HashSet<String>();
    input.add("o2");
    builder.add(info, target, input);

    builder.validate();
  }

  @Test
  public void testBuilderValidPipelineMultipleTargets() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o1");
    output.add("o2");
    Builder builder = new Builder(info, source, output);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("target1");
    Target target = Mockito.mock(Target.class);
    Set<String> input = new HashSet<String>();
    input.add("o1");
    builder.add(info, target, input);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("target2");
    target = Mockito.mock(Target.class);
    input = new HashSet<String>();
    input.add("o2");
    builder.add(info, target, input);

    builder.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderInvalidPipelineProcessorWithNoInput() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("name");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Builder builder = new Builder(info, source, output);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("processor");
    Processor processor = Mockito.mock(Processor.class);
    Set<String> input = new HashSet<String>();
    output = new HashSet<String>();
    output.add("p1");
    output.add("p2");
    builder.add(info, processor, input, output);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderInvalidPipelineProcessorWithNoOutput() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("name");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Builder builder = new Builder(info, source, output);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("processor");
    Processor processor = Mockito.mock(Processor.class);
    Set<String> input = new HashSet<String>();
    input.add("o");
    output = new HashSet<String>();
    builder.add(info, processor, input, output);
  }

  @Test
  public void testBuilderValidPipelineProcessorMultiOutput() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("s");
    Builder builder = new Builder(info, source, output);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("processor");
    Processor processor = Mockito.mock(Processor.class);
    Set<String> input = new HashSet<String>();
    input.add("s");
    output = new HashSet<String>();
    output.add("p1");
    output.add("p2");
    builder.add(info, processor, input, output);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("target1");
    Target target = Mockito.mock(Target.class);
    input = new HashSet<String>();
    input.add("p1");
    builder.add(info, target, input);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("target2");
    target = Mockito.mock(Target.class);
    input = new HashSet<String>();
    input.add("p2");
    builder.add(info, target, input);

    builder.validate();
  }

  @Test
  public void testBuilderBuild() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Builder builder = new Builder(info, source, output);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("target");
    Target target = Mockito.mock(Target.class);
    Set<String> input = new HashSet<String>();
    input.add("o");
    builder.add(info, target, input);

    builder.build();
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderDoubleBuild() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Builder builder = new Builder(info, source, output);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("target");
    Target target = Mockito.mock(Target.class);
    Set<String> input = new HashSet<String>();
    input.add("o");
    builder.add(info, target, input);

    builder.build();
    builder.build();
  }

  @Test
  public void testBuilderWithObserverWithoutConfiguration() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Observer sourceObserver = Mockito.mock(Observer.class);
    Builder builder = new Builder(info, source, output, sourceObserver);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("processor");
    Processor processor = Mockito.mock(Processor.class);
    Set<String> input = new HashSet<String>();
    input.add("o");
    output = new HashSet<String>();
    output.add("o");
    Observer processorObserver = Mockito.mock(Observer.class);
    builder.add(info, processor, input, output, processorObserver);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("target");
    Target target = Mockito.mock(Target.class);
    input = new HashSet<String>();
    input.add("o");
    builder.add(info, target, input);

    builder.build();

    Mockito.verify(sourceObserver, Mockito.times(0)).configure(Mockito.any(Configuration.class));
    Mockito.verify(processorObserver, Mockito.times(0)).configure(Mockito.any(Configuration.class));
  }

  @Test
  public void testBuilderWithObserverWithConfiguration() {

    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Observer sourceObserver = Mockito.mock(Observer.class);
    Builder builder = new Builder(info, source, output, sourceObserver);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("processor");
    Processor processor = Mockito.mock(Processor.class);
    Set<String> input = new HashSet<String>();
    input.add("o");
    output = new HashSet<String>();
    output.add("o");
    Observer processorObserver = Mockito.mock(Observer.class);
    builder.add(info, processor, input, output, processorObserver);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("target");
    Target target = Mockito.mock(Target.class);
    input = new HashSet<String>();
    input.add("o");
    builder.add(info, target, input);

    builder.setConfiguration(new Configuration());

    builder.build();

    Mockito.verify(sourceObserver, Mockito.times(1)).configure(Mockito.any(Configuration.class));
    Mockito.verify(processorObserver, Mockito.times(1)).configure(Mockito.any(Configuration.class));
  }

  @Test
  public void testRunBatch() {

    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Observer sourceObserver = Mockito.mock(Observer.class);
    Builder builder = new Builder(info, source, output, sourceObserver);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("processor");
    Processor processor = Mockito.mock(Processor.class);
    Set<String> input = new HashSet<String>();
    input.add("o");
    output = new HashSet<String>();
    output.add("o");
    Observer processorObserver = Mockito.mock(Observer.class);
    builder.add(info, processor, input, output, processorObserver);

    info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("target");
    Target target = Mockito.mock(Target.class);
    input = new HashSet<String>();
    input.add("o");
    builder.add(info, target, input);

    Pipeline pipeline = builder.build();

    PipelineBatch batch = new PipelineBatch("1");
    pipeline.runBatch(batch);

  }

}
