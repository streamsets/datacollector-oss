/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Module;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.Configuration;
import com.streamsets.pipeline.container.Pipeline.Builder;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Set;

public class TestPipeline {

  @Test(expected = NullPointerException.class)
  public void testBuilderConstructorInvalid1() {
    new Builder(null, null, null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testBuilderConstructorInvalid2() {
    new Builder(new MetricRegistry(), null, null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testBuilderConstructorInvalid3() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "i");
    new Builder(new MetricRegistry(), info, null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testBuilderConstructorInvalid4() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "i");
    Source source = Mockito.mock(Source.class);
    new Builder(new MetricRegistry(), info, source, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderConstructorInvalid5() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstanceName()).thenReturn("name");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    new Builder(new MetricRegistry(), info, source, output);
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderInvalidPipelineOnlySource() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "i");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Builder builder = new Builder(new MetricRegistry(), info, source, output);

    builder.validate();
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderInvalidPipelinePipesWithSameName() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "i");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Builder builder = new Builder(new MetricRegistry(), info, source, output);

    Target target = Mockito.mock(Target.class);
    Set<String> input = new HashSet<String>();
    input.add("o");
    builder.add(info, target, input);

    builder.validate();
  }

  @Test
  public void testBuilderValidMinimalPipeline() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Builder builder = new Builder(new MetricRegistry(), info, source, output);

    info = new ModuleInfo("m", "1", "d", "target");
    Target target = Mockito.mock(Target.class);
    Set<String> input = new HashSet<String>();
    input.add("o");
    builder.add(info, target, input);

    builder.validate();
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderInvalidPipelineUnconsumedInput() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o1");
    output.add("o2");
    Builder builder = new Builder(new MetricRegistry(), info, source, output);

    info = new ModuleInfo("m", "1", "d", "target");
    Target target = Mockito.mock(Target.class);
    Set<String> input = new HashSet<String>();
    input.add("o1");
    builder.add(info, target, input);

    builder.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderInvalidPipelineTargetWithNoInput() {
    Module.Info info = new ModuleInfo("m", "1", "d", "source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Builder builder = new Builder(new MetricRegistry(), info, source, output);

    info = new ModuleInfo("m", "1", "d", "target");
    Target target = Mockito.mock(Target.class);
    Set<String> input = new HashSet<String>();
    builder.add(info, target, input);

    builder.validate();
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderInvalidPipelineUnknownInput() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o1");
    Builder builder = new Builder(new MetricRegistry(), info, source, output);

    info = new ModuleInfo("m", "1", "d", "target");
    Target target = Mockito.mock(Target.class);
    Set<String> input = new HashSet<String>();
    input.add("o2");
    builder.add(info, target, input);

    builder.validate();
  }

  @Test
  public void testBuilderValidPipelineMultipleTargets() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o1");
    output.add("o2");
    Builder builder = new Builder(new MetricRegistry(), info, source, output);

    info = new ModuleInfo("m", "1", "d", "target1");
    Target target = Mockito.mock(Target.class);
    Set<String> input = new HashSet<String>();
    input.add("o1");
    builder.add(info, target, input);

    info = new ModuleInfo("m", "1", "d", "target2");
    target = Mockito.mock(Target.class);
    input = new HashSet<String>();
    input.add("o2");
    builder.add(info, target, input);

    builder.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderInvalidPipelineProcessorWithNoInput() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Builder builder = new Builder(new MetricRegistry(), info, source, output);

    info = new ModuleInfo("m", "1", "d", "processor");
    Processor processor = Mockito.mock(Processor.class);
    Set<String> input = new HashSet<String>();
    output = new HashSet<String>();
    output.add("p1");
    output.add("p2");
    builder.add(info, processor, input, output);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderInvalidPipelineProcessorWithNoOutput() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Builder builder = new Builder(new MetricRegistry(), info, source, output);

    info = new ModuleInfo("m", "1", "d", "processor");
    Processor processor = Mockito.mock(Processor.class);
    Set<String> input = new HashSet<String>();
    input.add("o");
    output = new HashSet<String>();
    builder.add(info, processor, input, output);
  }

  @Test
  public void testBuilderValidPipelineProcessorMultiOutput() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("s");
    Builder builder = new Builder(new MetricRegistry(), info, source, output);

    info = new ModuleInfo("m", "1", "d", "processor");
    Processor processor = Mockito.mock(Processor.class);
    Set<String> input = new HashSet<String>();
    input.add("s");
    output = new HashSet<String>();
    output.add("p1");
    output.add("p2");
    builder.add(info, processor, input, output);

    info = new ModuleInfo("m", "1", "d", "target1");
    Target target = Mockito.mock(Target.class);
    input = new HashSet<String>();
    input.add("p1");
    builder.add(info, target, input);

    info = new ModuleInfo("m", "1", "d", "target2");
    target = Mockito.mock(Target.class);
    input = new HashSet<String>();
    input.add("p2");
    builder.add(info, target, input);

    builder.validate();
  }

  @Test
  public void testBuilderBuild() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Builder builder = new Builder(new MetricRegistry(), info, source, output);

    info = new ModuleInfo("m", "1", "d", "target");
    Target target = Mockito.mock(Target.class);
    Set<String> input = new HashSet<String>();
    input.add("o");
    builder.add(info, target, input);

    builder.build();
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderDoubleBuild() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Builder builder = new Builder(new MetricRegistry(), info, source, output);

    info = new ModuleInfo("m", "1", "d", "target");
    Target target = Mockito.mock(Target.class);
    Set<String> input = new HashSet<String>();
    input.add("o");
    builder.add(info, target, input);

    builder.build();
    builder.build();
  }

  @Test
  public void testPipelineLifeCycle() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "source");
    Observer sourceObserver = Mockito.mock(Observer.class);
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Builder builder = new Builder(new MetricRegistry(), info, source, output, sourceObserver);

    info = new ModuleInfo("m", "1", "d", "processor");
    Observer processorObserver = Mockito.mock(Observer.class);
    Processor processor = Mockito.mock(Processor.class);
    Set<String> input = new HashSet<String>();
    input.add("o");
    output = new HashSet<String>();
    output.add("o");
    builder.add(info, processor, input, output, processorObserver);

    info = new ModuleInfo("m", "1", "d", "target");
    Target target = Mockito.mock(Target.class);
    input = new HashSet<String>();
    input.add("o");
    builder.add(info, target, input);

    Pipeline pipeline = builder.build();

    try {
      pipeline.configure(new Configuration());
      Assert.fail();
    } catch (IllegalStateException ex) {
      //NOP
    }
    try {
      pipeline.destroy();
      Assert.fail();
    } catch (IllegalStateException ex) {
      //NOP
    }
    try {
      pipeline.runBatch(new PipelineBatch("1"));
      Assert.fail();
    } catch (IllegalStateException ex) {
      //NOP
    }

    Mockito.verifyZeroInteractions(source);
    Mockito.verifyZeroInteractions(sourceObserver);
    Mockito.verifyZeroInteractions(processor);
    Mockito.verifyZeroInteractions(processorObserver);
    Mockito.verifyZeroInteractions(target);
    pipeline.init();
   // Mockito.verify(source).init(Mockito.any(Module.Info.class), Mockito.any(Source.Context.class));
    Mockito.verify(sourceObserver).init();
  //  Mockito.verify(processor).init(Mockito.any(Module.Info.class), Mockito.any(Processor.Context.class));
    Mockito.verify(processorObserver).init();
   // Mockito.verify(target).init(Mockito.any(Module.Info.class), Mockito.any(Target.Context.class));
    Mockito.verifyNoMoreInteractions(source);
    Mockito.verifyNoMoreInteractions(sourceObserver);
    Mockito.verifyNoMoreInteractions(processor);
    Mockito.verifyNoMoreInteractions(processorObserver);
    Mockito.verifyNoMoreInteractions(target);

    try {
      pipeline.init();
      Assert.fail();
    } catch (IllegalStateException ex) {
      //NOP
    }
    pipeline.configure(new Configuration());
    Mockito.verify(sourceObserver).configure(Mockito.any(Configuration.class));
    Mockito.verify(processorObserver).configure(Mockito.any(Configuration.class));
    Mockito.verifyNoMoreInteractions(source);
    Mockito.verifyNoMoreInteractions(sourceObserver);
    Mockito.verifyNoMoreInteractions(processor);
    Mockito.verifyNoMoreInteractions(processorObserver);
    Mockito.verifyNoMoreInteractions(target);

    pipeline.runBatch(new PipelineBatch("1"));
   // Mockito.verify(source).produce(Mockito.anyString(), Mockito.any(BatchMaker.class));
    Mockito.verify(sourceObserver).isActive();
  //  Mockito.verify(processor).process(Mockito.any(Batch.class), Mockito.any(BatchMaker.class));
    Mockito.verify(processorObserver).isActive();
  //  Mockito.verify(target).write(Mockito.any(Batch.class));
    Mockito.verifyNoMoreInteractions(source);
    Mockito.verifyNoMoreInteractions(sourceObserver);
    Mockito.verifyNoMoreInteractions(processor);
    Mockito.verifyNoMoreInteractions(processorObserver);
    Mockito.verifyNoMoreInteractions(target);

    Mockito.reset(sourceObserver);
    Mockito.reset(processorObserver);
    Mockito.when(sourceObserver.isActive()).thenReturn(true);
    Mockito.when(processorObserver.isActive()).thenReturn(true);
    pipeline.runBatch(new PipelineBatch("1"));
    Mockito.verify(sourceObserver).isActive();
    Mockito.verify(sourceObserver).observe(Mockito.any(Batch.class));
    Mockito.verify(processorObserver).isActive();
    Mockito.verify(processorObserver).observe(Mockito.any(Batch.class));

    pipeline.configure(new Configuration());
    pipeline.runBatch(new PipelineBatch("1"));

    Mockito.reset(source);
    Mockito.reset(sourceObserver);
    Mockito.reset(processor);
    Mockito.reset(processorObserver);
    Mockito.reset(target);
    pipeline.destroy();
    Mockito.verify(source).destroy();
    Mockito.verify(sourceObserver).destroy();
    Mockito.verify(processor).destroy();
    Mockito.verify(processorObserver).destroy();
    Mockito.verify(target).destroy();
    Mockito.verifyNoMoreInteractions(source);
    Mockito.verifyNoMoreInteractions(sourceObserver);
    Mockito.verifyNoMoreInteractions(processor);
    Mockito.verifyNoMoreInteractions(processorObserver);
    Mockito.verifyNoMoreInteractions(target);

    pipeline.destroy();
    try {
      pipeline.configure(new Configuration());
      Assert.fail();
    } catch (IllegalStateException ex) {
      //NOP
    }
    try {
      pipeline.runBatch(new PipelineBatch("1"));
      Assert.fail();
    } catch (IllegalStateException ex) {
      //NOP
    }
    try {
      pipeline.init();
      Assert.fail();
    } catch (IllegalStateException ex) {
      //NOP
    }
  }


  @Test
  public void testBuilderWithObserverWithoutConfiguration() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Observer sourceObserver = Mockito.mock(Observer.class);
    Builder builder = new Builder(new MetricRegistry(), info, source, output, sourceObserver);

    info = new ModuleInfo("m", "1", "d", "processor");
    Processor processor = Mockito.mock(Processor.class);
    Set<String> input = new HashSet<String>();
    input.add("o");
    output = new HashSet<String>();
    output.add("o");
    Observer processorObserver = Mockito.mock(Observer.class);
    builder.add(info, processor, input, output, processorObserver);

    info = new ModuleInfo("m", "1", "d", "target");
    Target target = Mockito.mock(Target.class);
    input = new HashSet<String>();
    input.add("o");
    builder.add(info, target, input);

    Pipeline pipeline = builder.build();

    pipeline.init();

    Mockito.verify(sourceObserver, Mockito.times(0)).configure(Mockito.any(Configuration.class));
    Mockito.verify(processorObserver, Mockito.times(0)).configure(Mockito.any(Configuration.class));
  }

  @Test
  public void testBuilderWithObserverWithConfiguration() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Observer sourceObserver = Mockito.mock(Observer.class);
    Builder builder = new Builder(new MetricRegistry(), info, source, output, sourceObserver);

    info = new ModuleInfo("m", "1", "d", "processor");
    Processor processor = Mockito.mock(Processor.class);
    Set<String> input = new HashSet<String>();
    input.add("o");
    output = new HashSet<String>();
    output.add("o");
    Observer processorObserver = Mockito.mock(Observer.class);
    builder.add(info, processor, input, output, processorObserver);

    info = new ModuleInfo("m", "1", "d", "target");
    Target target = Mockito.mock(Target.class);
    input = new HashSet<String>();
    input.add("o");
    builder.add(info, target, input);

    Pipeline pipeline = builder.build();

    pipeline.init();
    pipeline.configure(new Configuration());

    Mockito.verify(sourceObserver, Mockito.times(1)).configure(Mockito.any(Configuration.class));
    Mockito.verify(processorObserver, Mockito.times(1)).configure(Mockito.any(Configuration.class));
  }

  @Test
  public void testRunBatch() {
    ModuleInfo info = new ModuleInfo("m", "1", "d", "source");
    Source source = Mockito.mock(Source.class);
    Set<String> output = new HashSet<String>();
    output.add("o");
    Observer sourceObserver = Mockito.mock(Observer.class);
    Builder builder = new Builder(new MetricRegistry(), info, source, output, sourceObserver);

    info = new ModuleInfo("m", "1", "d", "processor");
    Processor processor = Mockito.mock(Processor.class);
    Set<String> input = new HashSet<String>();
    input.add("o");
    output = new HashSet<String>();
    output.add("o");
    Observer processorObserver = Mockito.mock(Observer.class);
    builder.add(info, processor, input, output, processorObserver);

    info = new ModuleInfo("m", "1", "d", "target");
    Target target = Mockito.mock(Target.class);
    input = new HashSet<String>();
    input.add("o");
    builder.add(info, target, input);

    Pipeline pipeline = builder.build();

    pipeline.init();

    PipelineBatch batch = new PipelineBatch("1");
    pipeline.runBatch(batch);

  }

}
