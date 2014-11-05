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
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.container.Pipeline.Builder;
import org.junit.Test;

import java.util.Iterator;

public class TestEndToEnd {

  public static class TSource implements Source {
    private Context context;

    @Override
    public void init(Info info, Context context) {
      this.context = context;
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) {
      int count = (lastSourceOffset == null) ? 0 : Integer.parseInt(lastSourceOffset);
      for (int i = 0; i < 2; i++) {
        Record record = context.createRecord("id:" + count + ":" + i);
        record.setField("batch", Field.create(count));
        record.setField("idx", Field.create(i));
        record.setField("name", Field.create("" + count + ":" + i));
        batchMaker.addRecord(record, "lane");
      }
      return (count < 3) ? "" + (count + 1) : null;
    }

    @Override
    public void destroy() {

    }
  }

  public static class TProcessor implements Processor {
    private int counter;

    @Override
    public void init(Info info, Context context) {

    }

    @Override
    public void process(Batch batch, BatchMaker batchMaker) {
      Iterator<Record> it = batch.getRecords();
      while (it.hasNext()) {
        Record record = it.next();
        record.setField("idx", Field.create(100 + counter++));
        record.setField("p-added", Field.create(counter++));
        batchMaker.addRecord(record, "lane");
      }
    }

    @Override
    public void destroy() {

    }
  }

  public static class TTarget implements Target {

    @Override
    public void init(Info info, Target.Context context) {

    }

    @Override
    public void write(Batch batch) {
      System.out.println("Target processing batch: " + batch.getSourceOffset());
      Iterator<Record> it = batch.getRecords();
      while (it.hasNext()) {
        Record record = it.next();
        System.out.println(" Record: " + record);
      }
    }

    @Override
    public void destroy() {

    }
  }

  public static class TSourceTracker implements SourceTracker {
    private boolean finished;
    private String batchId;

    public boolean isFinished() {
      return finished;
    }

    @Override
    public String getLastBatchId() {
      return batchId;
    }

    @Override
    public void udpateLastBatchId(String batchId) {
      this.batchId = batchId;
      finished = batchId == null;
    }
  }

  private Pipeline createPipeline(boolean complete) {
    MetricRegistry metrics = new MetricRegistry();

    Source.Info sourceInfo = new ModuleInfo("s", "1", "S", "si");
    Source source = new TSource();
    Pipeline.Builder pb = new Builder(metrics, sourceInfo, source, ImmutableSet.of("lane"));

    Processor.Info processorInfo = new ModuleInfo("p", "1", "P", "pi");
    Processor processor = new TProcessor();
    pb.add(processorInfo, processor, ImmutableSet.of("lane"), ImmutableSet.of("lane"));

    if (complete) {
      Target.Info targetInfo = new ModuleInfo("t", "1", "T", "ti");
      Target target = new TTarget();
      pb.add(targetInfo, target, ImmutableSet.of("lane"));
    }

    return (complete) ? pb.build() : pb.buildPreview();
  }

  @Test
  public void testPreview() {
    Pipeline pipeline = createPipeline(true);

    pipeline.init();
    PipelineRunner pr = new PipelineRunner(pipeline, new TSourceTracker(), true);
    RunOutput po = pr.preview(null);
    while (po.getBatchId() != null) {
      po = pr.preview(po.getBatchId());
      System.out.println(po.getOutput());
    }
    pipeline.destroy();
  }

  @Test
  public void testPreviewIncomplete() {
    Pipeline pipeline = createPipeline(false);

    pipeline.init();
    PipelineRunner pr = new PipelineRunner(pipeline, new TSourceTracker(), true);
    RunOutput po = pr.preview(null);
    while (po.getBatchId() != null) {
      po = pr.preview(po.getBatchId());
      System.out.println(po.getOutput());
    }
    pipeline.destroy();
  }

  @Test
  public void testRun() {
    Pipeline pipeline = createPipeline(true);

    pipeline.init();
    PipelineRunner pr = new PipelineRunner(pipeline, new TSourceTracker(), false);
    pr.run();
    pipeline.destroy();
  }
}
