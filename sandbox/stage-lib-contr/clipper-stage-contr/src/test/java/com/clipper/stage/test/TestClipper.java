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
package com.clipper.stage.test;

import com.clipper.stage.ConsoleTarget;
import com.clipper.stage.FareCalculatorProcessor;
import com.clipper.stage.PDFLineProducer;
import com.clipper.stage.TSourceTracker;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.container.ModuleInfo;
import com.streamsets.pipeline.container.Pipeline;
import com.streamsets.pipeline.container.PipelineRunner;
import org.junit.Test;

public class TestClipper {

  @Test
  public void testRun() {
    Pipeline pipeline = createPipeline(true);

    pipeline.init();
    PipelineRunner pr = new PipelineRunner(pipeline, new TSourceTracker(), false);
    pr.run();
    pipeline.destroy();
  }

  private Pipeline createPipeline(boolean complete) {
    MetricRegistry metrics = new MetricRegistry();

    Source.Info sourceInfo = new ModuleInfo(
      "PdfLineProducer",
      "1.0",
      "Produces records containing clipper transactions",
      "pdfProd");
    Source source = new PDFLineProducer("/Users/Harikiran/Documents/ridehistory.pdf");
    Pipeline.Builder pb = new Pipeline.Builder(metrics, sourceInfo, source, ImmutableSet.of("lane"));

    Processor.Info processorInfo = new ModuleInfo(
      "FareCalculatorProcessor",
      "1.0",
      "Produces 2 additional fields per transaction in the record - transactionFare and totalClaim",
      "fareCalcProc");
    Processor processor = new FareCalculatorProcessor();
    pb.add(processorInfo, processor, ImmutableSet.of("lane"), ImmutableSet.of("lane"));

    if (complete) {
      Target.Info targetInfo = new ModuleInfo(
        "ConsoleTarget",
        "1.0",
        "Produces transaction line, fare and totalClaim",
        "ct");
      Target target = new ConsoleTarget();
      pb.add(targetInfo, target, ImmutableSet.of("lane"));
    }

    return (complete) ? pb.build() : pb.buildPreview();
  }
}
