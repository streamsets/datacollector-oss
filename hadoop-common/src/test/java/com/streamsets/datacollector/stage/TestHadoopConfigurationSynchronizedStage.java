/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.stage;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.impl.ClusterSource;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.mockito.Mockito.withSettings;

public class TestHadoopConfigurationSynchronizedStage {


  @Test
  public void testHadoopConfSynchronizedClusterSource() throws Exception {
    Source source = Mockito.mock(ClusterSource.class, withSettings().extraInterfaces(OffsetCommitter.class,
        ErrorListener.class));
    HadoopConfigurationSynchronizedClusterSource syncSource = new HadoopConfigurationSynchronizedClusterSource(source);
    syncSource.init(Mockito.any(Stage.Info.class), Mockito.any(Source.Context.class));
    Mockito.verify(source, Mockito.times(1)).init(Mockito.any(Stage.Info.class), Mockito.any(Source.Context.class));
    syncSource.produce(Mockito.anyString(), Mockito.anyInt(), Mockito.any(BatchMaker.class));
    Mockito.verify(source, Mockito.times(1)).produce(Mockito.any(String.class),
        Mockito.anyInt(),
        Mockito.any(BatchMaker.class)
    );
    syncSource.commit(Mockito.anyString());
    Mockito.verify((OffsetCommitter)source, Mockito.times(1)).commit(Mockito.anyString());
    syncSource.errorNotification(Mockito.any(Throwable.class));
    Mockito.verify((ErrorListener)source, Mockito.times(1)).errorNotification(Mockito.any(Throwable.class));

    syncSource.put(Mockito.any(List.class));
    Mockito.verify((ClusterSource)source, Mockito.times(1)).put(Mockito.any(List.class));

    syncSource.postDestroy();
    Mockito.verify((ClusterSource)source, Mockito.times(1)).postDestroy();

    syncSource.shutdown();
    Mockito.verify((ClusterSource)source, Mockito.times(1)).shutdown();

    syncSource.completeBatch();
    Mockito.verify((ClusterSource)source, Mockito.times(1)).completeBatch();

    syncSource.getConfigsToShip();
    Mockito.verify((ClusterSource)source, Mockito.times(1)).getConfigsToShip();

    syncSource.getName();
    Mockito.verify((ClusterSource)source, Mockito.times(1)).getName();

    syncSource.getParallelism();
    Mockito.verify((ClusterSource)source, Mockito.times(1)).getParallelism();

    syncSource.getRecordsProduced();
    Mockito.verify((ClusterSource)source, Mockito.times(1)).getRecordsProduced();

    syncSource.inErrorState();
    Mockito.verify((ClusterSource)source, Mockito.times(1)).inErrorState();

    syncSource.isInBatchMode();
    Mockito.verify((ClusterSource)source, Mockito.times(1)).isInBatchMode();
  }

  @Test
  public void testHadoopConfSynchronizedProcessor() throws Exception {
    Processor processor = Mockito.mock(Processor.class);
    Processor syncProcessor = new HadoopConfigurationSynchronizedProcessor(processor);
    syncProcessor.init(Mockito.any(Stage.Info.class), Mockito.any(Processor.Context.class));
    Mockito.verify(processor, Mockito.times(1)).init(Mockito.any(Stage.Info.class), Mockito.any(Processor.Context.class));
    syncProcessor.process(Mockito.any(Batch.class), Mockito.any(BatchMaker.class));
    Mockito.verify(processor, Mockito.times(1)).process(Mockito.any(Batch.class), Mockito.any(BatchMaker.class));
  }

  @Test
  public void testHadoopConfSynchronizedTarget() throws Exception {
    Target target = Mockito.mock(Target.class);
    Target syncTarget = new HadoopConfigurationSynchronizedTarget(target);
    syncTarget.init(Mockito.any(Stage.Info.class), Mockito.any(Target.Context.class));
    Mockito.verify(target, Mockito.times(1)).init(Mockito.any(Stage.Info.class), Mockito.any(Target.Context.class));
    syncTarget.write(Mockito.any(Batch.class));
    Mockito.verify(target, Mockito.times(1)).write(Mockito.any(Batch.class));
  }

  @Test
  public void testHadoopConfSynchronizedExecutor() throws Exception {
    Executor executor = Mockito.mock(Executor.class);
    Executor syncExecutor = new HadoopConfigurationSynchronizedExecutor(executor);
    syncExecutor.init(Mockito.any(Stage.Info.class), Mockito.any(Executor.Context.class));
    Mockito.verify(executor, Mockito.times(1)).init(Mockito.any(Stage.Info.class), Mockito.any(Executor.Context.class));
    syncExecutor.write(Mockito.any(Batch.class));
    Mockito.verify(executor, Mockito.times(1)).write(Mockito.any(Batch.class));
  }

}
