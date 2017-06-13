/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.stage;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ClusterSource;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HadoopConfigurationSynchronizedClusterSource extends HadoopConfigurationSynchronizedStage<Source.Context>
    implements ClusterSource, OffsetCommitter, ErrorListener {

  public HadoopConfigurationSynchronizedClusterSource(Source source) {
    super(source);
    Utils.checkState(
        source instanceof OffsetCommitter,
        Utils.format("Stage '{}' does not implement '{}'", source.getClass().getName(), OffsetCommitter.class.getName())
    );
    Utils.checkState(
        source instanceof ErrorListener,
        Utils.format("Stage '{}' does not implement '{}'", source.getClass().getName(), ErrorListener.class.getName())
    );
    Utils.checkState(
        source instanceof ClusterSource,
        Utils.format("Stage '{}' does not implement '{}'", source.getClass().getName(), ClusterSource.class.getName())
    );
  }

  @Override
  public String produce(String s, int i, BatchMaker batchMaker) throws StageException {
    return ((Source) stage).produce(s, i, batchMaker);
  }

  @Override
  public void errorNotification(Throwable throwable) {
    ((ErrorListener) stage).errorNotification(throwable);
  }

  @Override
  public void commit(String s) throws StageException {
    ((OffsetCommitter) stage).commit(s);
  }

  @Override
  public Object put(List<Map.Entry> list) throws InterruptedException {
    return ((ClusterSource) stage).put(list);
  }

  @Override
  public void completeBatch() throws InterruptedException {
    ((ClusterSource) stage).completeBatch();
  }

  @Override
  public long getRecordsProduced() {
    return ((ClusterSource) stage).getRecordsProduced();
  }

  @Override
  public boolean inErrorState() {
    return ((ClusterSource) stage).inErrorState();
  }

  @Override
  public String getName() {
    return ((ClusterSource) stage).getName();
  }

  @Override
  public boolean isInBatchMode() {
    return ((ClusterSource) stage).isInBatchMode();
  }

  @Override
  public Map<String, String> getConfigsToShip() {
    return ((ClusterSource) stage).getConfigsToShip();
  }

  @Override
  public int getParallelism() throws IOException, StageException {
    return ((ClusterSource) stage).getParallelism();
  }

  @Override
  public void shutdown() {
    ((ClusterSource) stage).shutdown();
  }

  @Override
  public void postDestroy() {
    ((ClusterSource) stage).postDestroy();
  }
}
