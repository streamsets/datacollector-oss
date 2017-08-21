/*
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
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

import java.util.List;

public class DelegatingKafkaSource extends BaseSource implements OffsetCommitter {
  private final KafkaSourceFactory standaloneKafkaSourceFactory;
  private final KafkaSourceFactory clusterKafkaSourceFactory;
  private BaseKafkaSource delegate;

  public DelegatingKafkaSource(KafkaSourceFactory standaloneKafkaSourceFactory,
                               KafkaSourceFactory clusterKafkaSourceFactory) {
    this.standaloneKafkaSourceFactory = standaloneKafkaSourceFactory;
    this.clusterKafkaSourceFactory = clusterKafkaSourceFactory;
  }

  @Override
  protected List<ConfigIssue> init() {
    if (getContext().isPreview()
      || !(getContext().getExecutionMode() == ExecutionMode.CLUSTER_BATCH
           || getContext().getExecutionMode() == ExecutionMode.CLUSTER_YARN_STREAMING
           || getContext().getExecutionMode() == ExecutionMode.CLUSTER_MESOS_STREAMING)) {
      delegate = standaloneKafkaSourceFactory.create();
    } else {
      delegate = clusterKafkaSourceFactory.create();
    }
    return delegate.init(getInfo(), getContext());
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    return delegate.produce(lastSourceOffset, maxBatchSize, batchMaker);
  }

  @Override
  public void commit(String offset) throws StageException {
    delegate.commit(offset);
  }

  @Override
  public void destroy() {
    delegate.destroy();
  }

  public BaseKafkaSource getSource() {
    return delegate;
  }
}
