/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.BatchMaker;
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
    if (getContext().isPreview() || !getContext().isClusterMode()) {
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
