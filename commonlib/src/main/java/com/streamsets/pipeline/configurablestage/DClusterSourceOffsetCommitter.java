/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.configurablestage;

import java.util.List;
import java.util.Map;

import com.streamsets.pipeline.api.impl.ClusterSource;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DClusterSourceOffsetCommitter extends DSourceOffsetCommitter implements ClusterSource {
  private static final Logger LOG = LoggerFactory.getLogger(DClusterSourceOffsetCommitter.class);
  private ClusterSource clusterSource;

  @Override
  Stage<Source.Context> createStage() {
    Stage<Source.Context> result = super.createStage();
    LOG.info("Created source of type: {}", source);
    if (source instanceof ClusterSource) {
      clusterSource = (ClusterSource) source;
    } else if (source == null) {
      throw new NullPointerException("Source cannot be null");
    }
    return result;
  }

  @Override
  public String getName() {
    initializeClusterSource();
    return clusterSource.getName();
  }

  @Override
  public boolean isInBatchMode() {
    initializeClusterSource();
    return clusterSource.isInBatchMode();
  }

  /**
   * Writes batch of data to the source
   * @param batch
   * @throws InterruptedException
   */
  @Override
  public void put(List<Map.Entry> batch) throws InterruptedException {
    initializeClusterSource();
    clusterSource.put(batch);
  }

  private void initializeClusterSource() {
    // TODO fix this hack and ensure initialization is synchronous
    long start = System.currentTimeMillis();
    while (clusterSource == null && ThreadUtil.sleep(1) && (System.currentTimeMillis() - start) < 60L * 1000L) {
      // Get actual source in case of source being a delegating source (DelegatingKafkaSource)
      Source source = getSource();
      if (source instanceof ClusterSource) {
        clusterSource = (ClusterSource) source;
      } else if (source != null) {
        throw new RuntimeException(Utils.format(
          "The instance '{}' should not call this method as it does not implement '{}'", source.getClass().getName(),
          ClusterSource.class.getName()));
      }
    }
    if (clusterSource == null) {
      throw new RuntimeException("Could not obtain cluster source");
    }
  }

  /**
   * Return the no of records produced by this source
   * @return
   */
  @Override
  public long getRecordsProduced() {
    initializeClusterSource();
    return clusterSource.getRecordsProduced();
  }

  /**
   * Return true if a unrecoverable error has occured
   * @return
   */
  @Override
  public boolean inErrorState() {
    initializeClusterSource();
    return clusterSource.inErrorState();
  }

  @Override
  public Map<String, String> getConfigsToShip() {
    initializeClusterSource();
    return clusterSource.getConfigsToShip();
  }

  @Override
  public void postDestroy() {
    initializeClusterSource();
    clusterSource.postDestroy();
  }

}
