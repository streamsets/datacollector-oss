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

public abstract class DClusterSourceOffsetCommitter extends DSourceOffsetCommitter implements ClusterSource {
  private ClusterSource clusterSource;

  @Override
  Stage<Source.Context> createStage() {
    Stage<Source.Context> result = super.createStage();
    if (source instanceof ClusterSource) {
      clusterSource = (ClusterSource) source;
    }
    return result;
  }

  @Override
  public String getName() {
    if (clusterSource == null) {
      // Get actual source in case of source being a delegating source (DelegatingKafkaSource)
      if (getSource() instanceof ClusterSource) {
        clusterSource = (ClusterSource) getSource();
      } else {
        throw new RuntimeException(Utils.format(
          "The instance '{}' should not call this method as it does not implement '{}'", source.getClass().getName(),
          ClusterSource.class.getName()));
      }
    }
    return clusterSource.getName();
  }

  @Override
  public boolean isInBatchMode() {
    if (clusterSource == null) {
      // Get actual source in case of source being a delegating source (DelegatingKafkaSource)
      if (getSource() instanceof ClusterSource) {
        clusterSource = (ClusterSource) getSource();
      } else {
      throw new RuntimeException(Utils.format(
        "The instance '{}' should not call this method as it does not implement '{}'", source.getClass().getName(),
        ClusterSource.class.getName()));
      }
    }
    return clusterSource.isInBatchMode();
  }

  /**
   * Writes batch of data to the source
   * @param batch
   * @throws InterruptedException
   */
  @Override
  public void put(List<Map.Entry> batch) throws InterruptedException {
    if (clusterSource == null) {
      // Get actual source in case of source being a delegating source (DelegatingKafkaSource)
      if (getSource() instanceof ClusterSource) {
        clusterSource = (ClusterSource) getSource();
      } else {
      throw new RuntimeException(Utils.format(
        "The instance '{}' should not call this method as it does not implement '{}'", source.getClass().getName(),
        ClusterSource.class.getName()));
      }
    }
    clusterSource.put(batch);
  }

  /**
   * Return the no of records produced by this source
   * @return
   */
  @Override
  public long getRecordsProduced() {
    if (clusterSource == null) {
      // Get actual source in case of source being a delegating source (DelegatingKafkaSource)
      if (getSource() instanceof ClusterSource) {
        clusterSource = (ClusterSource) getSource();
      } else {
      throw new RuntimeException(Utils.format(
        "The instance '{}' should not call this method as it does not implement '{}'", source.getClass().getName(),
        ClusterSource.class.getName()));
      }
    }
    return clusterSource.getRecordsProduced();
  }

  /**
   * Return true if a unrecoverable error has occured
   * @return
   */
  @Override
  public boolean inErrorState() {
    if (clusterSource == null) {
      // Get actual source in case of source being a delegating source (DelegatingKafkaSource)
      if (getSource() instanceof ClusterSource) {
        clusterSource = (ClusterSource) getSource();
      } else {
      throw new RuntimeException(Utils.format(
        "The instance '{}' should not call this method as it does not implement '{}'", source.getClass().getName(),
        ClusterSource.class.getName()));
      }
    }
    return clusterSource.inErrorState();
  }

  @Override
  public Map<String, String> getConfigsToShip() {
    if (clusterSource == null) {
      // Get actual source in case of source being a delegating source (DelegatingKafkaSource)
      if (getSource() instanceof ClusterSource) {
        clusterSource = (ClusterSource) getSource();
      } else {
      throw new RuntimeException(Utils.format(
        "The instance '{}' should not call this method as it does not implement '{}'", source.getClass().getName(),
        ClusterSource.class.getName()));
      }
    }
    return clusterSource.getConfigsToShip();
  }

}
