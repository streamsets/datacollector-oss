/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class RoundRobinPartitioner implements Partitioner {

  private int lastPartition = 0;

  public RoundRobinPartitioner(VerifiableProperties props) {

  }

  public int partition(Object key, int numPartitions) {
    lastPartition = (lastPartition + 1) % numPartitions;
    return lastPartition;
  }

}