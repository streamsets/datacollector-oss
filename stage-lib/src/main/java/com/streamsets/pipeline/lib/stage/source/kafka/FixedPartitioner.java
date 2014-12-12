/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class FixedPartitioner implements Partitioner {

  public FixedPartitioner(VerifiableProperties props) {

  }

  public int partition(Object key, int numPartitions) {
    return Integer.valueOf((String)key);
  }

}
