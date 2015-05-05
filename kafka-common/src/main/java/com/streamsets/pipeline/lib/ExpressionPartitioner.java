/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class ExpressionPartitioner implements Partitioner {

  public ExpressionPartitioner(VerifiableProperties props) {

  }

  public int partition(Object key, int numPartitions) {
    return Integer.valueOf((String)key);
  }

}
