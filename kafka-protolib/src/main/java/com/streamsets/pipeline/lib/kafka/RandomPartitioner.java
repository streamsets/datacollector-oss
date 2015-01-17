/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import java.util.Random;

public class RandomPartitioner implements Partitioner {

  private Random random;

  public RandomPartitioner (VerifiableProperties props) {
    random = new Random();
  }

  public int partition(Object key, int numPartitions) {
    return random.nextInt(numPartitions);
  }

}