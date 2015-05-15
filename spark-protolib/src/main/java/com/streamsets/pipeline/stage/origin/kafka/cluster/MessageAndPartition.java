/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka.cluster;

// If we have the offset, reuse com.streamsets.pipeline.stage.origin.kafka.MessageOffset
/**
 * Wrapper around the message received from kafka dstream
 *
 */
class MessageAndPartition {

  private byte[] payload;
  private byte[] partition;

  public MessageAndPartition(byte[] payload, byte[] partition) {
    this.payload = payload;
    this.partition = partition;
  }

  public byte[] getPayload()  {
    return payload;
  }

  public byte[] getPartition() {
    return partition;
  }
}
