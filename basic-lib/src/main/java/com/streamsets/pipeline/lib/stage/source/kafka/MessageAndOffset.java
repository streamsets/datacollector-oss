/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

public class MessageAndOffset {

  private final byte[] payload;
  private final long offset;
  private final int partition;

  public MessageAndOffset(byte[] payload, long offset, int partition) {
    this.payload = payload;
    this.offset = offset;
    this.partition = partition;
  }

  public byte[] getPayload() {
    return payload;
  }

  public long getOffset() {
    return offset;
  }

  public int getPartition() {
    return partition;
  }
}
