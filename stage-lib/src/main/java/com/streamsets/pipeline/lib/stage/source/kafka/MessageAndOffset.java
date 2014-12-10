/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

public class MessageAndOffset {

  private final byte[] payload;
  private final long offset;

  public MessageAndOffset(byte[] payload, long offset) {
    this.payload = payload;
    this.offset = offset;
  }

  public byte[] getPayload() {
    return payload;
  }

  public long getOffset() {
    return offset;
  }
}
