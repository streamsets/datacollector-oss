/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.cassandra;

import com.datastax.driver.core.ProtocolOptions;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum CassandraCompressionCodec implements Label {
  NONE("None",  ProtocolOptions.Compression.NONE),
  SNAPPY("Snappy", ProtocolOptions.Compression.SNAPPY),
  LZ4("LZ4", ProtocolOptions.Compression.LZ4),
  ;

  private final String label;
  private final ProtocolOptions.Compression codec;

  CassandraCompressionCodec(String label, ProtocolOptions.Compression codec) {
    this.label = label;
    this.codec = codec;
  }

  public ProtocolOptions.Compression getCodec() {
    return codec;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
