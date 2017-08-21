/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;

@GenerateResourceBundle
public enum CompressionMode implements Label {
  NONE("None", null),
  GZIP("Gzip", GzipCodec.class),
  BZIP2("Bzip2", BZip2Codec.class),
  SNAPPY("Snappy", SnappyCodec.class),
  LZ4("LZ4", Lz4Codec.class),
  OTHER("Other...", null),

  ;

  private final String label;
  private final Class<? extends CompressionCodec> codec;

  CompressionMode(String label, Class<? extends CompressionCodec> codec) {
    this.label = label;
    this.codec = codec;
  }

  public  Class<? extends CompressionCodec> getCodec() {
    return codec;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
