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
package com.streamsets.pipeline.lib.util;

import org.apache.avro.file.Codec;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.iq80.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Cloned from Avro's SnappyCodec, replaced Xerial Snappy (native code) with IQ80 Snappy (100% Java)
 */
public class AvroJavaSnappyCodec extends Codec {
  private CRC32 crc32 = new CRC32();

  static {
    CodecFactory.addCodec(DataFileConstants.SNAPPY_CODEC, new AvroJavaSnappyCodec.Option());
  }

  public static void initialize() {
  }

  static class Option extends CodecFactory {
    @Override
    protected Codec createInstance() {
      return new AvroJavaSnappyCodec();
    }
  }

  private AvroJavaSnappyCodec() {}

  @Override public String getName() { return DataFileConstants.SNAPPY_CODEC; }

  @Override
  public ByteBuffer compress(ByteBuffer in) throws IOException {
    ByteBuffer out =
        ByteBuffer.allocate(Snappy.maxCompressedLength(in.remaining())+4);
    int size = Snappy.compress(in.array(), in.position(), in.remaining(),
        out.array(), 0);
    crc32.reset();
    crc32.update(in.array(), in.position(), in.remaining());
    out.putInt(size, (int)crc32.getValue());

    out.limit(size+4);

    return out;
  }

  @Override
  public ByteBuffer decompress(ByteBuffer in) throws IOException {
    ByteBuffer out = ByteBuffer.allocate
        (Snappy.getUncompressedLength(in.array(),in.position()));
    int size = Snappy.uncompress(in.array(),in.position(),in.remaining()-4,
        out.array(), 0);
    out.limit(size);

    crc32.reset();
    crc32.update(out.array(), 0, size);
    if (in.getInt(in.limit()-4) != (int)crc32.getValue())
      throw new IOException("Checksum failure");

    return out;
  }

  @Override public int hashCode() { return getName().hashCode(); }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (getClass() != obj.getClass())
      return false;
    return true;
  }

}
