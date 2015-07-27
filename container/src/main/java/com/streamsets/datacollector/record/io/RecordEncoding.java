/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.record.io;

import com.streamsets.pipeline.api.impl.Utils;

import java.io.IOException;

class RecordEncodingConstants {

  //10100000
  static final byte BASE_MAGIC_NUMBER = (byte) 0xa0;
  //10100002
  static final byte KRYO1_MAGIC_NUMBER = BASE_MAGIC_NUMBER | (byte) 0x02;
  //10100001
  static final byte JSON1_MAGIC_NUMBER = BASE_MAGIC_NUMBER | (byte) 0x01;
}

public enum RecordEncoding {
  JSON1(RecordEncodingConstants.JSON1_MAGIC_NUMBER),
  KRYO1(RecordEncodingConstants.KRYO1_MAGIC_NUMBER),

  ;

  public static final RecordEncoding DEFAULT = JSON1;

  private final byte magicNumber;

  RecordEncoding(byte magicNumber) {
    this.magicNumber = magicNumber;
  }

  public byte getMagicNumber() {
    return magicNumber;
  }

  public static RecordEncoding getEncoding(String name) throws IOException {
    RecordEncoding encoding = DEFAULT;
    if (name != null) {
      try {
        encoding = valueOf(name);
      } catch (IllegalArgumentException ex) {
        throw new IOException(Utils.format("Unsupported encoding '{}'", encoding));
      }
    }
    return encoding;
  }

  public static RecordEncoding getEncoding(byte magicNumber) throws IOException {
    for (RecordEncoding encoding : values()) {
      if (encoding.getMagicNumber() == magicNumber) {
        return encoding;
      }
    }
    throw new IOException(String.format("Unsupported magic number '0x%X'", magicNumber));
  }

}
