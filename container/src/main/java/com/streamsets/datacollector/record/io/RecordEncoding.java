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

  private RecordEncodingConstants() {}
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
