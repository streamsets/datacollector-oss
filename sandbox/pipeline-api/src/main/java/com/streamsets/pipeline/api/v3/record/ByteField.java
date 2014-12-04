/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.record;

public class ByteField extends Field<Byte> {

  public ByteField(byte value) {
    super(Type.BYTE, value, true, null);
  }

  public ByteField(String value) {
    super(Type.BYTE, Byte.parseByte(value), true, value);
  }
}
