/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.record;

import org.apache.commons.codec.binary.Base64;

public class BinaryField extends Field<byte[]> {

  public BinaryField(byte[] value) {
    super(Type.BINARY, value, true, null);
  }

  public BinaryField(String value) {
    super(Type.BINARY, Base64.decodeBase64(value), true, value); //TODO handle errors
  }

}
