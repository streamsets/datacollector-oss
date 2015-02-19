/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldhasher;

public enum HashType {
  MD5("MD5"),
  SHA1("SHA-1"),
  SHA2("SHA-256");

  private String digest;

  private HashType(String digest) {
    this.digest = digest;
  }

  public String getDigest() {
    return digest;
  }
}
