/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.s3;

public class BadSpoolObjectException extends Exception {
  private final String object;
  private final String pos;

  public BadSpoolObjectException(String object, String pos, Exception ex) {
    super(ex);
    this.object = object;
    this.pos = pos;
  }

  public String getObject() {
    return object;
  }

  public String getPos() {
    return pos;
  }
}
