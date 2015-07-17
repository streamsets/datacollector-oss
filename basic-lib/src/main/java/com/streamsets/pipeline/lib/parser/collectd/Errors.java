package com.streamsets.pipeline.lib.parser.collectd;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  COLLECTD_00("Corrupt message: '{}'"),
  COLLECTD_01("Unsupported Value type: {}"),
  COLLECTD_02("Signature verification failed. {}"),
  COLLECTD_03("Decryption failed: {}"),
  ;

  private final String msg;

  Errors(String msg) {
    this.msg = msg;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return msg;
  }
}
