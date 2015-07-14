/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.preview.common;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum PreviewError implements ErrorCode {

  PREVIEW_0001("No task is running"),
  PREVIEW_0002("Could not retrieve the preview output : {}"),
  PREVIEW_0003("Encountered error while previewing : {}")

  ;

  private final String msg;

  PreviewError(String msg) {
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
