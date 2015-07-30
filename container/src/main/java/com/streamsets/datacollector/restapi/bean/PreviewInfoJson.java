/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import com.streamsets.datacollector.execution.PreviewStatus;

public class PreviewInfoJson {
  private String previewerId;
  private PreviewStatus status;

  public PreviewInfoJson(String previewerId, PreviewStatus previewStatus) {
    this.previewerId = previewerId;
    this.status = previewStatus;
  }

  public String getPreviewerId() {
    return previewerId;
  }

  public void setPreviewerId(String previewerId) {
    this.previewerId = previewerId;
  }

  public PreviewStatus getStatus() {
    return status;
  }

  public void setStatus(PreviewStatus previewStatus) {
    this.status = previewStatus;
  }
}
