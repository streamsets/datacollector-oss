/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.preview.common;

import com.streamsets.datacollector.execution.RawPreview;

public class RawPreviewImpl implements RawPreview {

  private final String previewData;
  private final String mimeType;

  public RawPreviewImpl(String previewData, String mimeType) {
    this.previewData = previewData;
    this.mimeType = mimeType;
  }

  @Override
  public String getPreviewData() {
    return previewData;
  }

  @Override
  public String getMimeType() {
    return mimeType;
  }
}
