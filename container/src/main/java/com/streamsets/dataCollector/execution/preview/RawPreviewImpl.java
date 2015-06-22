/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.preview;

import com.streamsets.dataCollector.execution.RawPreview;

import java.io.InputStream;

public class RawPreviewImpl implements RawPreview {

  private final InputStream inputStream;
  private final String mimeType;

  public RawPreviewImpl(InputStream inputStream, String mimeType) {
    this.inputStream = inputStream;
    this.mimeType = mimeType;
  }

  @Override
  public InputStream getData() {
    return inputStream;
  }

  @Override
  public String getMimeType() {
    return mimeType;
  }
}
