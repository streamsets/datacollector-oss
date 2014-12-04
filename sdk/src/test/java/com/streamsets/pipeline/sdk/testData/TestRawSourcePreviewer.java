/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.testData;

import com.streamsets.pipeline.api.RawSourcePreviewer;

import java.io.Reader;

public class TestRawSourcePreviewer {

  class FaultyRawSourcePreviewer implements RawSourcePreviewer {
    @Override
    public Reader preview(int maxLength) {
      return null;
    }

    @Override
    public String getMimeType() {
      return null;
    }

    @Override
    public void setMimeType(String mimeType) {

    }
  }
}
