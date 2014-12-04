/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.clipper.stage;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.RawSourcePreviewer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

public class ClipperSourcePreviewer implements RawSourcePreviewer {

  private String mimeType;

  @ConfigDef(defaultValue = "", description = "The name of the file to preview", label = "File Name",
    required = true, type = ConfigDef.Type.STRING)
  public String fileName;

  @Override
  public Reader preview(int maxLength) {
    Reader reader = null;
    try {
      //TODO: make sure this field in injected at runtime
      reader = new FileReader(fileName);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    return reader;
  }

  @Override
  public String getMimeType() {
    return mimeType;
  }

  @Override
  public void setMimeType(String mimeType) {
    this.mimeType = mimeType;
  }
}
