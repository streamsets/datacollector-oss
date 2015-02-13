/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.RawSourcePreviewer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class FileRawSourcePreviewer implements RawSourcePreviewer {

  private String mimeType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "File Path",
      description = "The full path of the file"
  )
  public String fileName;

  @Override
  public InputStream preview(int maxLength) {
    InputStream in;
    try {
      in = new FileInputStream(fileName);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    return in;
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
