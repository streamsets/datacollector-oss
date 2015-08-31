/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor.testData;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.RawSourcePreviewer;

import java.io.InputStream;
import java.util.List;

public class TwitterRawSourcePreviewer implements RawSourcePreviewer{

  @FieldSelectorModel
  @ConfigDef(
      defaultValue = "admin",
      label = "username",
      required = true,
      description = "The user name of the twitter user",
      type = ConfigDef.Type.MODEL
  )
  public List<String> username;

  @ConfigDef(
      defaultValue = "admin",
      label = "password",
      required = true,
      description = "The password the twitter user",
      type = ConfigDef.Type.STRING
  )
  public String password;

  @Override
  public InputStream preview(int maxLength) {
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
