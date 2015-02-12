/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.LocalizableString;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.HashMap;
import java.util.Map;

public class Issue {
  protected final String configGroup;
  protected final String configName;
  private final LocalizableString message;
  private Map<String, Object> additionalInfo;

  protected Issue(String configName, String configGroup, ErrorCode error, Object... args) {
    message = new ErrorMessage(error, args);
    this.configName = configName;
    this.configGroup = configGroup;
  }

  protected Issue(ErrorCode error, Object... args) {
    this(null, null, error, args);
  }

  protected Issue(ValidationError error, Object... args) {
    this((ErrorCode) error, args);
  }

  public void setAdditionalInfo(String key, Object value) {
    if (additionalInfo == null) {
      additionalInfo = new HashMap<>();
    }
    additionalInfo.put(key, value);
  }

  public Map getAdditionalInfo() {
    return additionalInfo;
  }

  public String getMessage() {
    return message.getLocalized();
  }

  public String getConfigGroup() {
    return configGroup;
  }

  public String getConfigName() {
    return configName;
  }

  public String toString() {
    return Utils.format("Issue[message='{}']", message.getNonLocalized());
  }

}
