/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.validation;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.LocalizableString;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.HashMap;
import java.util.Map;

public class Issue {
  private final String instanceName;
  private final String configGroup;
  private final String configName;
  private final LocalizableString message;
  private Map<String, Object> additionalInfo;

  protected Issue(String instanceName, String configGroup, String configName, ErrorCode error, Object... args) {
    this.instanceName = instanceName;
    this.configGroup = configGroup;
    this.configName = configName;
    message = new ErrorMessage(error, args);
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

  public String getErrorCode() {
    return ((ErrorMessage)message).getErrorCode();
  }

  public String getInstanceName() {
    return instanceName;
  }

  public String getLevel() {
    String level;
    if (instanceName == null) {
      level = (getConfigName() == null) ? "PIPELINE" : "PIPELINE_CONFIG";
    } else {
      level = (getConfigName() == null) ? "STAGE" : "STAGE_CONFIG";
    }
    return level;
  }

  public String getConfigGroup() {
    return configGroup;
  }

  public String getConfigName() {
    return configName;
  }

  @Override
  public String toString() {
    return Utils.format("Issue[instance='{}' group='{}' config='{}' message='{}']", instanceName, configGroup,
                        configName, message.getNonLocalized());
  }

}
