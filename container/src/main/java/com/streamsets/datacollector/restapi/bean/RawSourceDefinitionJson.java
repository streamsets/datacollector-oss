/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import java.util.List;

public class RawSourceDefinitionJson {

  private final com.streamsets.datacollector.config.RawSourceDefinition rawSourceDefinition;

  public RawSourceDefinitionJson(com.streamsets.datacollector.config.RawSourceDefinition rawSourceDefinition) {
    this.rawSourceDefinition = rawSourceDefinition;
  }

  public String getRawSourcePreviewerClass() {
    return rawSourceDefinition.getRawSourcePreviewerClass();
  }

  public List<ConfigDefinitionJson> getConfigDefinitions() {
    return BeanHelper.wrapConfigDefinitions(rawSourceDefinition.getConfigDefinitions());
  }

  public String getMimeType() {
    return rawSourceDefinition.getMimeType();
  }

}