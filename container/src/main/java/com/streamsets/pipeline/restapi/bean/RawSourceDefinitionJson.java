/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import java.util.List;

public class RawSourceDefinitionJson {

  private final com.streamsets.pipeline.config.RawSourceDefinition rawSourceDefinition;

  public RawSourceDefinitionJson(com.streamsets.pipeline.config.RawSourceDefinition rawSourceDefinition) {
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