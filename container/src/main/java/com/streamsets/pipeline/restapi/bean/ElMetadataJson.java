/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.el.ElMetadata;

import java.util.Map;

public class ElMetadataJson {

  private final ElMetadata elMetadata;

  public ElMetadataJson(ElMetadata elMetadata) {
    Utils.checkNotNull(elMetadata, "elMetadata");
    this.elMetadata = elMetadata;
  }

  public ElMetadataJson(Map<String, ElFunctionDefinitionJson> elFunctionDefinitions,
                    Map<String, ElConstantDefinitionJson> elConstantDefinitions, Map<String, String> elGroupDefinitions) {
    this.elMetadata = new ElMetadata(
      BeanHelper.unwrapElFunctionDefinitionsMap(elFunctionDefinitions),
      BeanHelper.unwrapElConstantDefinitionsMap(elConstantDefinitions),
      elGroupDefinitions);
  }

  public Map<String, String> getElGroupDefinitions() {
    return elMetadata.getElGroupDefinitions();
  }

  public Map<String, ElFunctionDefinitionJson> getElFunctionDefinitions() {
    return BeanHelper.wrapElFunctionDefinitionsMap(elMetadata.getElFunctionDefinitions());
  }

  public Map<String, ElConstantDefinitionJson> getElConstantDefinitions() {
    return BeanHelper.wrapElConstantDefinitionsMap(elMetadata.getElConstantDefinitions());
  }

  @JsonIgnore
  public ElMetadata getElMetadata() {
    return elMetadata;
  }
}
