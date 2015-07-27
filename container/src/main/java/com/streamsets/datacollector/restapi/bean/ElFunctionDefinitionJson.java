/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

public class ElFunctionDefinitionJson {

  private final ElFunctionDefinition elFunctionDefinition;

  public ElFunctionDefinitionJson(ElFunctionDefinition elFunctionDefinition) {
    Utils.checkNotNull(elFunctionDefinition, "elFunctionDefinition");
    this.elFunctionDefinition = elFunctionDefinition;
  }

  public String getName() {
    return elFunctionDefinition.getName();
  }

  public String getDescription() {
    return elFunctionDefinition.getDescription();
  }

  public String getGroup() {
    return elFunctionDefinition.getGroup();
  }

  public String getReturnType() {
    return elFunctionDefinition.getReturnType();
  }

  public List<ElFunctionArgumentDefinitionJson> getElFunctionArgumentDefinition() {
    return BeanHelper.wrapElFunctionArgumentDefinitions(elFunctionDefinition.getElFunctionArgumentDefinition());
  }

}
