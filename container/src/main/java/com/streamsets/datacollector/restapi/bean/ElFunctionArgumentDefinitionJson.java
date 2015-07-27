/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import com.streamsets.datacollector.el.ElFunctionArgumentDefinition;
import com.streamsets.pipeline.api.impl.Utils;

public class ElFunctionArgumentDefinitionJson {

  private final ElFunctionArgumentDefinition elFunctionArgumentDefinition;

  public ElFunctionArgumentDefinitionJson(ElFunctionArgumentDefinition elFunctionArgumentDefinition) {
    Utils.checkNotNull(elFunctionArgumentDefinition, "elFunctionArgumentDefinition");
    this.elFunctionArgumentDefinition = elFunctionArgumentDefinition;
  }

  public String getName() {
    return elFunctionArgumentDefinition.getName();
  }

  public String getType() {
    return elFunctionArgumentDefinition.getType();
  }

}
