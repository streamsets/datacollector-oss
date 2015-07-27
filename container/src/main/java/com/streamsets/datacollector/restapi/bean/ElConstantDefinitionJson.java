/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import com.streamsets.datacollector.el.ElConstantDefinition;
import com.streamsets.pipeline.api.impl.Utils;

public class ElConstantDefinitionJson {

  private final ElConstantDefinition elConstantDefinition;

  public ElConstantDefinitionJson(ElConstantDefinition elConstantDefinition) {
    Utils.checkNotNull(elConstantDefinition, "elConstantDefinition");
    this.elConstantDefinition = elConstantDefinition;
  }

  public String getName() {
    return elConstantDefinition.getName();
  }

  public String getDescription() {
    return elConstantDefinition.getDescription();
  }

  public String getReturnType() {
    return elConstantDefinition.getReturnType();
  }

}
