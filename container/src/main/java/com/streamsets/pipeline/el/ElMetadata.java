/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import java.util.Map;

public class ElMetadata {
  private final Map<String, ElFunctionDefinition> elFunctionDefinitions;
  private final Map<String, ElConstantDefinition> elConstantDefinitions;
  private final Map<String, String> elGroupDefinitions;

  public ElMetadata(Map<String, ElFunctionDefinition> elFunctionDefinitions,
                    Map<String, ElConstantDefinition> elConstantDefinitions, Map<String, String> elGroupDefinitions) {
    this.elFunctionDefinitions = elFunctionDefinitions;
    this.elConstantDefinitions = elConstantDefinitions;
    this.elGroupDefinitions = elGroupDefinitions;
  }

  public Map<String, String> getElGroupDefinitions() {
    return elGroupDefinitions;
  }

  public Map<String, ElFunctionDefinition> getElFunctionDefinitions() {
    return elFunctionDefinitions;
  }

  public Map<String, ElConstantDefinition> getElConstantDefinitions() {
    return elConstantDefinitions;
  }
}
