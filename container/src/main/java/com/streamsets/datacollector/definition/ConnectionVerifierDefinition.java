/*
 * Copyright 2020 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.datacollector.definition;

import com.streamsets.pipeline.api.impl.Utils;

/**
 * Class representing the connection verifier information
 */
public class ConnectionVerifierDefinition {

  private String verifierClass;
  private String verifierConnectionFieldName;
  private String verifierConnectionSelectionFieldName;
  private String verifierType;
  private String library;

  public ConnectionVerifierDefinition() { }

  public ConnectionVerifierDefinition(
      String verifierClass,
      String verifierConnectionFieldName,
      String verifierConnectionSelectionFieldName,
      String verifierType,
      String library
  ) {
    this.verifierClass = verifierClass;
    this.verifierConnectionFieldName = verifierConnectionFieldName;
    this.verifierConnectionSelectionFieldName = verifierConnectionSelectionFieldName;
    this.verifierType = verifierType;
    this.library = library;
  }

  public String getVerifierClass() {
    return verifierClass;
  }

  public String getVerifierConnectionFieldName() {
    return verifierConnectionFieldName;
  }

  public String getVerifierConnectionSelectionFieldName() {
    return verifierConnectionSelectionFieldName;
  }

  public String getVerifierType() {
    return verifierType;
  }

  public String getLibrary() {
    return library;
  }

  @Override
  public String toString() {
    return Utils.format(
        "ConnectionVerifierDefinition[verifierClass='{}', " +
            "verifierConnectionFieldName='{}', " +
            "verifierConnectionSelectionFieldName='{}', " +
            "verifierType='{}', " +
            "library='{}']",
        verifierClass, verifierConnectionFieldName, verifierConnectionSelectionFieldName, verifierType, library
    );
  }
}
