/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.config;

public enum StageType {
  SOURCE,
  PROCESSOR,
  TARGET,
  EXECUTOR,
  PIPELINE,
  ;

  public boolean isOneOf(StageType ...types) {
    if(types == null) {
      return false;
    }

    for(StageType t : types) {
      if(this == t) {
        return true;
      }
    }

    return false;
  }

  /**
   * Return API representation of this StageType.
   *
   * TODO: Why do we have two different stage types in the first place?
   */
  public com.streamsets.pipeline.api.StageType getApiType() {
    switch (this) {
      case SOURCE:
        return com.streamsets.pipeline.api.StageType.SOURCE;
      case PROCESSOR:
        return com.streamsets.pipeline.api.StageType.PROCESSOR;
      case TARGET:
        return com.streamsets.pipeline.api.StageType.TARGET;
      case EXECUTOR:
        return com.streamsets.pipeline.api.StageType.EXECUTOR;
      case PIPELINE:
        return com.streamsets.pipeline.api.StageType.PIPELINE;
      default:
        return null;
    }
  }

}
