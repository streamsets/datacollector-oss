/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.store;

import com.streamsets.pipeline.api.ErrorId;

public enum PipelineStoreErrors implements ErrorId {
  PIPELINE_DOES_NOT_EXIST("Pipeline '{}' does not exist"),
  PIPELINE_ALREADY_EXISTS("Pipeline '{}' already exists"),
  COULD_NOT_CREATE_PIPELINE("Could not create pipeline '{}', {}"),
  COULD_NOT_DELETE_PIPELINE("Could not delete pipeline '{}', {}"),
  COULD_NOT_SAVE_PIPELINE("Could not save pipeline '{}', {}"),
  COULD_NOT_LOAD_PIPELINE_INFO("Could not load pipeline '{}' info, {}");

  private final String msgTemplate;

  PipelineStoreErrors(String msgTemplate) {
    this.msgTemplate = msgTemplate;
  }

  @Override
  public String getMessageTemplate() {
    return msgTemplate;
  }

}
