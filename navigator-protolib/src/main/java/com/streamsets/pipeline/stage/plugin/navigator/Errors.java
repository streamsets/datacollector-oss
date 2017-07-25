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

package com.streamsets.pipeline.stage.plugin.navigator;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  NAVIGATOR_01("Error sending to Navigator '{}'"),
  NAVIGATOR_02("Error communicating with Navigator server '{}'"),
  NAVIGATOR_03("Invalid LineageEndPointType '{}'"),
  NAVIGATOR_04("START message - but key {} is already cached. "),
  NAVIGATOR_05("got {} event for {} but nothing is cached. {}"),
  NAVIGATOR_06("processing key {} but cache has no entries."),
  NAVIGATOR_07("error multiple STOP events."),
  NAVIGATOR_08("error multiple START events."),
  ;

  private final String msg;

  Errors(String msg) {
    this.msg = msg;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return msg;
  }
}
