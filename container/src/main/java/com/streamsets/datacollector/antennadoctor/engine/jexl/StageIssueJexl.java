/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.antennadoctor.engine.jexl;

import com.streamsets.pipeline.api.ErrorCode;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class StageIssueJexl {

  private final Throwable exception;
  private final String errorMessage;
  private final ErrorCode errorCode;
  private final List<Object> args;

  public StageIssueJexl(Throwable e) {
    this.exception = e;
    this.errorMessage = e.getMessage();
    this.errorCode = null;
    this.args = Collections.emptyList();
  }

  public StageIssueJexl(String errorMessage) {
    this.exception = null;
    this.errorMessage = errorMessage;
    this.errorCode = null;
    this.args = Collections.emptyList();
  }

  public StageIssueJexl(ErrorCode errorCode, Object ...args) {
    this.exception = null;
    this.errorMessage = null;
    this.errorCode = errorCode;
    this.args = Arrays.asList(args);
  }

  public Throwable exception() {
    return exception;
  }

  public String errorMessage() {
    return errorMessage;
  }

  public ErrorCode errorCode() {
    return errorCode;
  }

  public List<Object> args() {
    return args;
  }
}
