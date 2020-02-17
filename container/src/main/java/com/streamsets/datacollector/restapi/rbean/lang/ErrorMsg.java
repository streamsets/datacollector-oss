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
package com.streamsets.datacollector.restapi.rbean.lang;

import com.streamsets.pipeline.api.ErrorCode;

public class ErrorMsg extends BaseMsg<ErrorMsg> {
  private BaseMsg cause;

  public ErrorMsg(ErrorCode code, Object... args) {
    this(null, code, args);
  }

  public ErrorMsg(RType reference, ErrorCode code, Object... args) {
    super(Type.ERROR, code, args);
    if (args != null && args.length > 0 && args[args.length - 1] instanceof BaseMsg) {
      cause = (BaseMsg) args[args.length - 1];
    }
  }

  public BaseMsg getCause() {
    return cause;
  }

  @Override
  public String toString() {
    if (getCause() == null) {
      return super.toString();
    } else {
      return super.toString() + " Cause: " + getCause();
    }
  }

}
