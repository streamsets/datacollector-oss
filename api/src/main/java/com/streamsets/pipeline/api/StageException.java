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

package com.streamsets.pipeline.api;

import com.streamsets.pipeline.container.LocalizableErrorId;
import com.streamsets.pipeline.container.NonLocalizableErrorId;
import com.streamsets.pipeline.container.LocalizableString;
import com.streamsets.pipeline.container.Utils;

public class StageException extends Exception {

  private static Throwable getCause(Object... params) {
    Throwable throwable = null;
    if (params.length > 0 && params[params.length - 1] instanceof Throwable) {
      throwable = (Throwable) params[params.length - 1];
    }
    return throwable;
  }

  private final ErrorId errorId;
  private final LocalizableString localizedErrorId;

  // last parameter can be an exception cause
  public StageException(ErrorId errorId, Object... params) {
    super(getCause(params));
    this.errorId = Utils.checkNotNull(errorId, "errorId");
    this.localizedErrorId = (errorId instanceof Enum) ? new LocalizableErrorId(errorId, params)
                                                      : new NonLocalizableErrorId(errorId, params);
  }

  public ErrorId getId() {
    return errorId;
  }

  @Override
  public String getMessage() {
    return localizedErrorId.getNonLocalized();
  }

  @Override
  public String getLocalizedMessage() {
    return localizedErrorId.getLocalized();
  }

}
