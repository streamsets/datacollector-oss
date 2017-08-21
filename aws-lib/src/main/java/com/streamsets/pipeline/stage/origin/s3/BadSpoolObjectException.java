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
package com.streamsets.pipeline.stage.origin.s3;

public class BadSpoolObjectException extends Exception {
  private final String object;
  private final String pos;

  public BadSpoolObjectException(String object, String pos, Exception ex) {
    super(ex);
    this.object = object;
    this.pos = pos;
  }

  public String getObject() {
    return object;
  }

  public String getPos() {
    return pos;
  }
}
