/**
 * Copyright 2020 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.restapi.rbean.usermgnt;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streamsets.datacollector.restapi.rbean.lang.RBean;
import com.streamsets.datacollector.restapi.rbean.lang.RBoolean;
import com.streamsets.datacollector.restapi.rbean.lang.RString;

public class RResetPasswordLink extends RBean<RResetPasswordLink> {

  private RString link = new RString();
  private RBoolean sentByEmail = new RBoolean(false);

  @JsonIgnore
  @Override
  public RString getId() {
    return null;
  }

  @Override
  public void setId(RString id) {
  }

  public RString getLink() {
    return link;
  }

  public RResetPasswordLink setLink(RString link) {
    this.link = link;
    return this;
  }

  public RBoolean getSentByEmail() {
    return sentByEmail;
  }

  public RResetPasswordLink setSentByEmail(RBoolean sentByEmail) {
    this.sentByEmail = sentByEmail;
    return this;
  }
}
