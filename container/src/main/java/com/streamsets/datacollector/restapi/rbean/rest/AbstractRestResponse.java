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
package com.streamsets.datacollector.restapi.rbean.rest;

import com.streamsets.datacollector.restapi.rbean.lang.BaseMsg;
import com.streamsets.datacollector.restapi.rbean.lang.NotificationMsg;
import com.streamsets.datacollector.restapi.rbean.lang.RType;
import com.streamsets.datacollector.restapi.rbean.lang.WarningMsg;

public abstract class AbstractRestResponse<B, R extends AbstractRestResponse> extends RType<R> {
  public static final String ENVELOPE_VERSION = "1";
  public static final int HTTP_OK = 200;
  public static final int HTTP_CREATED = 201;
  public static final int HTTP_NO_CONTENT = 204;

  private final String type;
  private int httpStatusCode = HTTP_OK;
  private B data;


  public AbstractRestResponse(String type) {
    this.type = type;
  }

  public String getEnvelopeVersion() {
    return ENVELOPE_VERSION;
  }

  public String getType() {
    return type;
  }

  public int getHttpStatusCode() {
    return httpStatusCode;
  }

  @SuppressWarnings("unchecked")
  public R setHttpStatusCode(int httpStatusCode) {
    this.httpStatusCode = httpStatusCode;
    return (R) this;
  }

  BaseMsg[] getMessagesArray() {
    return getMessages().toArray(new BaseMsg[getMessages().size()]);
  }

  public R addMessage(WarningMsg warning) {
    return (R) super.addMessage(warning);
  }

  public R addMessage(NotificationMsg notification) {
    return (R) super.addMessage(notification);
  }

  public B getData() {
    return data;
  }

  @SuppressWarnings("unchecked")
  public R setData(B data) {
    this.data = data;
    return (R) this;
  }

}
