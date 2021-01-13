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
package com.streamsets.datacollector.tunneling;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TunnelingResponse {
  private String id;
  private int status;
  private Object payload;
  private Map<String, List<Object>> headers = new HashMap<>();

  /**
   * Get the WebSocket Message Id associated with the response.
   * @return the WebSocket Message Id.
   */
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public Object getPayload() {
    return payload;
  }

  public void setPayload(Object payload) {
    this.payload = payload;
  }

  public Map<String, List<Object>> getHeaders() {
    return headers;
  }

  public void setHeaders(Map<String, List<Object>> headers) {
    this.headers = headers;
  }
}
