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
package com.streamsets.pipeline.lib.http.oauth2;

import com.google.common.annotations.VisibleForTesting;

import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;
import java.util.Collections;

public class OAuth2HeaderFilter implements ClientRequestFilter {

  public static final String BEARER = "Bearer ";
  private boolean shouldInsertHeader = true;
  @VisibleForTesting
  String authToken;

  public OAuth2HeaderFilter(String authToken) {
    this.authToken = BEARER + authToken;
  }

  @Override
  public synchronized void filter(ClientRequestContext requestContext) throws IOException {
    if (shouldInsertHeader) {
      requestContext.getHeaders().put(HttpHeaders.AUTHORIZATION, Collections.singletonList((Object) authToken));
    }
  }

  public synchronized void setAuthToken(String newToken) {
    this.authToken = BEARER + newToken;
  }

  public void setShouldInsertHeader(boolean shouldInsertHeader) {
    this.shouldInsertHeader = shouldInsertHeader;
  }
}
