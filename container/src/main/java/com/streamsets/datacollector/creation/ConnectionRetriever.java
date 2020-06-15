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
package com.streamsets.datacollector.creation;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.ConnectionConfigurationJson;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.lib.security.http.RestClient;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

public class ConnectionRetriever {

  private static final String CONNECTION_RETRIEVAL_PATH_PREFIX =
      "/connection/rest/v1/connection/";
  private static final String CONNECTION_RETRIEVAL_PATH_POSTFIX = "/configs";
  private static final String USER_PARAM = "user";

  private final String schBaseUrl;
  private final RuntimeInfo runtimeInfo;

  public ConnectionRetriever(Configuration configuration, RuntimeInfo runtimeInfo) {
    this.schBaseUrl = RemoteSSOService.getValidURL(configuration.get(
        RemoteSSOService.DPM_BASE_URL_CONFIG,
        RemoteSSOService.DPM_BASE_URL_DEFAULT
    ));
    this.runtimeInfo = runtimeInfo;
  }

  public ConnectionConfiguration get(String connectionId, ConfigInjector.Context context) {
    if (runtimeInfo.isDPMEnabled()) {
      try {
        final RestClient restClient = getRestClientBuilder(schBaseUrl)
            .path(CONNECTION_RETRIEVAL_PATH_PREFIX + connectionId + CONNECTION_RETRIEVAL_PATH_POSTFIX)
            .json(true)
            .csrf(true)
            .appAuthToken(runtimeInfo.getAppAuthToken())
            .componentId(runtimeInfo.getId())
            .queryParam(USER_PARAM, context.getUser())
            .build();

        final RestClient.Response response = restClient.get();
        if (response.haveData() && response.successful()) {
          ConnectionConfigurationJson ccj = response.getData(ConnectionConfigurationJson.class);
          return BeanHelper.unwrapConnectionConfiguration(ccj);
        } else {
          String errorBody = StringUtils.join(response.getError());
          context.createIssue(CreationError.CREATION_1104, errorBody);
        }
      } catch (IOException e) {
        context.createIssue(CreationError.CREATION_1104, e.getMessage(), e);
      }
    } else {
      context.createIssue(CreationError.CREATION_1105);
    }
    return null;
  }

  @VisibleForTesting
  protected RestClient.Builder getRestClientBuilder(String schBaseUrl) {
    return RestClient.builder(schBaseUrl);
  }
}
