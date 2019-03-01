/*
 * Copyright 2019 StreamSets Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.datacollector.credential.thycotic.api;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.common.annotations.VisibleForTesting;

public class GetThycoticSecrets {

  private static final Logger LOG = LoggerFactory.getLogger(GetThycoticSecrets.class);

  public String getSecretField(
      CloseableHttpClient httpclient,
      String token,
      String secretServerUrl,
      Integer secretId,
      String secretField,
      String group
  ) {
    secretServerUrl = Joiner.on("").join(secretServerUrl, "/api/v1/secrets/", secretId, "/fields/", secretField);
    LOG.debug("Thycotic Secret Server URL: ", secretServerUrl);
    String returnedSecret;
    try {
      returnedSecret = getValue(httpclient, secretServerUrl, token);
      if (returnedSecret != null && !returnedSecret.isEmpty()) {
        // Thycotic Secret Server API returns the String with quotes. So removing the
        // quotes to get the actual value
        returnedSecret = returnedSecret.replace("\"", "");
        return returnedSecret;
      }
    } catch (Exception e) {
      LOG.debug("Exception in retrieving the secret value: ", e);
    }
    return null;
  }

  @VisibleForTesting
  protected String getValue(
      CloseableHttpClient httpclient, String secretServerUrl, String token
  ) throws Exception {

    HttpGet httpGet = new HttpGet(secretServerUrl);
    httpGet.setHeader("Authorization", "Bearer " + token);
    CloseableHttpResponse response2 = httpclient.execute(httpGet);
    try {
      String secretValue = EntityUtils.toString(response2.getEntity(), "UTF-8");
      return secretValue;
    } finally {
      response2.close();
    }
  }
}
