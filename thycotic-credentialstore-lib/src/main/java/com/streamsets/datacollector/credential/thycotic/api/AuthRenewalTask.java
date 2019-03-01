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

import java.util.ArrayList;
import java.util.List;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.annotations.VisibleForTesting;

public class AuthRenewalTask {

  private static final Logger LOG = LoggerFactory.getLogger(AuthRenewalTask.class);
  private static final String GRANT_TYPE = "password";
  private volatile String accessToken;
  private volatile long expires;
  private String secretServerUrl;
  private String username;
  private String password;
  private CloseableHttpClient httpclient;

  public AuthRenewalTask() {

  }

  public AuthRenewalTask(
      CloseableHttpClient httpclient, String secretServerUrl, String username, String password
  ) {
    this.secretServerUrl = secretServerUrl;
    this.username = username;
    this.password = password;
    this.httpclient = httpclient;
  }

  /**
   * <p>
   * Scheduler is not used since a dedicated thread will be running to update the token.Even if the
   * thycotic server is not being used,the thread will hit for every refresh interval to get a new
   * access token. So instead <tt>volatile</tt> variables are used to store the token.So only when
   * the token expires, thycotic server will be called to fetch the new token.
   * </p>
   * <p>
   * There are 2 expire checks to make sure that not more than one thread updates the token in case
   * of expiration
   */
  public String getAccessToken() {
    if (System.currentTimeMillis() > getExpireTime()) {
      try {
        synchronized (this) {
          if (System.currentTimeMillis() > getExpireTime()) {
            accessToken = fetchAccessToken();
            if (accessToken != null && !accessToken.isEmpty()) {
              expires = (System.currentTimeMillis() + expires);
            }
          }
        }
      } catch (Exception e) {
        LOG.debug("Error in fetching the access token: {} ", e);
      }
    }
    return accessToken;
  }

  @VisibleForTesting
  protected String fetchAccessToken() throws Exception {
    HttpPost httpPost = new HttpPost(secretServerUrl + "/oauth2/token");
    List<NameValuePair> nvps = new ArrayList<NameValuePair>();
    nvps.add(new BasicNameValuePair("username", username));
    nvps.add(new BasicNameValuePair("password", password));
    nvps.add(new BasicNameValuePair("grant_type", GRANT_TYPE));
    httpPost.setEntity(new UrlEncodedFormEntity(nvps));

    CloseableHttpResponse response2 = httpclient.execute(httpPost);

    try {
      String json = EntityUtils.toString(response2.getEntity(), "UTF-8");
      JSONParser parser = new JSONParser();
      Object resultObject = parser.parse(json);

      if (resultObject instanceof JSONObject) {
        JSONObject obj = (JSONObject) resultObject;
        if (obj.get("error") != null) {
          throw new Exception(
              "Error authenticating. Ensure username / password are correct and web services are enabled");
        }
        accessToken = (String) obj.get("access_token");
        expires = (Long) obj.get("expires_in");
        LOG.debug("Auth Bearer Token {}:", accessToken);
        return accessToken;
      }
    } finally {
      response2.close();
    }
    return null;
  }

  @VisibleForTesting
  public long getExpireTime() {
    return expires;
  }
}
