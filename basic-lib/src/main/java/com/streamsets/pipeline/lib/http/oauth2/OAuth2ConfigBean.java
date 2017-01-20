/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.lib.http.oauth2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.VaultEL;
import com.streamsets.pipeline.lib.http.AuthenticationFailureException;
import com.streamsets.pipeline.lib.http.RequestEntityProcessingChooserValues;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.RequestEntityProcessing;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.channels.UnresolvedAddressException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OAuth2ConfigBean {

  public static final String CLIENT_ID_KEY = "client_id";
  public static final String CLIENT_SECRET_KEY = "client_secret";
  public static final String GRANT_TYPE_KEY = "grant_type";
  public static final String CLIENT_CREDENTIALS_GRANT = "client_credentials";

  public static final String RESOURCE_OWNER_KEY = "username";
  public static final String PASSWORD_KEY = "password";// NOSONAR
  public static final String RESOURCE_OWNER_GRANT = "password";

  public static final String ACCESS_TOKEN_KEY = "access_token";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Credentials Grant Type",
      displayPosition = 10,
      elDefs = VaultEL.class,
      group = "#0",
      dependsOn = "useOAuth2^",
      triggeredByValue = "true"
  )
  @ValueChooserModel(OAuth2GrantTypesChooserValues.class)
  public OAuth2GrantTypes credentialsGrantType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Token URL",
      description = "URL for token can be obtained using Client ID and Client Secret",
      displayPosition = 20,
      group = "#0",
      dependsOn = "useOAuth2^",
      triggeredByValue = "true"
  )
  public String tokenUrl;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Client ID",
      description = "OAuth2 Client ID",
      displayPosition = 30,
      elDefs = VaultEL.class,
      group = "#0",
      dependencies = {
          @Dependency(configName = "useOAuth2^", triggeredByValues = "true"),
          @Dependency(configName = "authType^", triggeredByValues = "NONE"),
          @Dependency(configName = "credentialsGrantType", triggeredByValues = "CLIENT_CREDENTIALS")
      }
  )
  public String clientId;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Client Secret",
      description = "OAuth 2 Client Secret",
      displayPosition = 40,
      elDefs = VaultEL.class,
      group = "#0",
      dependencies = {
          @Dependency(configName = "useOAuth2^", triggeredByValues = "true"),
          @Dependency(configName = "authType^", triggeredByValues = "NONE"),
          @Dependency(configName = "credentialsGrantType", triggeredByValues = "CLIENT_CREDENTIALS")
      }
  )
  public String clientSecret;

  /*
   * The next two are not required according to the protocol, but servers like IdentityServer 3 and Getty Images
   * require this even for resource owner credentials grant. So we have them with same labels, but they are not
   * required.
   */

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Client ID",
      description = "OAuth2 Client ID",
      displayPosition = 30,
      elDefs = VaultEL.class,
      group = "#0",
      dependencies = {
          @Dependency(configName = "useOAuth2^", triggeredByValues = "true"),
          @Dependency(configName = "credentialsGrantType", triggeredByValues = "RESOURCE_OWNER")
      }
  )
  public String resourceOwnerClientId;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Client Secret",
      description = "OAuth2 Client Secret",
      displayPosition = 40,
      elDefs = VaultEL.class,
      group = "#0",
      dependencies = {
          @Dependency(configName = "useOAuth2^", triggeredByValues = "true"),
          @Dependency(configName = "credentialsGrantType", triggeredByValues = "RESOURCE_OWNER")
      }
  )
  public String resourceOwnerClientSecret;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "User Name",
      description = "OAuth2 Resource Owner User Name",
      displayPosition = 30,
      elDefs = VaultEL.class,
      group = "#0",
      dependencies = {
          @Dependency(configName = "useOAuth2^", triggeredByValues = "true"),
          @Dependency(configName = "credentialsGrantType", triggeredByValues = "RESOURCE_OWNER")
      }
  )
  public String username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Password",
      description = "OAuth2 Password",
      displayPosition = 40,
      elDefs = VaultEL.class,
      group = "#0",
      dependencies = {
          @Dependency(configName = "useOAuth2^", triggeredByValues = "true"),
          @Dependency(configName = "credentialsGrantType", triggeredByValues = "RESOURCE_OWNER")
      }
  )
  public String password;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Request Transfer Encoding",
      defaultValue = "BUFFERED",
      displayPosition = 50,
      group = "#0",
      dependsOn = "useOAuth2^",
      triggeredByValue = "true"
  )
  @ValueChooserModel(RequestEntityProcessingChooserValues.class)
  public RequestEntityProcessing transferEncoding;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Additional Key-Value pairs in token request body",
      description = "Additional Key-Value pairs to be sent to the token URL while requesting for a token",
      displayPosition = 60,
      elDefs = VaultEL.class,
      group = "#0",
      dependsOn = "useOAuth2^",
      triggeredByValue = "true"
  )
  public Map<String, String> additionalValues = new HashMap<>();

  @VisibleForTesting
  OAuth2HeaderFilter filter;

  public void init(Client webClient) throws AuthenticationFailureException, IOException { // NOSONAR
    String accessToken = obtainAccessToken(webClient);
    filter = new OAuth2HeaderFilter(parseAccessToken(accessToken));
    webClient.register(filter);
  }

  @VisibleForTesting
  String obtainAccessToken(Client webClient) throws AuthenticationFailureException, IOException { //NOSONAR
    WebTarget tokenTarget = webClient.target(tokenUrl);

    Invocation.Builder builder = tokenTarget.request();
    Response response = null;
    try {
      response =
          builder.property(ClientProperties.REQUEST_ENTITY_PROCESSING, transferEncoding)
              .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED + "; charset=utf-8")
              .post(generateRequestEntity());
    } catch (ProcessingException ex) {
      if (ex.getCause() instanceof UnresolvedAddressException || ex.getCause() instanceof UnknownHostException) {
        throw new NotFoundException(ex.getCause());
      }
      throw ex;
    }
    final int status = response.getStatus();
    if (status == 404) {
      throw new NotFoundException();
    }
    final boolean statusOk = status >= 200 && status < 300;
    if (!statusOk) {
      throw new AuthenticationFailureException(
          Utils.format("Authentication failed with error Code: {} and  error message: {}",
              status, response.readEntity(String.class)));
    }
    return response.readEntity(String.class);
  }

  @VisibleForTesting
  String parseAccessToken(String tokenJson) throws IOException {
    JsonNode node = OBJECT_MAPPER.reader().readTree(tokenJson);
    return node.findValue(ACCESS_TOKEN_KEY).asText();
  }

  private Entity generateRequestEntity() {
    MultivaluedMap<String, String> requestValues = new MultivaluedHashMap<>();
   switch (credentialsGrantType) {
      case CLIENT_CREDENTIALS:
        insertClientCredentialsFields(requestValues);
        break;
      case RESOURCE_OWNER:
        insertResourceOwnerFields(requestValues);
        break;
      default:
    }
    for (Map.Entry<String, String> additionalValue : additionalValues.entrySet()) {
      requestValues.put(additionalValue.getKey(), Collections.singletonList(additionalValue.getValue()));
    }
    return Entity.form(requestValues);
  }

  private void insertClientCredentialsFields(MultivaluedMap<String, String> requestValues) {
    if (!StringUtils.isEmpty(clientId)) {
      requestValues.put(CLIENT_ID_KEY, Collections.singletonList(clientId));
      requestValues.put(CLIENT_SECRET_KEY, Collections.singletonList(clientSecret));
    }
    requestValues.put(GRANT_TYPE_KEY, Collections.singletonList(CLIENT_CREDENTIALS_GRANT));
  }

  private void insertResourceOwnerFields(MultivaluedMap<String, String> requestValues) {
    requestValues.put(RESOURCE_OWNER_KEY, Collections.singletonList(username));
    requestValues.put(PASSWORD_KEY, Collections.singletonList(password));
    requestValues.put(GRANT_TYPE_KEY, Collections.singletonList(RESOURCE_OWNER_GRANT));
    if (!StringUtils.isEmpty(resourceOwnerClientId)) {
      requestValues.put(CLIENT_ID_KEY, Collections.singletonList(resourceOwnerClientId));
    }
    if (!StringUtils.isEmpty(resourceOwnerClientSecret)) {
      requestValues.put(CLIENT_SECRET_KEY, Collections.singletonList(resourceOwnerClientSecret));
    }
  }

  public void onAccessDenied(Client webClient) throws AuthenticationFailureException, IOException { // NOSONAR
    filter.setShouldInsertHeader(false); // don't insert the header for requests to get new tokens.
    try {
      String newToken = obtainAccessToken(webClient);
      filter.setAuthToken(parseAccessToken(newToken));
    } finally {
      filter.setShouldInsertHeader(true);
    }
  }
}