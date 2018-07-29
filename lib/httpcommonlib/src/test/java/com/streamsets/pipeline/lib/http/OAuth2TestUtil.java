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

import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.mockito.Mockito;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.util.Collections;

import static org.mockito.Matchers.any;

public class OAuth2TestUtil {
  private String clientId = "streamsets_id";
  private String clientSecret = "this is a major secret, don't tell anyone";
  private String scope = "https://restricted/";
  public static final String TOKEN = "CJDb25DYWxlbmRhcnMuUmVhZCIsIkdyb3VwLlJlYWQuQWxsIiwiRGlyZWN0b3J5L";
  public static final String TOKEN_RESPONSE = "{\n" +
      "  \"token_type\": \"Bearer\",\n" +
      "  \"expires_in\": \"3600\",\n" +
      "  \"ext_expires_in\": \"0\",\n" +
      "  \"expires_on\": \"1484788319\",\n" +
      "  \"not_before\": \"1484784419\",\n" +
      "  \"access_token\": \"" + TOKEN + "\"\n" +
      "}";

  public OAuth2ConfigBean setup(Client client, WebTarget target, Invocation.Builder builder, Response response, OAuth2GrantTypes grantType) {
    OAuth2ConfigBean configBean = new OAuth2ConfigBean();
    MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
    RequestEntityProcessing transferEncoding = RequestEntityProcessing.BUFFERED;
    if (grantType == OAuth2GrantTypes.CLIENT_CREDENTIALS) {
      params.put(OAuth2ConfigBean.CLIENT_ID_KEY, Collections.singletonList(clientId));
      params.put(OAuth2ConfigBean.CLIENT_SECRET_KEY, Collections.singletonList(clientSecret));
      params.put(OAuth2ConfigBean.GRANT_TYPE_KEY, Collections.singletonList(OAuth2ConfigBean.CLIENT_CREDENTIALS_GRANT));
      configBean.clientId = () -> clientId;
      configBean.clientSecret = () -> clientSecret;
    } else {
      params.put(OAuth2ConfigBean.RESOURCE_OWNER_KEY, Collections.singletonList(clientId));
      params.put(OAuth2ConfigBean.PASSWORD_KEY, Collections.singletonList(clientSecret));
      params.put(OAuth2ConfigBean.GRANT_TYPE_KEY, Collections.singletonList(OAuth2ConfigBean.RESOURCE_OWNER_GRANT));
      configBean.username = () -> clientId;
      configBean.password = () -> clientSecret;
    }

    configBean.credentialsGrantType = grantType;
    configBean.transferEncoding = RequestEntityProcessing.BUFFERED;
    configBean.tokenUrl = "https://example.com";

    Mockito.when(response.readEntity(String.class)).thenReturn(TOKEN_RESPONSE);
    Mockito.when(response.getStatus()).thenReturn(200);
    Mockito.when(builder.post(Mockito.argThat(new FormMatcher(Entity.form(params))))).thenReturn(response);
    Mockito.when(builder.property(ClientProperties.REQUEST_ENTITY_PROCESSING, transferEncoding)).thenReturn(builder);
    Mockito.when(builder.header(any(String.class), any(Object.class))).thenReturn(builder);
    Mockito.when(target.request()).thenReturn(builder);

    Mockito.when(client.target(configBean.tokenUrl)).thenReturn(target);
    return configBean;
  }
}
