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
package com.streamsets.lib.security.http.aster;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.streamsets.datacollector.io.DataStore;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.client.resource.OAuth2ProtectedResourceDetails;
import org.springframework.security.oauth2.client.token.ClientTokenServices;
import org.springframework.security.oauth2.common.DefaultOAuth2AccessToken;
import org.springframework.security.oauth2.common.DefaultOAuth2RefreshToken;
import org.springframework.security.oauth2.common.OAuth2AccessToken;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.time.Instant;
import java.util.Date;

/**
 * {@link ClientTokenServices} implementation that stores the engine access and refresh tokens.
 * <p/>
 * This implementation does not use resources and authentication parameters as it is used to store a single set of
 * access and refresh tokens, the ones from the engine itself.
 */
public class EngineClientTokenServices implements ClientTokenServices {
  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(
      JsonGenerator.Feature.AUTO_CLOSE_TARGET,
      false
  );

  private final String clientId;
  private final DataStore dataStore;

  private String getSubject(AsterTokenResponse response) {
    String subject;
    try {
      JWTClaimsSet claims = SignedJWT.parse(response.getAccess_token()).getJWTClaimsSet();
      subject = claims.getSubject();
      if (subject == null) {
        throw new AsterException("AccessToken does not have a subject");
      }
    } catch (ParseException ex){
      throw new AsterException(String.format("Could not parse AccessToken, error: %s", ex), ex);
    }
    return subject;
  }

  private void assertSubjectIsEngineId(AsterTokenResponse response) {
    String sub = getSubject(response);
    if (!clientId.equals(sub)) {
      throw new AsterException(String.format(
          "Engine ID '%s' does not match client ID '%s' in access token. Delete the '%s' file and restart.",
          clientId,
          sub,
          dataStore.getFile().getAbsolutePath()
      ));
    }
  }

  /**
   * Constructor. It takes the file to use to store the access and refresh tokens.
   */
  public EngineClientTokenServices(String clientId, File file) {
    this.clientId = Preconditions.checkNotNull(clientId, "clientId");
    dataStore = new DataStore(Preconditions.checkNotNull(file, "file"));
  }

  /**
   * Returns if there is currently a file with tokens.
   */
  public boolean hasTokens() {
    try {
      return dataStore.exists();
    } catch (IOException ex1) {
      throw new AsterException(
          String.format("Could not check if exists '%s' engine tokens: %s", dataStore.getFile(), ex1),
          ex1
      );
    }
  }

  /**
   * Saves the access and refresh tokens received during registration in the store.
   */
  public void saveRegistrationToken(AsterTokenResponse registrationResponse) {
    Preconditions.checkNotNull(registrationResponse, "registrationResponse");
    assertSubjectIsEngineId(registrationResponse);
    long expiresOn = Instant.now().plusSeconds(registrationResponse.getExpires_in()).getEpochSecond();
    registrationResponse.setExpires_on(expiresOn);
    try (OutputStream os = dataStore.getOutputStream()) {
      OBJECT_MAPPER.writeValue(os, registrationResponse);
      dataStore.commit(os);
    } catch (IOException ex) {
      throw new AsterException(String.format("Could not save to '%s' engine tokens: %s", dataStore.getFile(), ex), ex);
    } finally {
      dataStore.release();
    }
  }

  /**
   * Loads the access and refresh tokens from the store.
   * <p/>
   * <p/>
   * The {@code resource} and {@code authentication} parameters are ignored.
   */
  @Override
  public OAuth2AccessToken getAccessToken(OAuth2ProtectedResourceDetails resource, Authentication authentication) {
    try (InputStream is = dataStore.getInputStream()) {
      AsterTokenResponse response = OBJECT_MAPPER.readValue(is, AsterTokenResponse.class);
      assertSubjectIsEngineId(response);
      DefaultOAuth2AccessToken accessToken = new DefaultOAuth2AccessToken(response.getAccess_token());
      accessToken.setExpiration(Date.from(Instant.ofEpochSecond(response.getExpires_on())));
      DefaultOAuth2RefreshToken refreshToken = new DefaultOAuth2RefreshToken(response.getRefresh_token());
      accessToken.setRefreshToken(refreshToken);
      accessToken.setTokenType("Bearer");
      return accessToken;
    } catch (IOException ex) {
      throw new AsterException(
          String.format("Could not load from '%s' engine tokens: %s", dataStore.getFile(), ex),
          ex
      );
    }
  }

  /**
   * Saves the access and refresh tokens received during a token refresh.
   * <p/>
   * The {@code resource} and {@code authentication} parameters are ignored.
   */
  @Override
  public void saveAccessToken(
      OAuth2ProtectedResourceDetails resource, Authentication authentication, OAuth2AccessToken accessToken
  ) {
    Preconditions.checkNotNull(accessToken, "accessToken");
    AsterTokenResponse response = new AsterTokenResponse().setAccess_token(accessToken.getValue())
        .setExpires_in(accessToken.getExpiresIn())
        .setExpires_on(Instant.now().plusSeconds(accessToken.getExpiresIn()).getEpochSecond())
        .setRefresh_token(accessToken.getRefreshToken().getValue());
    assertSubjectIsEngineId(response);
    saveRegistrationToken(response);
  }

  /**
   * Removes the access and refresh tokens from the store.
   * <p/>
   * <p/>
   * The {@code resource} and {@code authentication} parameters are ignored.
   */
  @Override
  public void removeAccessToken(OAuth2ProtectedResourceDetails resource, Authentication authentication) {
    try {
      dataStore.delete();
    } catch (IOException ex) {
      throw new AsterException(String.format("Could not delete '%s' engine tokens: %s", dataStore.getFile(), ex), ex);
    }
  }

}
