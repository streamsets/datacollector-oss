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
package com.streamsets.pipeline.lib.salesforce.connection;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.salesforce.connection.mutualauth.MutualAuthConfigBean;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.client.Client;

import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Date;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
@ConnectionDef(
    label = "Salesforce",
    type = SalesforceConnection.TYPE,
    description = "Connects to Salesforce and Salesforce Einstein Analytics",
    version = 2,
    upgraderDef = "upgrader/SalesforceConnection.yaml",
    supportedEngines = { ConnectionEngine.COLLECTOR }
)
@ConfigGroups(SalesforceConnectionGroups.class)
public class SalesforceConnection {

  public static final String TYPE = "STREAMSETS_SALESFORCE";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      defaultValue = "login.salesforce.com",
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Auth Endpoint",
      displayPosition = 10,
      group = "FORCE"
  )
  public String authEndpoint;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      defaultValue = "43.0",
      required = true,
      type = ConfigDef.Type.STRING,
      label = "API Version",
      displayPosition = 20,
      group = "FORCE"
  )
  public String apiVersion;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "BASIC",
      label = "Authentication Type",
      description = "Method to authenticate to Salesforce API.",
      displayPosition = 30,
      group = "FORCE"
  )
  @ValueChooserModel(AuthTypeChooserValues.class)
  public AuthType authType;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      displayPosition = 40,
      group = "FORCE"
  )
  public CredentialValue username;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      displayPosition = 50,
      dependsOn = "authType",
      triggeredByValue = "BASIC",
      group = "FORCE"
  )
  public CredentialValue password;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Consumer Key",
      description = "Consumer Key for connected app. Used as the client ID in the OAuth flow.",
      displayPosition = 60,
      dependsOn = "authType",
      triggeredByValue = "OAUTH",
      group = "FORCE"
  )
  public CredentialValue consumerKey;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Private Key",
      description = "Private key for signing JWT. Should correspond the certificate uploaded in the Salesforce UI " +
          "for the connected app. Should be stored in a credential vault and retried via a credential EL call, but " +
          "can be uploaded directly as a base-64 encoded string.",
      displayPosition = 80,
      dependsOn = "authType",
      triggeredByValue = "OAUTH",
      group = "FORCE"
  )
  public CredentialValue privateKey;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      label = "Use Proxy",
      type = ConfigDef.Type.BOOLEAN,
      description = "Connect to Salesforce via a proxy server.",
      defaultValue = "false",
      displayPosition = 10,
      group = "ADVANCED"
  )
  public boolean useProxy = false;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      label = "Proxy Hostname",
      type = ConfigDef.Type.STRING,
      description = "Proxy Server Hostname",
      defaultValue = "",
      displayPosition = 20,
      group = "ADVANCED",
      dependsOn = "useProxy",
      triggeredByValue = "true"
  )
  public String proxyHostname = "";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      label = "Proxy Port",
      type = ConfigDef.Type.NUMBER,
      description = "Proxy Server Hostname",
      defaultValue = "",
      displayPosition = 30,
      group = "ADVANCED",
      dependsOn = "useProxy",
      triggeredByValue = "true"
  )
  public int proxyPort = 0;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      label = "Proxy Requires Credentials",
      type = ConfigDef.Type.BOOLEAN,
      description = "Enable if you need to supply a username/password to connect via the proxy server.",
      defaultValue = "false",
      displayPosition = 40,
      group = "ADVANCED",
      dependsOn = "useProxy",
      triggeredByValue = "true"
  )
  public boolean useProxyCredentials = false;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      label = "Proxy Realm",
      type = ConfigDef.Type.CREDENTIAL,
      description = "Authentication realm for the proxy server.",
      displayPosition = 50,
      group = "ADVANCED",
      dependencies = {
          @Dependency(configName = "useProxy", triggeredByValues = "true"),
          @Dependency(configName = "useProxyCredentials", triggeredByValues = "true")
      }
  )
  public CredentialValue proxyRealm;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      label = "Proxy Username",
      type = ConfigDef.Type.CREDENTIAL,
      description = "Username for the proxy server.",
      displayPosition = 60,
      group = "ADVANCED",
      dependencies = {
          @Dependency(configName = "useProxy", triggeredByValues = "true"),
          @Dependency(configName = "useProxyCredentials", triggeredByValues = "true")
      }
  )
  public CredentialValue proxyUsername;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      label = "Proxy Password",
      type = ConfigDef.Type.CREDENTIAL,
      description = "Password for the proxy server.",
      displayPosition = 70,
      group = "ADVANCED",
      dependencies = {
          @Dependency(configName = "useProxy", triggeredByValues = "true"),
          @Dependency(configName = "useProxyCredentials", triggeredByValues = "true")
      }
  )
  public CredentialValue proxyPassword;

  @ConfigDefBean(groups = "ADVANCED")
  public MutualAuthConfigBean mutualAuth = new MutualAuthConfigBean();

  /* Construct JWT bearer token when OAuth is chosen. Closely follows examples
    from https://help.salesforce.com/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5
    and https://developer.okta.com/blog/2018/10/31/jwts-with-java.
    The JWT standard claims are iss, sub, aud, and exp. As per the first link above, the expiration should be within
    three minutes of the assertion.
   */
  public String getBearerToken() throws Exception {

    // Get current time and set the expiration in milliseconds to three minutes from now.
    long nowMillis = System.currentTimeMillis();
    Date now = new Date(nowMillis);
    long expMillis = nowMillis + 180000;
    Date exp = new Date(expMillis);

    // Create JWT claims object.
    JwtBuilder builder = Jwts.builder()
        .setIssuedAt(now)
        .setIssuer(consumerKey.get())
        .setSubject(username.get())
        .setAudience("https://" + authEndpoint)
        .setExpiration(exp);

    // Set signature algorithm for jwt signing.
    SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.RS256;

    // Retrieve Base64-encoded private key from credential store (or pipeline).
    // Strip headers, spaces, and newlines.
    String key = privateKey.get()
        .replace("-----BEGIN PRIVATE KEY-----\n", "")
        .replace("-----END PRIVATE KEY-----", "")
        .replace(" ", "")
        .replace("\n", "")
        .replace("\r", "");

    // Base-64 decode the data, and PKCS8-decode the encoded RSA private key.
    byte[] encoded = Base64.getDecoder().decode(key.getBytes());
    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);

    // Obtain the private key and sign the JWT with it.
    KeyFactory kf = KeyFactory.getInstance("RSA");
    PrivateKey signingKey = kf.generatePrivate(keySpec);
    builder.signWith(signatureAlgorithm, signingKey);

    // Return JWT serialized as string.
    return builder.compact();
  }

  /* Construct HTTP client for OAuth login that returns the Salesforce instance URL and
    session token. The instance URL is used as the Service Endpoint and the session token is
    used as the Session Id in the partnerConfig.

    Salesforce follows RFC-7523 JSON Web Token Profile for OAuth2 Auth and Authz.
    The spec is available at https://tools.ietf.org/html/rfc7523.
    As per the spec, the grant_type parameter is "urn:ietf:params:oauth:grant-type:jwt-bearer."

    See here for sample token request:
      https://help.salesforce.com/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5
   */
  public SalesforceJWTResponse getSessionConfig() throws Exception {
    final String grantType = "urn:ietf:params:oauth:grant-type:jwt-bearer";
    String params;
    Client client = ClientBuilder.newClient();

    // Construct URL parameters and post as URL form encoded. We'll construct the URL directly
    // instead of relying on any helper method to construct a form.
    try {
      params = "?grant_type=" + grantType + "&assertion=" + getBearerToken();
    } catch (Exception e) {
      throw new JWTGenerationException();
    }

    try {
      Response response = client.target("https://" + authEndpoint + "/services/oauth2/token" + params).request().post(
          Entity.entity(SalesforceJWTResponse.class, MediaType.APPLICATION_JSON));
      if (response.getStatus() != 200) {
        throw new SessionIdFailureException();
      }
      SalesforceJWTResponse sf = response.readEntity(SalesforceJWTResponse.class);
      return sf;
    } catch (Exception e) {
      throw new SessionIdFailureException();
    }
  }
}
