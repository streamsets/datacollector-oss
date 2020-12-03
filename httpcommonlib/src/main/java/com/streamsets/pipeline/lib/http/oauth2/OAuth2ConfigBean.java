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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.http.AuthenticationFailureException;
import com.streamsets.pipeline.lib.http.RequestEntityProcessingChooserValues;
import io.jsonwebtoken.Header;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.lib.http.Errors.HTTP_25;
import static com.streamsets.pipeline.lib.http.Errors.HTTP_26;
import static com.streamsets.pipeline.lib.http.Errors.HTTP_27;
import static com.streamsets.pipeline.lib.http.Errors.HTTP_33;

public class OAuth2ConfigBean {

  public static final String CONFIG_GROUP = "OAUTH2";
  public static final String ASSERTION_KEY = "assertion";
  private static final Logger LOG = LoggerFactory.getLogger(OAuth2ConfigBean.class);

  public static final String CLIENT_ID_KEY = "client_id";
  public static final String CLIENT_SECRET_KEY = "client_secret";
  public static final String GRANT_TYPE_KEY = "grant_type";
  public static final String CLIENT_CREDENTIALS_GRANT = "client_credentials";
  public static final String JWT_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer";

  public static final String RESOURCE_OWNER_KEY = "username";
  public static final String PASSWORD_KEY = "password";// NOSONAR
  public static final String RESOURCE_OWNER_GRANT = "password";

  // RFC-6749 name for access token:
  private static final String ACCESS_TOKEN_RFC = "access_token";
  // SalesForce uses this name for access token (SDC-9185 for discussion):
  private static final String ACCESS_TOKEN_SF = "accessToken";
  // Google JWT auth may use this style (SDC-8089 for discussion):
  private static final String ACCESS_TOKEN_GOOGLE = "id_token";

  private static final String PREFIX = "conf.client.oauth2.";

  public static final String RSA = "RSA";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Credentials Grant Type",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
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
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      dependsOn = "useOAuth2^",
      triggeredByValue = "true"
  )
  public String tokenUrl;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Client ID",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      dependencies = {
          @Dependency(configName = "useOAuth2^", triggeredByValues = "true"),
          @Dependency(configName = "authType^", triggeredByValues = "NONE"),
          @Dependency(configName = "credentialsGrantType", triggeredByValues = "CLIENT_CREDENTIALS")
      }
  )
  public CredentialValue clientId = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Client Secret",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      dependencies = {
          @Dependency(configName = "useOAuth2^", triggeredByValues = "true"),
          @Dependency(configName = "authType^", triggeredByValues = "NONE"),
          @Dependency(configName = "credentialsGrantType", triggeredByValues = "CLIENT_CREDENTIALS")
      }
  )
  public CredentialValue clientSecret = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      dependencies = {
          @Dependency(configName = "useOAuth2^", triggeredByValues = "true"),
          @Dependency(configName = "credentialsGrantType", triggeredByValues = "RESOURCE_OWNER")
      }
  )
  public CredentialValue username = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      dependencies = {
          @Dependency(configName = "useOAuth2^", triggeredByValues = "true"),
          @Dependency(configName = "credentialsGrantType", triggeredByValues = "RESOURCE_OWNER")
      }
  )
  public CredentialValue password = () -> "";

  /*
   * The next two are not required according to the protocol, but servers like IdentityServer 3 and Getty Images
   * require this even for resource owner credentials grant. So we have them with same labels, but they are not
   * required.
   */
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Client ID",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      dependencies = {
          @Dependency(configName = "useOAuth2^", triggeredByValues = "true"),
          @Dependency(configName = "credentialsGrantType", triggeredByValues = "RESOURCE_OWNER")
      }
  )
  public CredentialValue resourceOwnerClientId = () -> "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Client Secret",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      dependencies = {
          @Dependency(configName = "useOAuth2^", triggeredByValues = "true"),
          @Dependency(configName = "credentialsGrantType", triggeredByValues = "RESOURCE_OWNER")
      }
  )
  public CredentialValue resourceOwnerClientSecret = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "JWT Signing Algorithm",
      description = "The algorithm to use for signing the JWT",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      defaultValue = "NONE",
      dependencies = {
          @Dependency(configName = "useOAuth2^", triggeredByValues = "true"),
          @Dependency(configName = "credentialsGrantType", triggeredByValues = "JWT"),
      }
  )
  @ValueChooserModel(SigningAlgorithmsChooserValues.class)
  public SigningAlgorithms algorithm;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "JWT Signing Key (Base64-encoded)",
      description = "Base64 encoded key for signing the JWT",
      displayPosition = 35,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      dependencies = {
          @Dependency(configName = "useOAuth2^", triggeredByValues = "true"),
          @Dependency(configName = "credentialsGrantType", triggeredByValues = "JWT"),
          @Dependency(configName = "algorithm", triggeredByValues = {
              "HS256", "HS384", "HS512", "RS256", "RS384", "RS512"
          })
      }
  )
  public CredentialValue key = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "JWT Claims",
      description = "Claims to be used with JWT token request, represented as JSON",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      elDefs = {TimeEL.class, TimeNowEL.class},
      group = "#0",
      dependencies = {
          @Dependency(configName = "useOAuth2^", triggeredByValues = "true"),
          @Dependency(configName = "credentialsGrantType", triggeredByValues = "JWT")
      },
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String jwtClaims = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Request Transfer Encoding",
      defaultValue = "BUFFERED",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      dependsOn = "useOAuth2^",
      triggeredByValue = "true"
  )
  @ValueChooserModel(RequestEntityProcessingChooserValues.class)
  public RequestEntityProcessing transferEncoding;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Additional Key-Value pairs in token request body",
      description = "Additional key-value pairs to be sent to the token URL while requesting for a token",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      dependsOn = "useOAuth2^",
      triggeredByValue = "true"
  )
  @ListBeanModel
  public List<BodyKeyValueBean> additionalValues = new ArrayList<>();

  @VisibleForTesting
  OAuth2HeaderFilter filter;

  private ELVars elVars;
  private PrivateKey privateKey;
  private ELEval timeEvaluator;

  public void init(
    Stage.Context context,
    List<Stage.ConfigIssue> issues,
    Client webClient
  ) throws AuthenticationFailureException, IOException, StageException {

    if (credentialsGrantType == OAuth2GrantTypes.JWT) {
      prepareEL(context, issues);
      if (isRSA()) {
        privateKey = parseRSAKey(key.get(), context, issues);
      }
    }

    if (issues.isEmpty()) {
      String accessToken = obtainAccessToken(webClient);
      if (!accessToken.isEmpty()) {
        String token = parseAccessToken(accessToken);
        if (token.isEmpty()) {
          issues.add(context.createConfigIssue("#0",
              "",
              HTTP_33,
              ACCESS_TOKEN_RFC,
              ACCESS_TOKEN_SF,
              ACCESS_TOKEN_GOOGLE
          ));
        } else {
          filter = new OAuth2HeaderFilter(token);
          webClient.register(filter);
        }
      }
    }
  }

  private boolean isRSA() {
    return algorithm == SigningAlgorithms.RS256 || algorithm == SigningAlgorithms.RS384 || algorithm == SigningAlgorithms.RS512;
  }

  private boolean isHMAC() {
    return algorithm == SigningAlgorithms.HS256 || algorithm == SigningAlgorithms.HS384 || algorithm == SigningAlgorithms.HS512;
  }

  private void prepareEL(Stage.Context context, List<Stage.ConfigIssue> issues) {
    try {
      String resolvedJwtClaims = jwtClaims;
      context.parseEL(resolvedJwtClaims);
      timeEvaluator = context.createELEval("jwtClaims");
      elVars = context.createELVars();
      TimeNowEL.setTimeNowInContext(elVars, new Date());
      TimeEL.setCalendarInContext(elVars, Calendar.getInstance());
      timeEvaluator.eval(elVars, resolvedJwtClaims, String.class);
    } catch (StageException ex) {
      LOG.warn("Invalid EL in JWT Claims", ex);
      issues.add(context.createConfigIssue(CONFIG_GROUP, PREFIX + "jwtClaims", HTTP_25));
    }
  }

  private static PrivateKey parseRSAKey(String key, Stage.Context context, List<Stage.ConfigIssue> issues) {
    String privKeyPEM = key.replace("-----BEGIN PRIVATE KEY-----\n", "");
    privKeyPEM = privKeyPEM.replace("-----END PRIVATE KEY-----", "");
    privKeyPEM = privKeyPEM.replace("\n", "");
    privKeyPEM = privKeyPEM.replace("\r", "");

    try {
      // Base64 decode the data
      byte [] encoded = Base64.getDecoder().decode(privKeyPEM.getBytes());

      // PKCS8 decode the encoded RSA private key
      PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);

      KeyFactory kf = KeyFactory.getInstance(RSA);
      return kf.generatePrivate(keySpec);
    } catch (NoSuchAlgorithmException ex) {
      LOG.error(Utils.format("'{}' algorithm not available", RSA), ex);
      issues.add(context.createConfigIssue(CONFIG_GROUP, PREFIX + "algorithm", HTTP_25));
    } catch (InvalidKeySpecException ex) {
      LOG.error(Utils.format("'{}' algorithm not available", RSA), ex);
      issues.add(context.createConfigIssue(CONFIG_GROUP, PREFIX + "key", HTTP_26));
    } catch (IllegalArgumentException ex) {
      LOG.error("Invalid key", ex);
      issues.add(context.createConfigIssue(CONFIG_GROUP, PREFIX + "key", HTTP_27, ex.toString()));
    }
    return null;
  }

  @VisibleForTesting
  String obtainAccessToken(Client webClient) throws AuthenticationFailureException, IOException, StageException { //NOSONAR
    WebTarget tokenTarget = webClient.target(tokenUrl);
    Invocation.Builder builder = tokenTarget.request();
    Response response = sendRequest(builder); // local var for debugging purposes
    return processResponse(response);
  }

  private Response sendRequest(Invocation.Builder builder) throws IOException, StageException {
    Response response;
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
    return response;
  }

  private String processResponse(Response response) throws AuthenticationFailureException {
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

    String answer = "";
    if (node.has(ACCESS_TOKEN_RFC)) {
      answer = node.findValue(ACCESS_TOKEN_RFC).asText();

    } else if (node.has(ACCESS_TOKEN_SF)) {
      answer = node.findValue(ACCESS_TOKEN_SF).asText();

    } else if (node.has(ACCESS_TOKEN_GOOGLE)) {
      answer = node.findValue(ACCESS_TOKEN_GOOGLE).asText();
    }

    return answer;
  }

  private Entity generateRequestEntity() throws IOException, StageException {
    MultivaluedMap<String, String> requestValues = new MultivaluedHashMap<>();
   switch (credentialsGrantType) {
      case CLIENT_CREDENTIALS:
        insertClientCredentialsFields(requestValues);
        break;
      case RESOURCE_OWNER:
        insertResourceOwnerFields(requestValues);
        break;
     case JWT:
       insertJWTFields(requestValues);
       break;
      default:
    }
    for (BodyKeyValueBean additionalValue : additionalValues) {
      if (!StringUtils.isBlank(additionalValue.key)) {
        requestValues.put(additionalValue.key, Collections.singletonList(additionalValue.value.get()));
      }
    }
    return Entity.form(requestValues);
  }

  private void insertClientCredentialsFields(MultivaluedMap<String, String> requestValues) throws StageException {
    if(clientId != null) {
      String resolvedClientId = clientId.get();
      if (!StringUtils.isEmpty(resolvedClientId)) {
        requestValues.put(CLIENT_ID_KEY, Collections.singletonList(resolvedClientId));
        requestValues.put(CLIENT_SECRET_KEY, Collections.singletonList(clientSecret.get()));
      }
    }
    requestValues.put(GRANT_TYPE_KEY, Collections.singletonList(CLIENT_CREDENTIALS_GRANT));
  }

  private void insertResourceOwnerFields(MultivaluedMap<String, String> requestValues) throws StageException {
    requestValues.put(RESOURCE_OWNER_KEY, Collections.singletonList(username.get()));
    requestValues.put(PASSWORD_KEY, Collections.singletonList(password.get()));
    requestValues.put(GRANT_TYPE_KEY, Collections.singletonList(RESOURCE_OWNER_GRANT));

    String resolvedResourceOwnerClientId = resourceOwnerClientId.get();
    String resolvedResourceOwnerClientSecret = resourceOwnerClientSecret.get();
    if (!StringUtils.isEmpty(resolvedResourceOwnerClientId)) {
      requestValues.put(CLIENT_ID_KEY, Collections.singletonList(resolvedResourceOwnerClientId));
    }
    if (!StringUtils.isEmpty(resolvedResourceOwnerClientSecret)) {
      requestValues.put(CLIENT_SECRET_KEY, Collections.singletonList(resolvedResourceOwnerClientSecret));
    }
  }

  @SuppressWarnings("unchecked")
  private void insertJWTFields(MultivaluedMap<String, String> requestValues) throws IOException, StageException {
    String parsedJwt;
    try {
      parsedJwt = timeEvaluator.eval(elVars, jwtClaims, String.class);
    } catch (Exception ex) { // NOSONAR
      throw new RuntimeException(ex); // NOSONAR Unlikely to ever happen since init would have failed.
    }
    Map<String, Object> claims = (Map<String, Object>) OBJECT_MAPPER.readValue(parsedJwt, Map.class);
    JwtBuilder builder = Jwts.builder().setClaims(claims);
    try {

      if (isRSA()) {
        builder.signWith(JWTUtils.getSignatureAlgorithm(algorithm), privateKey);
      } else if (isHMAC()) {
        builder.signWith(JWTUtils.getSignatureAlgorithm(algorithm), key.get());
      }
      Map<String, Object> header = new HashMap<>(1);
      header.put(Header.TYPE, Header.JWT_TYPE);
      builder.setHeader(header);
      String base64EncodedJWT = builder.compact();
      requestValues.put(GRANT_TYPE_KEY, Collections.singletonList(JWT_GRANT_TYPE));
      requestValues.put(ASSERTION_KEY, Collections.singletonList(base64EncodedJWT));
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  public void reInit(Client webClient) throws AuthenticationFailureException, IOException, StageException { // NOSONAR
    filter.setShouldInsertHeader(false); // don't insert the header for requests to get new tokens.
    if (credentialsGrantType == OAuth2GrantTypes.JWT) {
      // Set current time in EL Vars to resolve EL correctly
      TimeNowEL.setTimeNowInContext(elVars, new Date());
      TimeEL.setCalendarInContext(elVars, Calendar.getInstance());
    }
    try {
      String newToken = obtainAccessToken(webClient);
      filter.setAuthToken(parseAccessToken(newToken));
    } finally {
      filter.setShouldInsertHeader(true);
    }
  }
}
