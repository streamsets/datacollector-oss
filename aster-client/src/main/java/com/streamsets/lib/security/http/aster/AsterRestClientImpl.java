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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.codec.Hex;
import org.springframework.security.oauth2.client.DefaultOAuth2ClientContext;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.client.token.AccessTokenProviderChain;
import org.springframework.security.oauth2.client.token.grant.code.AuthorizationCodeAccessTokenProvider;
import org.springframework.security.oauth2.client.token.grant.code.AuthorizationCodeResourceDetails;
import org.springframework.security.oauth2.common.AuthenticationScheme;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.security.oauth2.common.util.RandomValueStringGenerator;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Aster Rest Client.
 * <p/>
 * Provides a preconfigured Spring Boot {@link RestTemplate} instance to make OAuth2 authorized
 * REST calls to Aster.
 * <p/>
 * Provides functionality to enable OAUth2 code grant to obtain OAuth2 tokens, as well as the logic
 * to automatically refresh then once a refresh token is available.
 */
public class AsterRestClientImpl implements AsterRestClient {
  @VisibleForTesting
  static final String ASTER_OAUTH_AUTHORIZE_PATH = "/login/oauth/access_token/elogin";
  @VisibleForTesting
  static final String ASTER_OAUTH_TOKEN_PATH = "/api/security/oauth/token";

  private static class EngineSpringBootAuthentication implements Authentication {
    private final String name;

    public EngineSpringBootAuthentication(String name) {
      this.name = name;
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
      return Collections.emptySet();
    }

    @Override
    public Object getCredentials() {
      return null;
    }

    @Override
    public Object getDetails() {
      return this;
    }

    @Override
    public Object getPrincipal() {
      return this;
    }

    @Override
    public boolean isAuthenticated() {
      return true;
    }

    @Override
    public void setAuthenticated(boolean isAuthenticated) throws IllegalArgumentException {

    }

    @Override
    public String getName() {
      return name;
    }
  }

  private final AsterRestConfig config;
  private final Authentication authentication;
  private final RandomValueStringGenerator randomValueStringGenerator;
  private final Cache<String, AsterAuthorizationRequest> requestsCache;
  private final EngineClientTokenServices tokenServices;

  /**
   * Constructor.
   */
  public AsterRestClientImpl(AsterRestConfig config, File tokensFile) {
    Preconditions.checkNotNull(config, "config");
    Preconditions.checkNotNull(tokensFile, "tokensFile");
    this.config = config.assertValid();
    authentication = new EngineSpringBootAuthentication(config.getClientId());

    randomValueStringGenerator = new RandomValueStringGenerator();
    requestsCache = CacheBuilder.newBuilder().expireAfterWrite(config.getStateCacheExpirationSecs(), TimeUnit.SECONDS).build();
    tokenServices = new EngineClientTokenServices(config.getClientId(), tokensFile);
  }

  /**
   * Returns the Aster REST client configuration.
   */
  public AsterRestConfig getConfig() {
    return config;
  }

  @Override
  public boolean hasTokens() {
    return tokenServices.hasTokens();
  }

  /**
   * Computes a PKCE challenge using S256 method.
   */
  @VisibleForTesting
  String computeChallenge(String challengeVerifier) {
    Preconditions.checkNotNull(challengeVerifier, "challengeVerifier");
    return Base64.getUrlEncoder()
        .encodeToString(new String(Hex.encode(DigestUtils.sha256(challengeVerifier))).getBytes());
  }

  private synchronized AsterAuthorizationRequest createAuthorizationRequest(
      AsterRestConfig.SubjectType subjectType,
      String callbackUri,
      String localState
  ) {
    String state = randomValueStringGenerator.generate();
    // we make sure we didn't generate a random string already in the cache.
    while (requestsCache.getIfPresent(state) != null) {
      state = randomValueStringGenerator.generate();
    }

    String challengeVerifier = randomValueStringGenerator.generate();
    AsterAuthorizationRequest request = new AsterAuthorizationRequest();
    request.setAuthorizeUri(config.getAsterUrl() + ASTER_OAUTH_AUTHORIZE_PATH);
    AsterAuthorizationRequest.Parameters parameters = new AsterAuthorizationRequest.Parameters()
        .setClient_id(config.getClientId())
        .setClient_type(subjectType.name())
        .setClient_version(config.getClientVersion())
        .setRedirect_uri(callbackUri)
        .setResponse_type("code")
        .setScope("DPLANE")
        .setState(state)
        .setCode_challenge(computeChallenge(challengeVerifier))
        .setCode_challenge_method("S256");
    request.setParameters(parameters);
    request.setLocalState(localState);
    request.setChallengeVerifier(challengeVerifier);

    // storing the request in the cache.
    requestsCache.put(state, request.assertValid());
    return request;
  }

  /**
   * Creates an engine authorization request to obtain Aster access and refresh tokens for the engine.
   */
  public AsterAuthorizationRequest createEngineAuthorizationRequest(String baseUrl, String localState) {
    Preconditions.checkNotNull(baseUrl, "bseUrl");
    Preconditions.checkNotNull(localState, "localState");
    return createAuthorizationRequest(config.getSubjectType(), baseUrl + config.getRegistrationCallbackPath(), localState);
  }

  /**
   * Creates an user authorization request to obtain User information for a user logging in.
   */
  public AsterAuthorizationRequest createUserAuthorizationRequest(String baseUrl, String localState) {
    Preconditions.checkNotNull(baseUrl, "bseUrl");
    Preconditions.checkNotNull(localState, "localState");
    return createAuthorizationRequest(AsterRestConfig.SubjectType.USER, baseUrl + config.getLoginCallbackPath(), localState);
  }

  /**
   * Retrieves an authorization request based on its state.
   * <p/>
   * Used to complete a token request (registration and user login).
   * <p/>
   * The authorization request is cached with a time expiration.
   */
  public synchronized AsterAuthorizationRequest findRequest(String state) {
    Preconditions.checkNotNull(state, "state");
    AsterAuthorizationRequest request = requestsCache.getIfPresent(state);
    if (request != null) {
      requestsCache.invalidate(state);
    } else {
      throw new AsterAuthException(String.format("State '%s' not found in cache, it could have expired", state));
    }
    return request;
  }

  @VisibleForTesting
  RestTemplate createRestTemplate() {
    return new RestTemplate();
  }

  @VisibleForTesting
  AsterTokenResponse requestToken(AsterAuthorizationRequest request, String code) {
    Preconditions.checkNotNull(request, "request");
    Preconditions.checkNotNull(code, "code");
    RestTemplate restTemplate = createRestTemplate();
    restTemplate.setMessageConverters(ImmutableList.of(
        new FormHttpMessageConverter(),
        new MappingJackson2HttpMessageConverter()
    ));
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("client_id", config.getClientId());
    map.add("client_type", request.getParameters().getClient_type());
    map.add("grant_type", "authorization_code");
    map.add("code_verifier", request.getChallengeVerifier());
    map.add("code", code);
    HttpEntity<MultiValueMap<String, String>> httpRequest = new HttpEntity<>(map, new HttpHeaders());
    ResponseEntity<AsterTokenResponse> httpResponse = restTemplate.exchange(
        config.getAsterUrl() + ASTER_OAUTH_TOKEN_PATH,
        HttpMethod.POST,
        httpRequest,
        AsterTokenResponse.class
    );
    if (!httpResponse.getStatusCode().is2xxSuccessful()) {
      tokenServices.removeAccessToken(null, null);
      throw new AsterAuthException(String.format(
          "Token request to '%s' for client_type '%s' failed with status code '%s', response headers '%s'",
          config.getAsterUrl() + ASTER_OAUTH_TOKEN_PATH,
          request.getParameters().getClient_type(),
          httpResponse.getStatusCode(),
          httpRequest.getHeaders()
      ));
    }
    return httpResponse.getBody();
  }

  @VisibleForTesting
  String getTokenOrg(String tokenStr) {
    Preconditions.checkNotNull(tokenStr, "tokenStr");
    try {
      JWTClaimsSet claims = SignedJWT.parse(tokenStr).getJWTClaimsSet();
      String org = claims.getStringClaim("t_o");
      if (org == null) {
        throw new AsterException("Token must have an org");
      }
      return org;
    } catch (ParseException ex) {
      throw new AsterException(String.format("Could not extract org information from stored tokens: %s", ex), ex);
    }
  }

  @VisibleForTesting
  String getEngineOrg() {
    String org = null;
    if (hasTokens()) {
      OAuth2AccessToken token = tokenServices.getAccessToken(null, null);
      return getTokenOrg(token.getValue());
    }
    return org;
  }

  /**
   * Completes the registration of an engine with Aster.
   */
  public void registerEngine(AsterAuthorizationRequest request, String code) {
    Preconditions.checkNotNull(request, "request");
    Preconditions.checkNotNull(code, "code");

    AsterTokenResponse response = requestToken(request, code);

    // verify ORG remains the same
    String tokenOrg = getTokenOrg(response.getAccess_token());
    String engineOrg = getEngineOrg();
    if (engineOrg != null && !engineOrg.equals(tokenOrg)) {
      throw new AsterAuthException(String.format("Engine org '%s' differs from new token org '%s'", engineOrg, tokenOrg));
    }

    tokenServices.saveRegistrationToken(response);
  }

  /**
   * Completes the Aster engine user login returning the user information for the engine to create the
   * local user session.
   */
  public AsterUser getUserInfo(AsterAuthorizationRequest request, String code) {
    Preconditions.checkNotNull(request, "request");
    Preconditions.checkNotNull(code, "code");

    AsterTokenResponse response = requestToken(request, code);

    String engineOrg = getEngineOrg();

    JWTClaimsSet claims;
    try {
      claims = SignedJWT.parse(response.getAccess_token()).getJWTClaimsSet();
      String type = claims.getStringClaim("t_type");
      if (!"USER".equals(type)) {
        throw new AsterAuthException(String.format("Access token should be for USER, is for '%s'", type));
      }
      String email = claims.getStringClaim("t_email");
      String org  = claims.getStringClaim("t_o");

      // verify engine ORG and user ORG matches
      if (!engineOrg.equals(org)) {
        throw new AsterAuthException(String.format(
            "Engine org '%s' differs from user '%s' org '%s'",
            engineOrg,
            email,
            org
        ));
      }

      Set<String> roles = ImmutableSet.copyOf(claims.getStringListClaim("t_e_roles"));
      Set<String> groups = ImmutableSet.copyOf(claims.getStringListClaim("t_e_groups"));
      return new AsterUser().setName(email).setOrg(org).setRoles(roles).setGroups(groups).assertValid();
    } catch (ParseException ex) {
      throw new AsterAuthException(String.format("Could not extract user information from access token: %s", ex), ex);
    }
  }

  /**
   * Creates a RestTemplate configured with Aster OAuth2 access and refresh Tokens that automatically does refreshes
   * when needed.
   */
  @VisibleForTesting
  RestTemplate createOAuth2RestTemplate() {
    // engine resource details, used locally only
    AuthorizationCodeResourceDetails details = new AuthorizationCodeResourceDetails();
    details.setId("aster");
    details.setClientId(config.getClientId());
    details.setAccessTokenUri(config.getAsterUrl() + ASTER_OAUTH_TOKEN_PATH);
    details.setScope(Arrays.asList());
    details.setUseCurrentUri(false);
    details.setClientAuthenticationScheme(AuthenticationScheme.form);
    details.setGrantType("authorization_code");

    // wiring authorization code token provider to be able to get new access and refresh tokens
    AuthorizationCodeAccessTokenProvider accessTokenProvider = new AuthorizationCodeAccessTokenProvider();
    AccessTokenProviderChain tokenProvider = new AccessTokenProviderChain(ImmutableList.of(accessTokenProvider));
    tokenProvider.setClientTokenServices(tokenServices);

    // setting the client context using a dummy SpringBoot authentication instance for the engine.
    DefaultOAuth2ClientContext clientContext = new DefaultOAuth2ClientContext();
    clientContext.setAccessToken(tokenServices.getAccessToken(null, authentication));

    // creating OAuth2 aware RestTemplate, it gets new access/refresh tokens as needed
    // hard precondition: refresh token is still valid.
    OAuth2RestTemplate oAuth2RestTemplate = new OAuth2RestTemplate(details, clientContext);

    // we add the payload converter for JSON as it is commonly used for JSON REST calls
    oAuth2RestTemplate.setMessageConverters(ImmutableList.of(
        new MappingJackson2HttpMessageConverter()
    ));

    // setting the token provider.
    oAuth2RestTemplate.setAccessTokenProvider(tokenProvider);

    return oAuth2RestTemplate;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <I, O> Response<O> doRestCall(Request<I> request) {
    try {
      // we need to set the SB SecurityContext to an Authentication with the client ID as Principal ID
      // as we are not using SpringBoot in the engine for anything else, this is fine.
      SecurityContextHolder.getContext().setAuthentication(authentication);
      HttpEntity httpEntity = new HttpEntity<>(request.getPayload());
      Class<?> requestPayloadCls = (request.getPayloadClass() != null) ? request.getPayloadClass() : Void.class;
      RestTemplate restTemplate = createOAuth2RestTemplate();
      if (request.getTimeout() >= 0) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(request.getTimeout());
        requestFactory.setReadTimeout(request.getTimeout());
        restTemplate.setRequestFactory(requestFactory);
      }
      ResponseEntity responseEntity = restTemplate.exchange(
          request.getResourcePath(),
          HttpMethod.valueOf(request.getRequestType().name()),
          httpEntity,
          requestPayloadCls
      );
      return (Response<O>) new Response<>().setBody(responseEntity.getBody())
          .setHeaders(responseEntity.getHeaders().toSingleValueMap())
          .setStatusCode(responseEntity.getStatusCodeValue());
    } finally {
      SecurityContextHolder.getContext().setAuthentication(null);
    }
  }

}
