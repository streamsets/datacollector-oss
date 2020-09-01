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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;

/**
 * Bean that contains all the URI and parameters required to initiate an OAuth2 authorization grant request using PKCE
 * to Aster to obtain the access token and refresh tokens for the engine, or to obtain the user login (to the engine)
 * access token.
 * <p/>
 * This bean is JSON serialized and returned to the engien client (typically a browser) so it can initiate the Aster
 * OAuth2 request.
 */
public class AsterAuthorizationRequest {

  /**
   * Bean that contains Aster OAuth2 authorization grant request parameters.
   * <p/>
   * The property names are such that their JSON serialization produces OAuth2 parameters names.
   */
  public static class Parameters {
    private String client_id;
    private String response_type;
    private String redirect_uri;
    private String scope;
    private String state;
    private String code_challenge;
    private String code_challenge_method;
    private String client_type;
    private String client_version;

    /**
     * Convenience method that asserts all required parameters are set.
     */
    private void assertValid() {
      Preconditions.checkNotNull(client_id, "client_id");
      Preconditions.checkNotNull(response_type, "response_type");
      Preconditions.checkNotNull(redirect_uri, "redirect_uri");
      Preconditions.checkNotNull(scope, "scope");
      Preconditions.checkNotNull(state, "state");
      Preconditions.checkNotNull(code_challenge, "code_challenge");
      Preconditions.checkNotNull(code_challenge_method, "code_challenge_method");
      Preconditions.checkNotNull(client_type, "client_type");
      Preconditions.checkNotNull(client_version, "client_version");
    }

    public String getClient_id() {
      return client_id;
    }

    public Parameters setClient_id(String client_id) {
      this.client_id = client_id;
      return this;
    }

    public String getResponse_type() {
      return response_type;
    }

    public Parameters setResponse_type(String response_type) {
      this.response_type = response_type;
      return this;
    }

    public String getRedirect_uri() {
      return redirect_uri;
    }

    public Parameters setRedirect_uri(String redirect_uri) {
      this.redirect_uri = redirect_uri;
      return this;
    }

    public String getScope() {
      return scope;
    }

    public Parameters setScope(String scope) {
      this.scope = scope;
      return this;
    }

    public String getState() {
      return state;
    }

    public Parameters setState(String state) {
      this.state = state;
      return this;
    }

    public String getCode_challenge() {
      return code_challenge;
    }

    public Parameters setCode_challenge(String code_challenge) {
      this.code_challenge = code_challenge;
      return this;
    }

    public String getCode_challenge_method() {
      return code_challenge_method;
    }

    public Parameters setCode_challenge_method(String code_challenge_method) {
      this.code_challenge_method = code_challenge_method;
      return this;
    }

    public String getClient_type() {
      return client_type;
    }

    public Parameters setClient_type(String client_type) {
      this.client_type = client_type;
      return this;
    }

    public String getClient_version() {
      return client_version;
    }

    public Parameters setClient_version(String client_version) {
      this.client_version = client_version;
      return this;
    }
  }

  private String authorizeUri;
  private Parameters parameters;
  private String localState;
  private String challengeVerifier;

  /**
   * Convenience method that asserts all required properties are set.
   */
  public AsterAuthorizationRequest assertValid() {
    Preconditions.checkNotNull(authorizeUri, "authorizeUri");
    Preconditions.checkNotNull(parameters, "parameters");
    Preconditions.checkNotNull(localState, "localState");
    Preconditions.checkNotNull(challengeVerifier, "challengeVerifier");
    parameters.assertValid();
    return this;
  }

  /**
   * Returns the Aster OAuth2 Authorize endpoint URL.
   */
  public String getAuthorizeUri() {
    return authorizeUri;
  }

  /**
   * Sets the Aster OAuth2 Authorize endpoint URL.
   */
  public AsterAuthorizationRequest setAuthorizeUri(String authorizeUri) {
    this.authorizeUri = authorizeUri;
    return this;
  }

  /**
   * Returns the Aster OAuth2 Authorize parameters.
   */
  public Parameters getParameters() {
    return parameters;
  }

  /**
   * Sets the Aster OAuth2 Authorize parameters.
   */
  public AsterAuthorizationRequest setParameters(Parameters parameters) {
    this.parameters = parameters;
    return this;
  }

  /**
   * Returns a local state used to cache locally some local state, typically the requested URL that triggered
   * the authorization request.
   * <p/>
   * This property is not JSON serialized.
   */
  @JsonIgnore
  public String getLocalState() {
    return localState;
  }

  /**
   * Sets a local state used to cache locally some local state, typically the requested URL that triggered
   * the authorization request.
   * <p/>
   * This property is not JSON serialized.
   */
  public AsterAuthorizationRequest setLocalState(String localState) {
    this.localState = localState;
    return this;
  }

  /**
   * Returns the PKCE verifier to use for completing the OAuth2 auhorization grant.
   * <p/>
   * This property is not JSON serialized.
   */
  @JsonIgnore
  public String getChallengeVerifier() {
    return challengeVerifier;
  }

  /**
   * Sets the PKCE verifier to use for completing the OAuth2 auhorization grant.
   * <p/>
   * This property is not JSON serialized.
   */
  public AsterAuthorizationRequest setChallengeVerifier(String challengeVerifier) {
    this.challengeVerifier = challengeVerifier;
    return this;
  }

}
