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
package com.streamsets.lib.security.http;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class CredentialsBeanJson {

  private final String token;
  private final String principal;
  private final String keytab;
  private final String tokenSignature;
  private final String dpmUrl;
  private final List<String> labels;
  private final String deploymentId;

  public CredentialsBeanJson(
      @JsonProperty("token") String token,
      @JsonProperty("principal") String principal,
      @JsonProperty("keytab") String keytab,
      @JsonProperty("tokenSignature") String tokenSignature,
      @JsonProperty("dpmUrl") String dpmUrl,
      @JsonProperty("labels") List<String> labels,
      @JsonProperty("deploymentId") String deploymentId
  ) {
    this.token = token;
    this.principal = principal;
    this.keytab = keytab;
    this.tokenSignature = tokenSignature;
    this.dpmUrl = dpmUrl;
    this.labels = labels;
    this.deploymentId = deploymentId;
  }


  public String getToken() {
    return token;
  }


  public String getKeytab() {
    return keytab;
  }


  public String getPrincipal() {
    return principal;
  }


  public String getTokenSignature() {
    return tokenSignature;
  }


  public String getDpmUrl() {
    return dpmUrl;
  }


  public List<String> getLabels() {
    return labels;
  }

  public String getDeploymentId() {
    return deploymentId;
  }
}
