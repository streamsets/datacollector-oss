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

import com.streamsets.pipeline.api.Label;

public enum  SigningAlgorithms implements Label {

  HS256("HMAC using SHA-256"),
  HS384("HMAC using SHA-384"),
  HS512("HMAC using SHA-512"),
  RS256("RSASSA-PKCS-v1_5 using SHA-256"),
  RS384("RSASSA-PKCS-v1_5 using SHA-384"),
  RS512("RSASSA-PKCS-v1_5 using SHA-512"),
// The following require BouncyCastle, so ignore for now.
//  PS256("RSASSA-PSS using SHA-256 and MGF1 with SHA-256"),
//  PS384("RSASSA-PSS using SHA-384 and MGF1 with SHA-384"),
//  PS512("RSASSA-PSS using SHA-512 and MGF1 with SHA-512"),
//  ES256("ECDSA using P-256 and SHA-256"),
//  ES384("ECDSA using P-384 and SHA-384"),
//  ES512("ECDSA using P-512 and SHA-512"),
  NONE("None")
  ;

  private final String label;

  SigningAlgorithms(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
