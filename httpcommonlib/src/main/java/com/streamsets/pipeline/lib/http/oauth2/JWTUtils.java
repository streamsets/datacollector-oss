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

import io.jsonwebtoken.SignatureAlgorithm;

public final class JWTUtils {

  private JWTUtils() {
  }

  public static SignatureAlgorithm getSignatureAlgorithm(SigningAlgorithms alg) {
    switch (alg) {
      case HS256:
        return SignatureAlgorithm.HS256;
      case HS384:
        return SignatureAlgorithm.HS384;
      case HS512:
        return SignatureAlgorithm.HS512;
      case RS256:
        return SignatureAlgorithm.RS256;
      case RS384:
        return SignatureAlgorithm.RS384;
      case RS512: // NOSONAR - asking to reduce line count
        return SignatureAlgorithm.RS512;
        // The following are not JDK standard and are difficult to test, so ignoring for now.
//      case PS256:
//        return SignatureAlgorithm.PS256; //NOSONAR
//      case PS384:
//        return SignatureAlgorithm.PS384; //NOSONAR
//      case PS512:
//        return SignatureAlgorithm.PS512; //NOSONAR
//      case ES256:
//        return SignatureAlgorithm.ES256; //NOSONAR
//      case ES384:
//        return SignatureAlgorithm.ES384; //NOSONAR
//      case ES512:
//        return SignatureAlgorithm.ES512; //NOSONAR
      case NONE:
        return SignatureAlgorithm.NONE;
      default:
        throw new IllegalStateException("Unknown Signing Algorithm: " + alg.getLabel());
    }

  }
}
