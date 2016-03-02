/**
 * Copyright 2016 StreamSets Inc.
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

package com.streamsets.lib.security.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;

public class PlainSSOTokenGenerator implements SSOTokenGenerator {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().disable(SerializationFeature.INDENT_OUTPUT);

  @Override
  public String getType() {
    return PlainSSOTokenParser.TYPE;
  }

  @Override
  public String getVerificationData() {
    return null;
  }

  protected String generateData(SSOUserPrincipal principal) {
    try {
      String data = OBJECT_MAPPER.writeValueAsString(SSOUserPrincipalImpl.toMap(principal));
      return Base64.encodeBase64String(data.getBytes());
    } catch (IOException ex) {
      throw new RuntimeException(Utils.format("Should never happen: {}", ex.toString(), ex));
    }
  }

  @Override
  public String generate(SSOUserPrincipal principal) {
    return getType() + SSOConstants.TOKEN_PART_SEPARATOR + generateData(principal);
  }

}
