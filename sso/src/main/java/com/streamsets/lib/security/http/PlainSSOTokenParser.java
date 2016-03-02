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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class PlainSSOTokenParser implements SSOTokenParser {
  private static final Logger LOG = LoggerFactory.getLogger(PlainSSOTokenParser.class);

  public static final String TYPE = "plain";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().disable(SerializationFeature.INDENT_OUTPUT);

  protected Logger getLog() {
    return LOG;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  protected String getHead(String token) throws IOException {
    int idx = token.indexOf(SSOConstants.TOKEN_PART_SEPARATOR);
    return (idx > -1) ? token.substring(0, idx) : null;
  }

  protected String getTail(String token) throws IOException {
    int idx = token.indexOf(SSOConstants.TOKEN_PART_SEPARATOR);
    return (idx > -1) ? token.substring(idx + 1) : null;
  }

  @SuppressWarnings("unchecked")
  protected SSOUserPrincipal parsePrincipal(String tokenStr, String dataB64) throws IOException {
    SSOUserPrincipal token = null;
    try {
      byte[] data = Base64.decodeBase64(dataB64);
      Map<String, ?> map = OBJECT_MAPPER.readValue(data, Map.class);
      token = SSOUserPrincipalImpl.fromMap(tokenStr, map);
    } catch (IOException ex) {
      LOG.warn("Could not parse token payload: {}", ex.toString(), ex);
    }
    return token;
  }

  @Override
  public void setVerificationData(String data) {
    //NOP
  }

  @Override
  public SSOUserPrincipal parse(String tokenStr) throws IOException {
    Utils.checkNotNull(tokenStr, "tokenStr");
    SSOUserPrincipal token = null;
    String type = getHead(tokenStr);
    if (type == null) {
      getLog().warn("Invalid token '{}', cannot get type for token", tokenStr);
    } else {
      if (getType().equals(type)) {
        String payload = getTail(tokenStr);
        if (payload == null) {
          getLog().warn("Invalid token '{}', cannot get payload", tokenStr);
        }
        token = parsePrincipal(tokenStr, payload);
      } else {
        getLog().warn("Invalid token type '{}', parser expects type '{}'", type, getType());
      }
    }
    return token;
  }

}
