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

import org.junit.Assert;
import org.junit.Test;
import org.springframework.security.oauth2.common.DefaultOAuth2AccessToken;
import org.springframework.security.oauth2.common.DefaultOAuth2RefreshToken;
import org.springframework.security.oauth2.common.OAuth2AccessToken;

import java.io.File;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;

public class TestEngineClientTokenServices {

  @Test
  public void testEngineClientTokenServices() {
    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");

    EngineClientTokenServices services = new EngineClientTokenServices(file);

    // no tokens
    Assert.assertFalse(services.hasTokens());

    Instant expires = Instant.now().plusSeconds(10);
    AsterTokenResponse response = new AsterTokenResponse().setAccess_token("AT")
        .setRefresh_token("RT")
        .setExpires_in(10)
        .setExpires_on(expires.getEpochSecond());

    // save registration
    services.saveRegistrationToken(response);

    // yes tokens
    Assert.assertTrue(services.hasTokens());

    // get tokens
    OAuth2AccessToken token = services.getAccessToken(null, null);
    Assert.assertNotNull(token);
    Assert.assertEquals("AT", token.getValue());
    Assert.assertEquals("RT", token.getRefreshToken().getValue());
    Assert.assertTrue(token.getExpiresIn() > 8 && token.getExpiresIn() <= 10);
    Assert.assertEquals("Bearer", token.getTokenType());

    // save tokens
    expires = Instant.now().plusSeconds(100);
    DefaultOAuth2AccessToken dToken = new DefaultOAuth2AccessToken("ATX");
    dToken.setRefreshToken(new DefaultOAuth2RefreshToken("RTX"));
    dToken.setExpiration(Date.from(expires));
    services.saveAccessToken(null, null, dToken);

    // get saved tokens
    token = services.getAccessToken(null, null);
    Assert.assertNotNull(token);
    Assert.assertEquals("ATX", token.getValue());
    Assert.assertEquals("RTX", token.getRefreshToken().getValue());
    Assert.assertTrue(token.getExpiresIn() > 98 && token.getExpiresIn() <= 100);
    Assert.assertEquals("Bearer", token.getTokenType());

    // remove tokens
    services.removeAccessToken(null, null);

    // no tokens
    Assert.assertFalse(services.hasTokens());
  }

}
