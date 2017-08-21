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

import com.streamsets.pipeline.api.Stage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class TestOAuth2ConfigBean {

  private final OAuth2TestUtil OAuth2ConfigBeanTestUil = new OAuth2TestUtil();
  private final OAuth2GrantTypes grantType;

  private OAuth2ConfigBean configBean;
  private Client client;
  private Invocation.Builder builder;
  private WebTarget target;
  private Response response;

  @Parameterized.Parameters
  public static Collection<Object> data() {
    return Arrays.asList(new Object[] {
        OAuth2GrantTypes.CLIENT_CREDENTIALS, OAuth2GrantTypes.RESOURCE_OWNER
    });
  }

  public TestOAuth2ConfigBean(OAuth2GrantTypes grantType) {
    this.grantType = grantType;
  }

  @Before
  public void setUp() {
    client = mock(Client.class);
    target = mock(WebTarget.class);
    builder = mock(Invocation.Builder.class);
    response = mock(Response.class);
    configBean = OAuth2ConfigBeanTestUil.setup(client, target, builder, response, grantType);
  }

  @Test
  public void testObtainToken() throws Exception {
    Assert.assertEquals(OAuth2TestUtil.TOKEN_RESPONSE, configBean.obtainAccessToken(client));
    // Since the response mock object is only sent back when we send the correct form, so no need of verifying exact values.
    verify(builder, times(1)).post(any(Entity.class));
    verify(target, times(1)).request();
    verify(client, times(1)).target(anyString());
  }

  @Test
  public void testParseToken() throws Exception {
    Assert.assertEquals(OAuth2TestUtil.TOKEN, configBean.parseAccessToken(OAuth2TestUtil.TOKEN_RESPONSE));
  }


  @Test
  public void testInit() throws Exception {
    configBean.init(mock(Stage.Context.class), new ArrayList<Stage.ConfigIssue>(), client);
    verify(builder, times(1)).post(any(Entity.class));
    verify(target, times(1)).request();
    verify(client, times(1)).target(anyString());
    Assert.assertEquals("Bearer " + OAuth2TestUtil.TOKEN, configBean.filter.authToken);
  }

  @Test
  public void onAccessDenied() throws Exception {
    OAuth2ConfigBean spyConfig = spy(configBean);
    spyConfig.init(mock(Stage.Context.class), new ArrayList<Stage.ConfigIssue>(), client);
    Assert.assertEquals("Bearer " + OAuth2TestUtil.TOKEN, spyConfig.filter.authToken);
    String newToken = "NEW TOKEN";
    String newTokenResponse = "{\n" +
        "  \"token_type\": \"Bearer\",\n" +
        "  \"expires_in\": \"3600\",\n" +
        "  \"ext_expires_in\": \"0\",\n" +
        "  \"expires_on\": \"1484788319\",\n" +
        "  \"not_before\": \"1484784419\",\n" +
        "  \"access_token\": \"" + newToken + "\"\n" +
        "}";
    when(response.readEntity(String.class)).thenReturn(newTokenResponse);
    spyConfig.reInit(client);

    verify(spyConfig, times(2)).obtainAccessToken(client);
    Assert.assertEquals("Bearer " + "NEW TOKEN", spyConfig.filter.authToken);
  }

}
