/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.datacollector.event.client.impl;

import com.streamsets.lib.security.http.DpmClientInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientResponseContext;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class TestMovedDpmJerseyClientFilter {

  @Test
  public void testFilter() throws IOException {
    DpmClientInfo clientInfo = Mockito.mock(DpmClientInfo.class);
    MovedDpmJerseyClientFilter filter = new MovedDpmJerseyClientFilter(clientInfo);
    filter = Mockito.spy(filter);

    ClientRequestContext request = Mockito.mock(ClientRequestContext.class);
    ClientResponseContext response = Mockito.mock(ClientResponseContext.class);

    // NO 308 status
    filter.filter(request, response);
    Mockito.verifyZeroInteractions(clientInfo);
    Mockito.verify(filter, Mockito.never()).repeatRequest(Mockito.eq(request), Mockito.eq(response));
  }

  @Test
  public void testRepeat() throws Exception {
    DpmClientInfo clientInfo = Mockito.mock(DpmClientInfo.class);
    MovedDpmJerseyClientFilter filter = new MovedDpmJerseyClientFilter(clientInfo);
    filter = Mockito.spy(filter);

    ClientRequestContext request = Mockito.mock(ClientRequestContext.class);
    ClientResponseContext response = Mockito.mock(ClientResponseContext.class);

    MultivaluedMap<String, String> responseHeaders = Mockito.mock(MultivaluedMap.class);
    Mockito.when(response.getHeaders()).thenReturn(responseHeaders);

    Client client = Mockito.mock(Client.class);
    Mockito.doReturn(client).when(filter).createClient(Mockito.eq(request));

    Mockito.when(request.getMethod()).thenReturn("POST");

    Mockito.when(request.getMediaType()).thenReturn(MediaType.APPLICATION_JSON_TYPE);

    URI lUri = new URI("http://old/bar?x=a");
    Mockito.when(request.getUri()).thenReturn(lUri);

    Mockito.when(clientInfo.getDpmBaseUrl()).thenReturn("http://new/");

    WebTarget resourceTarget = Mockito.mock(WebTarget.class);
    Mockito.when(client.target(Mockito.eq("http://new/bar?x=a"))).thenReturn(resourceTarget);

    Invocation.Builder builder = Mockito.mock(Invocation.Builder.class);
    Mockito.when(resourceTarget.request(Mockito.eq(MediaType.APPLICATION_JSON_TYPE))).thenReturn(builder);

    MultivaluedMap<String, Object> requestHeaders = Mockito.mock(MultivaluedMap.class);
    Mockito.when(request.getHeaders()).thenReturn(requestHeaders);

    Object entity = new Object();
    Mockito.when(request.getEntity()).thenReturn(entity);

    Invocation invocation = Mockito.mock(Invocation.class);

    ArgumentCaptor<Entity> entityCaptor = ArgumentCaptor.forClass(Entity.class);
    Mockito.when(builder.build(Mockito.eq("POST"), entityCaptor.capture())).thenReturn(invocation);

    Response nextResponse = Mockito.mock(Response.class);
    Mockito.when(invocation.invoke()).thenReturn(nextResponse);
    Mockito.when(nextResponse.hasEntity()).thenReturn(true);
    InputStream inputStream = Mockito.mock(InputStream.class);
    Mockito.when(nextResponse.readEntity(Mockito.eq(InputStream.class))).thenReturn(inputStream);

    MultivaluedMap<String, String> nextResponseHeaders = Mockito.mock(MultivaluedMap.class);
    Mockito.when(nextResponse.getStringHeaders()).thenReturn(nextResponseHeaders);
    Mockito.when(nextResponse.getStatus()).thenReturn(200);

    filter.repeatRequest(request, response);

    Assert.assertEquals(entity, entityCaptor.getValue().getEntity());
    Assert.assertEquals(MediaType.APPLICATION_JSON_TYPE, entityCaptor.getValue().getMediaType());

    Mockito.verify(response, Mockito.times(1)).setEntityStream(Mockito.eq(inputStream));
    Mockito.verify(response, Mockito.times(1)).getHeaders();
    Mockito.verify(responseHeaders, Mockito.times(1)).clear();
    Mockito.verify(responseHeaders, Mockito.times(1)).putAll(Mockito.eq((MultivaluedMap) nextResponseHeaders));
    Mockito.verify(response, Mockito.times(1)).setStatus(Mockito.eq(200));

  }


}
