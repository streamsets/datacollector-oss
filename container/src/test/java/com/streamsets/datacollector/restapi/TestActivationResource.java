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
package com.streamsets.datacollector.restapi;

import com.streamsets.datacollector.activation.Activation;
import com.streamsets.datacollector.usagestats.StatsCollector;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import javax.ws.rs.core.Response;

public class TestActivationResource {

  @Test
  public void testActivationResourceStatsCollectorNotOpted() throws Exception {
    Activation activation = Mockito.mock(Activation.class);
    StatsCollector statsCollector = Mockito.mock(StatsCollector.class);
    Mockito.when(statsCollector.isOpted()).thenReturn(false);
    ActivationResource resource = new ActivationResource(activation, statsCollector);

    Response response1 = resource.updateActivation("");
    Assert.assertEquals(Response.Status.NOT_IMPLEMENTED.getStatusCode(), response1.getStatus());

    Mockito.when(activation.isEnabled()).thenReturn(true);
    Response response2 = resource.updateActivation("");
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response2.getStatus());
    Mockito.verify(activation).setActivationKey(Mockito.anyString());
    Mockito.verify(statsCollector).setActive(true);
  }

  @Test
  public void testActivationResourceStatsCollectorOpted() throws Exception {
    Activation activation = Mockito.mock(Activation.class);
    StatsCollector statsCollector = Mockito.mock(StatsCollector.class);
    Mockito.when(statsCollector.isOpted()).thenReturn(true);

    ActivationResource resource = new ActivationResource(activation, statsCollector);

    Response response1 = resource.updateActivation("");
    Assert.assertEquals(Response.Status.NOT_IMPLEMENTED.getStatusCode(), response1.getStatus());

    Mockito.when(activation.isEnabled()).thenReturn(true);
    Response response2 = resource.updateActivation("");
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response2.getStatus());
    Mockito.verify(activation).setActivationKey(Mockito.anyString());
    Mockito.verify(statsCollector, Mockito.never()).setActive(Mockito.anyBoolean());
  }

  @Test
  public void testActivationResourceStatsCollectorNoOptLicenseType() throws Exception {
    Activation activation = Mockito.mock(Activation.class);
    StatsCollector statsCollector = Mockito.mock(StatsCollector.class);
    Mockito.when(statsCollector.isOpted()).thenReturn(false);

    Activation.Info mockInfo = Mockito.mock(Activation.Info.class);
    Mockito.when(mockInfo.getAdditionalInfo()).thenReturn(ImmutableMap.of(ActivationResource.LICENSE_TYPE, "NONTRIAL"));

    ActivationResource resource = new ActivationResource(activation, statsCollector);
    Mockito.when(activation.isEnabled()).thenReturn(true);



    Mockito.when(activation.getInfo()).thenReturn(mockInfo);
    Response response = resource.updateActivation("");
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Mockito.verify(activation).setActivationKey(Mockito.anyString());
    Mockito.verify(statsCollector, Mockito.never()).setActive(Mockito.anyBoolean());
  }

  @Test
  public void testActivationResourceStatsCollectorOptTrial() throws Exception {
    Activation activation = Mockito.mock(Activation.class);
    StatsCollector statsCollector = Mockito.mock(StatsCollector.class);
    Mockito.when(statsCollector.isOpted()).thenReturn(false);

    Activation.Info mockInfo = Mockito.mock(Activation.Info.class);
    Mockito.when(mockInfo.getAdditionalInfo()).thenReturn(ImmutableMap.of(ActivationResource.LICENSE_TYPE, "TRIAL"));

    ActivationResource resource = new ActivationResource(activation, statsCollector);
    Mockito.when(activation.isEnabled()).thenReturn(true);

    Mockito.when(activation.getInfo()).thenReturn(mockInfo);
    Response response = resource.updateActivation("");
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Mockito.verify(activation).setActivationKey(Mockito.anyString());
    Mockito.verify(statsCollector).setActive(true);
  }

  @Test
  public void testActivationResourceStatsCollectorOptRegistration() throws Exception {
    Activation activation = Mockito.mock(Activation.class);
    StatsCollector statsCollector = Mockito.mock(StatsCollector.class);
    Mockito.when(statsCollector.isOpted()).thenReturn(false);

    Activation.Info mockInfo = Mockito.mock(Activation.Info.class);
    Mockito.when(mockInfo.getAdditionalInfo()).thenReturn(ImmutableMap.of(ActivationResource.LICENSE_TYPE, "REGISTRATION"));

    ActivationResource resource = new ActivationResource(activation, statsCollector);
    Mockito.when(activation.isEnabled()).thenReturn(true);

    Mockito.when(activation.getInfo()).thenReturn(mockInfo);
    Response response = resource.updateActivation("");
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Mockito.verify(activation).setActivationKey(Mockito.anyString());
    Mockito.verify(statsCollector).setActive(true);
  }
}
