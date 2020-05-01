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
package com.streamsets.datacollector.util;

import com.streamsets.datacollector.activation.Activation;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import static org.mockito.Mockito.mock;

public class TestActivationUtil {
  @Test
  public void testEnabledActivation() {
    Activation activation = mock(Activation.class);
    Mockito.when(activation.isEnabled()).thenReturn(false);
    Assert.assertEquals(-1, ActivationUtil.getMaxExecutors(activation));
  }

  @Test
  public void testInvalidActivation() {
    Activation activation = mock(Activation.class);
    Mockito.when(activation.isEnabled()).thenReturn(true);

    Activation.Info info = mock(Activation.Info.class);
    Mockito.when(info.isValid()).thenReturn(false);
    Mockito.when(activation.getInfo()).thenReturn(info);

    Assert.assertEquals(ActivationUtil.defaultMaxExecutors, ActivationUtil.getMaxExecutors(activation));
  }

  @Test
  public void testValidActivation() {
    int maxExecutors = 1000;
    Activation activation = mock(Activation.class);
    Mockito.when(activation.isEnabled()).thenReturn(true);

    Activation.Info info = mock(Activation.Info.class);
    Mockito.when(info.isValid()).thenReturn(true);
    Mockito.when(info.getAdditionalInfo())
        .thenReturn(ImmutableMap.of(ActivationUtil.sparkMaxExecutorsParamName, maxExecutors));
    Mockito.when(activation.getInfo()).thenReturn(info);

    Assert.assertEquals(maxExecutors, ActivationUtil.getMaxExecutors(activation));
  }

  @Test
  public void testInvalidActivationInfo() {
    String maxExecutors = "invalid";
    Activation activation = mock(Activation.class);
    Mockito.when(activation.isEnabled()).thenReturn(true);

    Activation.Info info = mock(Activation.Info.class);
    Mockito.when(info.isValid()).thenReturn(true);
    Mockito.when(info.getAdditionalInfo())
        .thenReturn(ImmutableMap.of(ActivationUtil.sparkMaxExecutorsParamName, maxExecutors));
    Mockito.when(activation.getInfo()).thenReturn(info);

    Assert.assertEquals(ActivationUtil.defaultMaxExecutors, ActivationUtil.getMaxExecutors(activation));
  }
}
