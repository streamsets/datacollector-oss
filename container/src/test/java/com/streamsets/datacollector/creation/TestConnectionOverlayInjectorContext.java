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
package com.streamsets.datacollector.creation;

import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ErrorCode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestConnectionOverlayInjectorContext {

  ConfigInjector.Context parentContext;
  ConfigInjector.Context context;

  @Before
  public void setup() {
    parentContext = Mockito.mock(ConfigInjector.Context.class);
    context = new ConfigInjector.ConnectionOverlayInjectorContext(parentContext, "prefix.", Collections.emptyList());
  }

  @Test
  public void testConfigDefinition() {
    context.getConfigDefinition("foo");
    Mockito.verify(parentContext).getConfigDefinition("foo");
    Mockito.verifyNoMoreInteractions(parentContext);
  }

  @Test
  public void testGetConfigValue() {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("prop1", "value1"));
    configs.add(new Config("prop2", "value2"));
    context = new ConfigInjector.ConnectionOverlayInjectorContext(parentContext, "prefix.", configs);

    Assert.assertNull(context.getConfigValue("prop1"));
    Assert.assertEquals("value1", context.getConfigValue("prefix.prop1"));
    Assert.assertEquals("value2", context.getConfigValue("prefix.prop2"));
    Mockito.verifyZeroInteractions(parentContext);
  }

  @Test
  public void testCreateIssue() {
    ErrorCode errorCode = new ErrorCode() {
      @Override
      public String getCode() {
        return null;
      }

      @Override
      public String getMessage() {
        return null;
      }
    };
    context.createIssue(errorCode, "hello", "world");
    Mockito.verify(parentContext).createIssue(errorCode, "hello", "world");
    context.createIssue("configGroup", "configName", errorCode, "goodbye");
    Mockito.verify(parentContext).createIssue("configGroup", "configName", errorCode, "goodbye");
    Mockito.verifyNoMoreInteractions(parentContext);
  }

  @Test
  public void testErrorDescription() {
    context.errorDescription();
    Mockito.verify(parentContext).errorDescription();
    Mockito.verifyNoMoreInteractions(parentContext);
  }

  @Test
  public void testGetPipelineConstants() {
    context.getPipelineConstants();
    Mockito.verify(parentContext).getPipelineConstants();
    Mockito.verifyNoMoreInteractions(parentContext);
  }

  @Test
  public void testGetConnections() {
    context.getConnections();
    Mockito.verify(parentContext).getConnections();
    Mockito.verifyNoMoreInteractions(parentContext);
  }

  @Test
  public void testgetUser() {
    context.getUser();
    Mockito.verify(parentContext).getUser();
    Mockito.verifyNoMoreInteractions(parentContext);
  }
}
