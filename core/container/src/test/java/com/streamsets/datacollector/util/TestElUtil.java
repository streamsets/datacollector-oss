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
package com.streamsets.datacollector.util;

import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.credential.ClearCredentialValue;
import com.streamsets.datacollector.credential.CredentialEL;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

public class TestElUtil {
  public CredentialValue credentialValue;
  private static final Field CV_FIELD;

  static {
    try {
      CV_FIELD = TestElUtil.class.getField("credentialValue");
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Before
  public void setup() {
    CredentialStore cs = new CredentialStore() {
      @Override
      public List<ConfigIssue> init(Context context) {
        return null;
      }

      @Override
      public CredentialValue get(String group, String name, String credentialStoreOptions) throws StageException {
        return () -> "secret";
      }

      @Override
      public void destroy() {

      }
    };
    CredentialEL.setCredentialStores(ImmutableMap.of("cs", cs));
  }

  @After
  public void cleanup() {
    CredentialEL.setCredentialStores(null);
  }

  private Object evaluate(String el) throws Exception {
    StageDefinition sd = Mockito.mock(StageDefinition.class);
    Mockito.when(sd.getName()).thenReturn("stage");
    ConfigDefinition cd = Mockito.mock(ConfigDefinition.class);
    Mockito.when(cd.getEvaluation()).thenReturn(ConfigDef.Evaluation.IMPLICIT);
    Mockito.when(cd.getName()).thenReturn("config");
    Mockito.when(cd.getElDefs()).thenReturn(ImmutableList.of(CredentialEL.class));
    Mockito.when(cd.getConfigField()).thenReturn(CV_FIELD);
    Mockito.when(cd.getType()).thenReturn(ConfigDef.Type.CREDENTIAL);
    return ElUtil.evaluate("Test", el, cd, Collections.emptyMap());
  }

  @Test
  public void testCredentialELNonCredentialEL() throws Exception {
    Object evaluated = evaluate("${\"hello\"}");
    Assert.assertNotNull(evaluated);
    Assert.assertTrue(evaluated instanceof ClearCredentialValue);
    Assert.assertEquals("hello", ((ClearCredentialValue)evaluated).get());
  }

  @Test
  public void testCredentialELCredentialEL() throws Exception {
    Object evaluated = evaluate("${credential:get('cs','name', '')}");
    Assert.assertNotNull(evaluated);
    Assert.assertTrue(evaluated instanceof CredentialValue);
    Assert.assertEquals("secret", ((CredentialValue)evaluated).get());
  }

}
