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
package com.streamsets.datacollector.config;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialValue;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

public class TestCredentialStoreDefinition {

  public static class MyCredentialStore implements CredentialStore {
    @Override
    public List<ConfigIssue> init(Context context) {
      return null;
    }

    @Override
    public CredentialValue get(String group, String name, String credentialStoreOptions) throws StageException {
      return null;
    }

    @Override
    public void destroy() {

    }
  }

  @Test
  public void testDefinition() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    CredentialStoreDefinition def =
        new CredentialStoreDefinition(libraryDef, MyCredentialStore.class, "name", "label", "desc");
    Assert.assertEquals(libraryDef, def.getStageLibraryDefinition());
    Assert.assertEquals(MyCredentialStore.class, def.getStoreClass());
    Assert.assertEquals("name", def.getName());
    Assert.assertEquals("label", def.getLabel());
    Assert.assertEquals("desc", def.getDescription());
  }

}
