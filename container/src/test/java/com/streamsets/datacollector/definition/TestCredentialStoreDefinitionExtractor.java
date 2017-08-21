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
package com.streamsets.datacollector.definition;

import com.streamsets.datacollector.config.CredentialStoreDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialStoreDef;
import com.streamsets.pipeline.api.credential.CredentialValue;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

public class TestCredentialStoreDefinitionExtractor {

  @CredentialStoreDef(label = "label", description = "desc")
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
  public void testExtractor() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    CredentialStoreDefinition def =
        CredentialStoreDefinitionExtractor.get().extract(libraryDef, MyCredentialStore.class);

    Assert.assertEquals(libraryDef, def.getStageLibraryDefinition());
    Assert.assertEquals(StageDefinitionExtractor.getStageName(MyCredentialStore.class), def.getName());
    Assert.assertEquals("label", def.getLabel());
    Assert.assertEquals("desc", def.getDescription());
  }

}
