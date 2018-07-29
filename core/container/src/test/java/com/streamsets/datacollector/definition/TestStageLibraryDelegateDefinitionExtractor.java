/*
 * Copyright 2018 StreamSets Inc.
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

import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.config.StageLibraryDelegateDefinitition;
import com.streamsets.pipeline.api.delegate.BaseStageLibraryDelegate;
import com.streamsets.pipeline.api.delegate.StageLibraryDelegateDef;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestStageLibraryDelegateDefinitionExtractor {

  public interface MakeMeRich {
    public void doIt();
  }

  @StageLibraryDelegateDef(MakeMeRich.class)
  public static class MakeMeRichDelegate extends BaseStageLibraryDelegate implements MakeMeRich {
    @Override
    public void doIt() {
    }
  }


  private static final StageLibraryDefinition MOCK_LIB_DEF = new StageLibraryDefinition(
    TestServiceDefinitionExtractor.class.getClassLoader(),
    "mock",
    "MOCK",
    new Properties(),
    null,
    null,
    null
  );

  @Test
  public void testBasicExtraction() {
    StageLibraryDelegateDefinitition def = StageLibraryDelegateDefinitionExtractor.get().extract(MOCK_LIB_DEF, MakeMeRichDelegate.class);

    // General information
    assertNotNull(def);
    assertEquals(getClass().getClassLoader(), def.getClassLoader());
    assertEquals(MakeMeRich.class, def.getExportedInterface());
  }

  @StageLibraryDelegateDef(MakeMeRich.class)
  public static class DoNotImplementExportedInterface extends BaseStageLibraryDelegate {
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingExportedInterface() {
    StageLibraryDelegateDefinitionExtractor.get().extract(MOCK_LIB_DEF, DoNotImplementExportedInterface.class);
  }
}
