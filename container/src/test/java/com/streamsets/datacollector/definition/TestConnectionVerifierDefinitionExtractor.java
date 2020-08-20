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

package com.streamsets.datacollector.definition;

import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.definition.connection.TestConnectionDef;
import com.streamsets.datacollector.definition.connection.TestConnectionVerifier;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class TestConnectionVerifierDefinitionExtractor {

  private static final StageLibraryDefinition MOCK_LIB_DEF =
      new StageLibraryDefinition(
          TestConnectionVerifierDefinitionExtractor.class.getClassLoader(),
          "Test Library Name", "Test Library Description",
          new Properties(), null, null, null
      );

  @Test
  public void testExtractConnectionVerifierDef() {
    ConnectionVerifierDefinition verifierDef =
        ConnectionVerifierDefinitionExtractor.get().extract(MOCK_LIB_DEF, TestConnectionVerifier.class);
    Assert.assertEquals(TestConnectionDef.TestConnection.TYPE, verifierDef.getVerifierType());
    Assert.assertEquals(TestConnectionVerifier.class.getCanonicalName(), verifierDef.getVerifierClass());
    Assert.assertEquals("connection", verifierDef.getVerifierConnectionFieldName());
    Assert.assertEquals("connectionSelection", verifierDef.getVerifierConnectionSelectionFieldName());
    Assert.assertEquals(MOCK_LIB_DEF.getName(), verifierDef.getLibrary());
  }
}
