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

import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.service.Service;
import com.streamsets.pipeline.api.service.ServiceDef;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

public class TestServiceDefinitionExtractor {

  public interface PrintMoneyService {
    void print();
  }

  public enum Groups implements Label {
    FIRST("Prvni"),
    SECOND("Druha"),
    ;

    private final String label;

    Groups(String label) {
      this.label = label;
    }

    @Override
    public String getLabel() {
      return label;
    }
  }

  @ServiceDef(
    provides = PrintMoneyService.class,
    version = 1,
    label = "Rich 2.0"
  )
  @ConfigGroups(Groups.class)
  public static final class PrintMoneyServiceImpl implements Service, PrintMoneyService  {

    @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      required = true,
      group = "FIRST",
      label = "Amount of money to print"
    )
    public long amount;

    @Override
    public void print() {
    }

    @Override
    public List<ConfigIssue> init(Context context) {
      return null;
    }

    @Override
    public void destroy() {

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
    ServiceDefinition def = ServiceDefinitionExtractor.get().extract(MOCK_LIB_DEF, PrintMoneyServiceImpl.class);

    // General information
    Assert.assertNotNull(def);
    Assert.assertEquals(getClass().getClassLoader(), def.getStageClassLoader());
    Assert.assertEquals(PrintMoneyServiceImpl.class, def.getKlass());
    Assert.assertEquals(PrintMoneyService.class, def.getProvides());
    Assert.assertEquals("Rich 2.0", def.getLabel());
    Assert.assertEquals(1, def.getVersion());

    // Validate groups
    Assert.assertEquals(2, def.getGroupDefinition().getGroupNames().size());
    Assert.assertTrue(def.getGroupDefinition().getGroupNames().contains(Groups.FIRST.name()));
    Assert.assertTrue(def.getGroupDefinition().getGroupNames().contains(Groups.SECOND.name()));

    // Validate Configs
    Assert.assertEquals(1, def.getConfigDefinitions().size());
    Assert.assertEquals("amount", def.getConfigDefinitions().get(0).getFieldName());
  }

  @ServiceDef(
    provides = PrintMoneyService.class,
    version = 1,
    label = "Rich 2.0"
  )
  @ConfigGroups(Groups.class)
  public static final class DoesNotImplementServiceInterface implements Service {
    @Override
    public List<ConfigIssue> init(Context context) {
      return null;
    }

    @Override
    public void destroy() {

    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDoesNotImplementServiceInterface() {
    ServiceDefinitionExtractor.get().extract(MOCK_LIB_DEF, DoesNotImplementServiceInterface.class);
  }
}
