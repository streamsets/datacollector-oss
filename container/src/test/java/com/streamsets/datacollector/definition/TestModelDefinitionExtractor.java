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

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.ModelDefinition;
import com.streamsets.datacollector.config.ModelType;
import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.PredicateModel;
import com.streamsets.pipeline.api.ValueChooserModel;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public class TestModelDefinitionExtractor {

  public static class CV implements ChooserValues {
    @Override
    public String getResourceBundle() {
      return "rb";
    }

    @Override
    public List<String> getValues() {
      return ImmutableList.of("a");
    }

    @Override
    public List<String> getLabels() {
      return ImmutableList.of("A");
    }
  }

  public static class Bean {
    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true
    )
    public String name;

  }

  public static class Configs {
    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    public String invalid1;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @ListBeanModel
    public Bean invalid3;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @FieldSelectorModel
    public String fieldSelectorM;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @FieldSelectorModel(singleValued = true)
    public String fieldSelectorS;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @ValueChooserModel(CV.class)
    public String valueChooser;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @PredicateModel
    public Map lanePredicateMapping;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @ListBeanModel
    public List<Bean> complexField;
  }

  public static class InvalidConfigs {
    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @ListBeanModel
    public List<Configs> complexField;
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalid1() throws Exception {
    Field f = Configs.class.getField("invalid1");
    ModelDefinitionExtractor.get().extract("", f, "x");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalid3() throws Exception {
    Field f = Configs.class.getField("invalid3");
    ModelDefinitionExtractor.get().extract("", f, "x");
  }

  @Test
  public void testFieldSelectorM() throws Exception {
    Field f = Configs.class.getField("fieldSelectorM");
    ModelDefinition def = ModelDefinitionExtractor.get().extract("", f, "x");
    Assert.assertEquals(null, def.getValues());
    Assert.assertEquals(null, def.getLabels());
    Assert.assertEquals(ModelType.FIELD_SELECTOR_MULTI_VALUE, def.getModelType());
    Assert.assertEquals(null, def.getValuesProviderClass());
    Assert.assertEquals(null, def.getConfigDefinitions());
  }

  @Test
  public void testFieldSelectorS() throws Exception {
    Field f = Configs.class.getField("fieldSelectorS");
    ModelDefinition def = ModelDefinitionExtractor.get().extract("", f, "x");
    Assert.assertEquals(null, def.getValues());
    Assert.assertEquals(null, def.getLabels());
    Assert.assertEquals(ModelType.FIELD_SELECTOR, def.getModelType());
    Assert.assertEquals(null, def.getValuesProviderClass());
    Assert.assertEquals(null, def.getConfigDefinitions());
  }

  @Test
  public void testValueChooserConfig() throws Exception {
    Field f = Configs.class.getField("valueChooser");
    ModelDefinition def = ModelDefinitionExtractor.get().extract("", f, "x");
    Assert.assertEquals(ImmutableList.of("a"), def.getValues());
    Assert.assertEquals(ImmutableList.of("A"), def.getLabels());
    Assert.assertEquals(ModelType.VALUE_CHOOSER, def.getModelType());
    Assert.assertEquals(CV.class.getName(), def.getValuesProviderClass());
    Assert.assertEquals(null, def.getConfigDefinitions());
  }

  @Test
  public void testLanePredicateMapping() throws Exception {
    Field f = Configs.class.getField("lanePredicateMapping");
    ModelDefinition def = ModelDefinitionExtractor.get().extract("", f, "x");
    Assert.assertEquals(null, def.getValues());
    Assert.assertEquals(null, def.getLabels());
    Assert.assertEquals(ModelType.PREDICATE, def.getModelType());
    Assert.assertEquals(null, def.getValuesProviderClass());
    Assert.assertEquals(null, def.getConfigDefinitions());
  }

  @Test
  public void testComplexField() throws Exception {
    Field f = Configs.class.getField("complexField");
    ModelDefinition def = ModelDefinitionExtractor.get().extract("x.", f, "x");
    Assert.assertEquals(null, def.getValues());
    Assert.assertEquals(null, def.getLabels());
    Assert.assertEquals(ModelType.LIST_BEAN, def.getModelType());
    Assert.assertEquals(null, def.getValuesProviderClass());
    Assert.assertEquals(1, def.getConfigDefinitions().size());
    Assert.assertEquals("name", def.getConfigDefinitions().get(0).getName());
    Assert.assertEquals(ConfigDef.Type.STRING, def.getConfigDefinitions().get(0).getType());
  }

  @Test
  public void testComplexFieldInvalid() throws Exception {
    Field f = InvalidConfigs.class.getField("complexField");
    Assert.assertFalse(ModelDefinitionExtractor.get().validate("x.", f, "x").isEmpty());
  }

}
