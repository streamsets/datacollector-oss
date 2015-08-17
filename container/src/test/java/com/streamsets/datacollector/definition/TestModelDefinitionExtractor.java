/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.definition;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.ModelDefinition;
import com.streamsets.datacollector.config.ModelType;
import com.streamsets.datacollector.definition.ModelDefinitionExtractor;
import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.FieldValueChooser;
import com.streamsets.pipeline.api.LanePredicateMapping;
import com.streamsets.pipeline.api.ValueChooser;

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
    @FieldSelector
    @FieldValueChooser(CV.class)
    public String invalid2;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @ComplexField
    public Bean invalid3;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @FieldSelector
    public String fieldSelectorM;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @FieldSelector(singleValued = true)
    public String fieldSelectorS;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )

    @FieldValueChooser(CV.class)
    public String fieldChooserValue;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @ValueChooser(CV.class)
    public String valueChooser;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @LanePredicateMapping
    public Map lanePredicateMapping;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @ComplexField
    public List<Bean> complexField;
  }

  public static class InvalidConfigs {
    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @ComplexField
    public List<Configs> complexField;
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalid1() throws Exception {
    Field f = Configs.class.getField("invalid1");
    ModelDefinitionExtractor.get().extract("", f, "x");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalid2() throws Exception {
    Field f = Configs.class.getField("invalid2");
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
    Assert.assertEquals(ModelType.FIELD_SELECTOR_MULTI_VALUED, def.getModelType());
    Assert.assertEquals(null, def.getValuesProviderClass());
    Assert.assertEquals(null, def.getConfigDefinitions());
  }

  @Test
  public void testFieldSelectorS() throws Exception {
    Field f = Configs.class.getField("fieldSelectorS");
    ModelDefinition def = ModelDefinitionExtractor.get().extract("", f, "x");
    Assert.assertEquals(null, def.getValues());
    Assert.assertEquals(null, def.getLabels());
    Assert.assertEquals(ModelType.FIELD_SELECTOR_SINGLE_VALUED, def.getModelType());
    Assert.assertEquals(null, def.getValuesProviderClass());
    Assert.assertEquals(null, def.getConfigDefinitions());
  }

  @Test
  public void testFieldChooserValue() throws Exception {
    Field f = Configs.class.getField("fieldChooserValue");
    ModelDefinition def = ModelDefinitionExtractor.get().extract("", f, "x");
    Assert.assertEquals(ImmutableList.of("a"), def.getValues());
    Assert.assertEquals(ImmutableList.of("A"), def.getLabels());
    Assert.assertEquals(ModelType.FIELD_VALUE_CHOOSER, def.getModelType());
    Assert.assertEquals(CV.class.getName(), def.getValuesProviderClass());
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
    Assert.assertEquals(ModelType.LANE_PREDICATE_MAPPING, def.getModelType());
    Assert.assertEquals(null, def.getValuesProviderClass());
    Assert.assertEquals(null, def.getConfigDefinitions());
  }

  @Test
  public void testComplexField() throws Exception {
    Field f = Configs.class.getField("complexField");
    ModelDefinition def = ModelDefinitionExtractor.get().extract("x.", f, "x");
    Assert.assertEquals(null, def.getValues());
    Assert.assertEquals(null, def.getLabels());
    Assert.assertEquals(ModelType.COMPLEX_FIELD, def.getModelType());
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
