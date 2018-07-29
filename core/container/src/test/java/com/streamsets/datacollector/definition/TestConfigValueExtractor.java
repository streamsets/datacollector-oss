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
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.ConfigDef;

import com.streamsets.pipeline.api.MultiValueChooserModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public class TestConfigValueExtractor {

  public enum FooEnum { A, B, C }

  public static class FooEnumValueChooser extends BaseEnumChooserValues<FooEnum> {
    public FooEnumValueChooser() {
      super(FooEnum.class);
    }
  }

  public static class Configs {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        defaultValue = "X",
        required = true
    )
    public String withDefault;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.NUMBER,
        required = true
    )
    public int withoutDefault;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.BOOLEAN,
        required = true,
        defaultValue = "true"
    )
    public boolean boolB;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.CHARACTER,
        required = true,
        defaultValue = "c"
    )
    public char charC;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.NUMBER,
        required = true,
        defaultValue = "1"
    )
    public int byteN;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.NUMBER,
        required = true,
        defaultValue = "1"
    )
    public short shortN;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.NUMBER,
        required = true,
        defaultValue = "1"
    )
    public int intN;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.NUMBER,
        required = true,
        defaultValue = "1"
    )
    public long longN;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.NUMBER,
        required = true,
        defaultValue = "0.5"
    )
    public float floatN;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.NUMBER,
        required = true,
        defaultValue = "0.5"
    )
    public double doubleN;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.LIST,
        required = true,
        defaultValue = "[ 1 ]"
    )
    public List<Integer> listL;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MAP,
        required = true,
        defaultValue = "{ \"K\": \"V\"}"
    )
    public Map mapM;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.TEXT,
        required = true,
        defaultValue = "Hello"
    )
    public String textT;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    public String modelM;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.NUMBER,
        required = true,
        defaultValue = "${x}"
    )
    public int elEL;

    @ConfigDef(
        label = "L",
        defaultValue = "A",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @ValueChooserModel(FooEnumValueChooser.class)
    public FooEnum enumS;

    @ConfigDef(
        label = "L",
        defaultValue = "[ \"A\" ]",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @MultiValueChooserModel(FooEnumValueChooser.class)
    public List<FooEnum> enumM;

    @ConfigDef(
      type = ConfigDef.Type.RUNTIME,
      required = true,
      label = "L",
      defaultValue = "stringValue"
    )
    public String runtimeString;

    @ConfigDef(
      type = ConfigDef.Type.RUNTIME,
      required = true,
      label = "L",
      defaultValue = "1987"
    )
    public int runtimeInteger;
  }

  @Test
  public void testExtractValue() throws Exception {
    Field field = Configs.class.getField("withDefault");
    ConfigDef configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals("X", ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("boolB");
    configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals(true, ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("charC");
    configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals('c', ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("byteN");
    configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals(1, ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("shortN");
    configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals((short)1, ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("intN");
    configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals(1, ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("longN");
    configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals((long)1, ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("floatN");
    configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals((float)0.5, ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("doubleN");
    configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals(0.5, ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("listL");
    configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals(ImmutableList.of(1), ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("mapM");
    configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals(ImmutableList.of(ImmutableMap.of("key", "K", "value", "V")),
                        ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("textT");
    configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals("Hello", ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("modelM");
    configDef = field.getAnnotation(ConfigDef.class);
    // we get null here but the bean creator will inject the type default
    Assert.assertEquals(null, ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("elEL");
    configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals("${x}", ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("enumS");
    configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals(FooEnum.A, ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("enumM");
    configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals(ImmutableList.of(FooEnum.A), ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("runtimeString");
    configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals("stringValue", ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("runtimeInteger");
    configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals(1987, ConfigValueExtractor.get().extract(field, configDef, "x"));
  }
}
