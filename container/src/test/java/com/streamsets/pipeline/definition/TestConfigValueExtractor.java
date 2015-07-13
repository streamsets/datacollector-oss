/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.ConfigDef;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public class TestConfigValueExtractor {

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
    public List listL;

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
    Assert.assertEquals("", ConfigValueExtractor.get().extract(field, configDef, "x"));

    field = Configs.class.getField("elEL");
    configDef = field.getAnnotation(ConfigDef.class);
    Assert.assertEquals("${x}", ConfigValueExtractor.get().extract(field, configDef, "x"));
  }
}
