/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.creation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageLibraryDefinition;
import com.streamsets.pipeline.definition.StageDefinitionExtractor;
import com.streamsets.pipeline.validation.Issue;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestPipelineBeanCreator {

  public enum E { A, B }

  @Test
  public void testToEnum() {
    List<Issue> issues = new ArrayList<>();
    Object v = PipelineBeanCreator.get().toEnum(E.class, "A", "s", "c", issues);
    Assert.assertEquals(E.A, v);
    Assert.assertTrue(issues.isEmpty());

    v = PipelineBeanCreator.get().toEnum(E.class, "x", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());
  }

  @Test
  public void testToString() {
    List<Issue> issues = new ArrayList<>();
    Object v = PipelineBeanCreator.get().toString("A", "s", "c", issues);
    Assert.assertEquals("A", v);
    Assert.assertTrue(issues.isEmpty());

    v = PipelineBeanCreator.get().toString(1, "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());
  }

  @Test
  public void testChar() {
    List<Issue> issues = new ArrayList<>();
    Object v = PipelineBeanCreator.get().toChar("A", "s", "c", issues);
    Assert.assertEquals('A', v);
    Assert.assertTrue(issues.isEmpty());

    v = PipelineBeanCreator.get().toChar(1, "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    v = PipelineBeanCreator.get().toChar("", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    v = PipelineBeanCreator.get().toChar("abc", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());
  }

  @Test
  public void testToBoolean() {
    List<Issue> issues = new ArrayList<>();
    Object v = PipelineBeanCreator.get().toBoolean(true, "s", "c", issues);
    Assert.assertEquals(true, v);
    Assert.assertTrue(issues.isEmpty());

    v = PipelineBeanCreator.get().toBoolean(1, "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());
  }

  @Test
  public void testToNumber() {
    List<Issue> issues = new ArrayList<>();
    Assert.assertEquals((byte) 1, PipelineBeanCreator.get().toNumber(Byte.TYPE, (byte) 1, "s", "c", issues));
    Assert.assertEquals((short) 1, PipelineBeanCreator.get().toNumber(Short.TYPE, (short) 1, "s", "c", issues));
    Assert.assertEquals((int) 1, PipelineBeanCreator.get().toNumber(Integer.TYPE, (int) 1, "s", "c", issues));
    Assert.assertEquals((long) 1, PipelineBeanCreator.get().toNumber(Long.TYPE, (long) 1, "s", "c", issues));
    Assert.assertEquals((float) 1, PipelineBeanCreator.get().toNumber(Float.TYPE, (float) 1, "s", "c", issues));
    Assert.assertEquals((double) 1, PipelineBeanCreator.get().toNumber(Double.TYPE, (double) 1, "s", "c", issues));
    Assert.assertEquals((byte) 1, PipelineBeanCreator.get().toNumber(Byte.class, new Byte((byte)1), "s", "c", issues));
    Assert.assertEquals((short) 1, PipelineBeanCreator.get().toNumber(Short.class, new Short((short)1), "s", "c", issues));
    Assert.assertEquals((int) 1, PipelineBeanCreator.get().toNumber(Integer.class, new Integer(1), "s", "c", issues));
    Assert.assertEquals((long) 1, PipelineBeanCreator.get().toNumber(Long.class, new Long(1), "s", "c", issues));
    Assert.assertEquals((float) 1, PipelineBeanCreator.get().toNumber(Float.class, new Float(1), "s", "c", issues));
    Assert.assertEquals((double) 1, PipelineBeanCreator.get().toNumber(Double.class, new Double(1), "s", "c", issues));
    Assert.assertTrue(issues.isEmpty());

    Object v = PipelineBeanCreator.get().toNumber(Byte.class, 'a', "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());
  }

  public static class SubBean {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        defaultValue = "A",
        required = true
    )
    public E subBeanEnum;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        defaultValue = "B",
        required = false
    )
    public String subBeanString;


    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.LIST,
        defaultValue = "[\"3\"]",
        required = true
    )
    public List<String> subBeanList;

  }

  public static class Bean {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.NUMBER,
        defaultValue = "1",
        required = false
    )
    public int beanInt;

    @ConfigDefBean
    public SubBean beanSubBean;

  }

  @StageDef(version = "1", label = "L")
  public static class MySource extends BaseSource {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.LIST,
        defaultValue = "[\"1\"]",
        required = true
    )
    public List<String> list;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.LIST,
        defaultValue = "[\"2\"]",
        required = true,
        evaluation = ConfigDef.Evaluation.EXPLICIT
    )
    public List<String> listExplicit;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MAP,
        defaultValue = "{\"a\" : \"1\"}",
        required = true
    )
    public Map<String, String> map;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MAP,
        defaultValue = "{\"a\" : \"2\"}",
        required = true,
        evaluation = ConfigDef.Evaluation.EXPLICIT
    )
    public Map<String, String> mapExplicit;

    @ConfigDefBean
    public Bean bean;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @ComplexField(String.class)
    public List<Bean> complexField;

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  @Test
  public void testToList() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    StageDefinition stageDef = StageDefinitionExtractor.get().extract(libraryDef, MySource.class, "");
    ConfigDefinition configDef = stageDef.getConfigDefinition("list");
    Map<String, Object> constants = ImmutableMap.<String, Object>of("a", "A");
    List<Issue> issues = new ArrayList<>();
    Object value = ImmutableList.of("${a}");
    Object v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "s", "c", issues);
    Assert.assertEquals(ImmutableList.of("A"), v);
    Assert.assertTrue(issues.isEmpty());

    value = ImmutableList.of("A");
    v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "s", "c", issues);
    Assert.assertEquals(ImmutableList.of("A"), v);
    Assert.assertTrue(issues.isEmpty());

    value = Arrays.asList(null, "A");
    v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    value = ImmutableList.of("${a");
    v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    value = "x";
    v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    //explicit EL eval
    issues.clear();
    configDef = stageDef.getConfigDefinition("listExplicit");
    value = ImmutableList.of("${a}");
    v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "s", "c", issues);
    Assert.assertEquals(ImmutableList.of("${a}"), v);
    Assert.assertTrue(issues.isEmpty());
  }

  @Test
  public void testToMap() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    StageDefinition stageDef = StageDefinitionExtractor.get().extract(libraryDef, MySource.class, "");
    ConfigDefinition configDef = stageDef.getConfigDefinition("map");
    Map<String, Object> constants = ImmutableMap.<String, Object>of("a", 1);
    List<Issue> issues = new ArrayList<>();
    Object value = ImmutableList.of(ImmutableMap.of("key", "a", "value", 2));
    Object v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "s", "c", issues);
    Assert.assertEquals(ImmutableMap.of("a", 2), v);
    Assert.assertTrue(issues.isEmpty());


    value = ImmutableList.of(ImmutableMap.of("key", "a", "value", "${a}"));
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "s", "c", issues);
    Assert.assertEquals(ImmutableMap.of("a", "1"), v);
    Assert.assertTrue(issues.isEmpty());

    Map map = new HashMap();
    map.put("key", null);
    map.put("value", "A");
    value = ImmutableList.of(map);
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    map.put("key", "a");
    map.put("value", null);
    value = ImmutableList.of(map);
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    value = "x";
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    value = ImmutableList.of(1);
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    //explicit EL eval
    issues.clear();
    configDef = stageDef.getConfigDefinition("mapExplicit");
    value = ImmutableList.of(ImmutableMap.of("key", "a", "value", "${a}"));
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "s", "c", issues);
    Assert.assertNotNull(v);
    Assert.assertEquals(ImmutableMap.of("a", "${a}"), v);
    Assert.assertTrue(issues.isEmpty());
  }

  @Test
  public void testCreateAndInjectStageUsingDefaults() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    StageDefinition stageDef = StageDefinitionExtractor.get().extract(libraryDef, MySource.class, "");

    StageConfiguration stageConf = new StageConfiguration("i", "l", "n", "v", Collections.<ConfigConfiguration>emptyList(),
                                                          Collections.<String, Object>emptyMap(),
                                                          Collections.<String>emptyList(),
                                                          Collections.<String>emptyList());

    Map<String, Object> constants = ImmutableMap.<String, Object>of("a", 1);
    List<Issue> issues = new ArrayList<>();

    StageBean bean = PipelineBeanCreator.get().createStage(stageDef, stageConf, constants, issues);

    Assert.assertNotNull(bean);
    MySource source = (MySource) bean.getStage();
    Assert.assertEquals(ImmutableList.of("1"), source.list);
    Assert.assertEquals(ImmutableList.of("2"), source.listExplicit);
    Assert.assertEquals(ImmutableMap.of("a", "1"), source.map);
    Assert.assertEquals(ImmutableMap.of("a", "2"), source.mapExplicit);
    Assert.assertEquals(1, source.bean.beanInt);
    Assert.assertEquals(E.A, source.bean.beanSubBean.subBeanEnum);
    Assert.assertEquals(ImmutableList.of("3"), source.bean.beanSubBean.subBeanList);
    Assert.assertEquals("B", source.bean.beanSubBean.subBeanString);
    Assert.assertEquals(Collections.emptyList(), source.complexField);
  }

  @Test
  public void testCreateAndInjectStageUsingMixOfDefaultsAndConfig() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    StageDefinition stageDef = StageDefinitionExtractor.get().extract(libraryDef, MySource.class, "");

    List<ConfigConfiguration> configConfs = ImmutableList.of(
        new ConfigConfiguration("list", ImmutableList.of("X")),
        new ConfigConfiguration("map", ImmutableList.of(ImmutableMap.of("key", "a", "value", "AA"))),
        new ConfigConfiguration("bean.beanInt", new Long(3)),
        new ConfigConfiguration("bean.beanSubBean.subBeanEnum", "A"),
        new ConfigConfiguration("bean.beanSubBean.subBeanString", "AA"),
        new ConfigConfiguration("complexField", ImmutableList.of(ImmutableMap.of(
            "complexField.beanInt", 4,
            "complexField.beanSubBean.subBeanEnum", "A",
            "complexField.beanSubBean.subBeanList", ImmutableList.of("a", "b"),
            "complexField.beanSubBean.subBeanString", "X")))
    );
    StageConfiguration stageConf = new StageConfiguration("i", "l", "n", "v", configConfs,
                                                          Collections.<String, Object>emptyMap(),
                                                          Collections.<String>emptyList(),
                                                          Collections.<String>emptyList());

    Map<String, Object> constants = ImmutableMap.<String, Object>of("a", 1);
    List<Issue> issues = new ArrayList<>();

    StageBean bean = PipelineBeanCreator.get().createStage(stageDef, stageConf, constants, issues);

    Assert.assertNotNull(bean);
    MySource source = (MySource) bean.getStage();
    Assert.assertEquals(ImmutableList.of("X"), source.list);
    Assert.assertEquals(ImmutableList.of("2"), source.listExplicit);
    Assert.assertEquals(ImmutableMap.of("a", "AA"), source.map);
    Assert.assertEquals(ImmutableMap.of("a", "2"), source.mapExplicit);
    Assert.assertEquals(3, source.bean.beanInt);
    Assert.assertEquals(E.A, source.bean.beanSubBean.subBeanEnum);
    Assert.assertEquals(ImmutableList.of("3"), source.bean.beanSubBean.subBeanList);
    Assert.assertEquals("AA", source.bean.beanSubBean.subBeanString);
    Assert.assertEquals(1, source.complexField.size());
    Assert.assertEquals(4, source.complexField.get(0).beanInt);
    Assert.assertEquals(E.A, source.complexField.get(0).beanSubBean.subBeanEnum);
    Assert.assertEquals(ImmutableList.of("a", "b"), source.complexField.get(0).beanSubBean.subBeanList);
    Assert.assertEquals("X", source.complexField.get(0).beanSubBean.subBeanString);
  }

}
