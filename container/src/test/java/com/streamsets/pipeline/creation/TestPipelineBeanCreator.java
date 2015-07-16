/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.creation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageLibraryDefinition;
import com.streamsets.pipeline.definition.StageDefinitionExtractor;
import com.streamsets.pipeline.stagelibrary.ClassLoaderReleaser;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
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
import java.util.UUID;

public class TestPipelineBeanCreator {

  private StageDefinition getStageDef() {
    StageDefinition def = Mockito.mock(StageDefinition.class);
    Mockito.when(def.isErrorStage()).thenReturn(false);
    return def;
  }

  public enum E { A, B }

  @Test
  public void testToEnum() {
    List<Issue> issues = new ArrayList<>();
    Object v = PipelineBeanCreator.get().toEnum(E.class, "A", getStageDef(), "g", "s", "c", issues);
    Assert.assertEquals(E.A, v);
    Assert.assertTrue(issues.isEmpty());

    v = PipelineBeanCreator.get().toEnum(E.class, "x", getStageDef(), "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());
  }

  @Test
  public void testToString() {
    List<Issue> issues = new ArrayList<>();
    Object v = PipelineBeanCreator.get().toString("A", getStageDef(), "g", "s", "c", issues);
    Assert.assertEquals("A", v);
    Assert.assertTrue(issues.isEmpty());

    v = PipelineBeanCreator.get().toString(1, getStageDef(), "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());
  }

  @Test
  public void testChar() {
    List<Issue> issues = new ArrayList<>();
    Object v = PipelineBeanCreator.get().toChar("A", getStageDef(), "g", "s", "c", issues);
    Assert.assertEquals('A', v);
    Assert.assertTrue(issues.isEmpty());

    v = PipelineBeanCreator.get().toChar(1, getStageDef(), "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    v = PipelineBeanCreator.get().toChar("", getStageDef(), "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    v = PipelineBeanCreator.get().toChar("abc", getStageDef(), "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());
  }

  @Test
  public void testToBoolean() {
    List<Issue> issues = new ArrayList<>();
    Object v = PipelineBeanCreator.get().toBoolean(true, getStageDef(), "g", "s", "c", issues);
    Assert.assertEquals(true, v);
    Assert.assertTrue(issues.isEmpty());

    v = PipelineBeanCreator.get().toBoolean(1, getStageDef(), "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());
  }

  @Test
  public void testToNumber() {
    List<Issue> issues = new ArrayList<>();
    Assert.assertEquals((byte) 1, PipelineBeanCreator.get().toNumber(Byte.TYPE, (byte) 1, getStageDef(),
                                                                     "g", "s", "c", issues));
    Assert.assertEquals((short) 1, PipelineBeanCreator.get().toNumber(Short.TYPE, (short) 1, getStageDef(),
                                                                      "g", "s", "c", issues));
    Assert.assertEquals((int) 1, PipelineBeanCreator.get().toNumber(Integer.TYPE, (int) 1, getStageDef(),
                                                                    "g", "s", "c", issues));
    Assert.assertEquals((long) 1, PipelineBeanCreator.get().toNumber(Long.TYPE, (long) 1, getStageDef(),
                                                                     "g", "s", "c", issues));
    Assert.assertEquals((float) 1, PipelineBeanCreator.get().toNumber(Float.TYPE, (float) 1, getStageDef(),
                                                                      "g", "s", "c", issues));
    Assert.assertEquals((double) 1, PipelineBeanCreator.get().toNumber(Double.TYPE, (double) 1, getStageDef(),
                                                                       "g", "s", "c", issues));
    Assert.assertEquals((byte) 1, PipelineBeanCreator.get().toNumber(Byte.class, new Byte((byte)1), getStageDef(),
                                                                     "g", "s", "c", issues));
    Assert.assertEquals((short) 1, PipelineBeanCreator.get().toNumber(Short.class, new Short((short)1), getStageDef(),
                                                                      "g", "s", "c", issues));
    Assert.assertEquals((int) 1, PipelineBeanCreator.get().toNumber(Integer.class, new Integer(1), getStageDef(),
                                                                    "g", "s", "c", issues));
    Assert.assertEquals((long) 1, PipelineBeanCreator.get().toNumber(Long.class, new Long(1), getStageDef(),
                                                                     "g", "s", "c", issues));
    Assert.assertEquals((float) 1, PipelineBeanCreator.get().toNumber(Float.class, new Float(1), getStageDef(),
                                                                      "g", "s", "c", issues));
    Assert.assertEquals((double) 1, PipelineBeanCreator.get().toNumber(Double.class, new Double(1), getStageDef(),
                                                                       "g", "s", "c", issues));
    Assert.assertTrue(issues.isEmpty());

    Object v = PipelineBeanCreator.get().toNumber(Byte.class, 'a', getStageDef(), "g", "s", "c", issues);
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
  public static class MyTarget extends BaseTarget {

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
    public void write(Batch batch) throws StageException {

    }
  }

  @StageDef(version = "1", label = "L")
  @ErrorStage
  public static class ErrorMyTarget extends MyTarget {
  }

  @Test
  public void testToList() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    StageDefinition stageDef = StageDefinitionExtractor.get().extract(libraryDef, MyTarget.class, "");
    ConfigDefinition configDef = stageDef.getConfigDefinition("list");
    Map<String, Object> constants = ImmutableMap.<String, Object>of("a", "A");
    List<Issue> issues = new ArrayList<>();
    Object value = ImmutableList.of("${a}");
    Object v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertEquals(ImmutableList.of("A"), v);
    Assert.assertTrue(issues.isEmpty());

    value = ImmutableList.of("A");
    v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertEquals(ImmutableList.of("A"), v);
    Assert.assertTrue(issues.isEmpty());

    value = Arrays.asList(null, "A");
    v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    value = ImmutableList.of("${a");
    v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    value = "x";
    v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    //explicit EL eval
    issues.clear();
    configDef = stageDef.getConfigDefinition("listExplicit");
    value = ImmutableList.of("${a}");
    v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertEquals(ImmutableList.of("${a}"), v);
    Assert.assertTrue(issues.isEmpty());
  }

  @Test
  public void testToMap() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    StageDefinition stageDef = StageDefinitionExtractor.get().extract(libraryDef, MyTarget.class, "");
    ConfigDefinition configDef = stageDef.getConfigDefinition("map");
    Map<String, Object> constants = ImmutableMap.<String, Object>of("a", 1);
    List<Issue> issues = new ArrayList<>();
    Object value = ImmutableList.of(ImmutableMap.of("key", "a", "value", 2));
    Object v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertEquals(ImmutableMap.of("a", 2), v);
    Assert.assertTrue(issues.isEmpty());


    value = ImmutableList.of(ImmutableMap.of("key", "a", "value", "${a}"));
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertEquals(ImmutableMap.of("a", "1"), v);
    Assert.assertTrue(issues.isEmpty());

    Map map = new HashMap();
    map.put("key", null);
    map.put("value", "A");
    value = ImmutableList.of(map);
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    map.put("key", "a");
    map.put("value", null);
    value = ImmutableList.of(map);
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    value = "x";
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    value = ImmutableList.of(1);
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    //explicit EL eval
    issues.clear();
    configDef = stageDef.getConfigDefinition("mapExplicit");
    value = ImmutableList.of(ImmutableMap.of("key", "a", "value", "${a}"));
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertNotNull(v);
    Assert.assertEquals(ImmutableMap.of("a", "${a}"), v);
    Assert.assertTrue(issues.isEmpty());
  }

  @Test
  public void testCreateAndInjectStageUsingDefaults() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    StageDefinition stageDef = StageDefinitionExtractor.get().extract(libraryDef, MyTarget.class, "");

    StageConfiguration stageConf = new StageConfiguration("i", "l", "n", "v", Collections.<ConfigConfiguration>emptyList(),
                                                          Collections.<String, Object>emptyMap(),
                                                          Collections.<String>emptyList(),
                                                          Collections.<String>emptyList());

    Map<String, Object> constants = ImmutableMap.<String, Object>of("a", 1);
    List<Issue> issues = new ArrayList<>();

    StageBean bean = PipelineBeanCreator.get().createStage(stageDef, Mockito.mock(ClassLoaderReleaser.class), stageConf,
                                                           constants, issues);

    Assert.assertNotNull(bean);
    MyTarget stage = (MyTarget) bean.getStage();
    Assert.assertEquals(ImmutableList.of("1"), stage.list);
    Assert.assertEquals(ImmutableList.of("2"), stage.listExplicit);
    Assert.assertEquals(ImmutableMap.of("a", "1"), stage.map);
    Assert.assertEquals(ImmutableMap.of("a", "2"), stage.mapExplicit);
    Assert.assertEquals(1, stage.bean.beanInt);
    Assert.assertEquals(E.A, stage.bean.beanSubBean.subBeanEnum);
    Assert.assertEquals(ImmutableList.of("3"), stage.bean.beanSubBean.subBeanList);
    Assert.assertEquals("B", stage.bean.beanSubBean.subBeanString);
    Assert.assertEquals(Collections.emptyList(), stage.complexField);
  }

  @Test
  public void testCreateAndInjectStageUsingMixOfDefaultsAndConfig() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    StageDefinition stageDef = StageDefinitionExtractor.get().extract(libraryDef, MyTarget.class, "");

    List<ConfigConfiguration> configConfs = ImmutableList.of(
        new ConfigConfiguration("list", ImmutableList.of("X")),
        new ConfigConfiguration("map", ImmutableList.of(ImmutableMap.of("key", "a", "value", "AA"))),
        new ConfigConfiguration("bean.beanInt", new Long(3)),
        new ConfigConfiguration("bean.beanSubBean.subBeanEnum", "A"),
        new ConfigConfiguration("bean.beanSubBean.subBeanString", "AA"),
        new ConfigConfiguration("complexField", ImmutableList.of(ImmutableMap.of(
            "beanInt", 4,
            "beanSubBean.subBeanEnum", "A",
            "beanSubBean.subBeanList", ImmutableList.of("a", "b"),
            "beanSubBean.subBeanString", "X")))
    );
    StageConfiguration stageConf = new StageConfiguration("i", "l", "n", "v", configConfs,
                                                          Collections.<String, Object>emptyMap(),
                                                          Collections.<String>emptyList(),
                                                          Collections.<String>emptyList());

    Map<String, Object> constants = ImmutableMap.<String, Object>of("a", 1);
    List<Issue> issues = new ArrayList<>();

    StageBean bean = PipelineBeanCreator.get().createStage(stageDef, Mockito.mock(ClassLoaderReleaser.class), stageConf,
                                                           constants, issues);

    Assert.assertNotNull(bean);
    MyTarget stage = (MyTarget) bean.getStage();
    Assert.assertEquals(ImmutableList.of("X"), stage.list);
    Assert.assertEquals(ImmutableList.of("2"), stage.listExplicit);
    Assert.assertEquals(ImmutableMap.of("a", "AA"), stage.map);
    Assert.assertEquals(ImmutableMap.of("a", "2"), stage.mapExplicit);
    Assert.assertEquals(3, stage.bean.beanInt);
    Assert.assertEquals(E.A, stage.bean.beanSubBean.subBeanEnum);
    Assert.assertEquals(ImmutableList.of("3"), stage.bean.beanSubBean.subBeanList);
    Assert.assertEquals("AA", stage.bean.beanSubBean.subBeanString);
    Assert.assertEquals(1, stage.complexField.size());
    Assert.assertEquals(4, stage.complexField.get(0).beanInt);
    Assert.assertEquals(E.A, stage.complexField.get(0).beanSubBean.subBeanEnum);
    Assert.assertEquals(ImmutableList.of("a", "b"), stage.complexField.get(0).beanSubBean.subBeanList);
    Assert.assertEquals("X", stage.complexField.get(0).beanSubBean.subBeanString);
  }

  @Test
  public void testCreatePipelineBean() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    StageDefinition stageDef = StageDefinitionExtractor.get().extract(libraryDef, MyTarget.class, "");
    StageDefinition errorStageDef = StageDefinitionExtractor.get().extract(libraryDef, ErrorMyTarget.class, "");
    StageLibraryTask library = Mockito.mock(StageLibraryTask.class);
    Mockito.when(library.getStage(Mockito.eq("l"), Mockito.eq("s"), Mockito.eq("v"))).thenReturn(stageDef);
    Mockito.when(library.getStage(Mockito.eq("l"), Mockito.eq("e"), Mockito.eq("v"))).thenReturn(errorStageDef);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());

    List<ConfigConfiguration> pipelineConfigs = ImmutableList.of(
        new ConfigConfiguration("executionMode", ExecutionMode.CLUSTER.name()),
        new ConfigConfiguration("memoryLimit", 1000)
    );

    StageConfiguration stageConf = new StageConfiguration("si", "l", "s", "v",
        ImmutableList.of(new ConfigConfiguration("list", ImmutableList.of("S"))),
        Collections.<String, Object>emptyMap(), Collections.<String>emptyList(), Collections.<String>emptyList());
    StageConfiguration errorStageConf = new StageConfiguration("ei", "l", "e", "v",
        ImmutableList.of(new ConfigConfiguration("list", ImmutableList.of("E"))),
        Collections.<String, Object>emptyMap(), Collections.<String>emptyList(), Collections.<String>emptyList());
    PipelineConfiguration pipelineConf = new PipelineConfiguration(1, UUID.randomUUID(), "D", pipelineConfigs,
        Collections.EMPTY_MAP, ImmutableList.of(stageConf), errorStageConf);

    List<Issue> issues = new ArrayList<>();
    PipelineBean bean = PipelineBeanCreator.get().create(false, library, pipelineConf, issues);

    Assert.assertNotNull(bean);

    // pipeline configs
    Assert.assertEquals(ExecutionMode.CLUSTER, bean.getConfig().executionMode);

    // stages
    Assert.assertEquals(1, bean.getStages().size());
    MyTarget stage = (MyTarget) bean.getStages().get(0).getStage();
    Assert.assertEquals(ImmutableList.of("S"), stage.list);

    // error stage
    ErrorMyTarget errorStage = (ErrorMyTarget) bean.getErrorStage().getStage();
    Assert.assertEquals(ImmutableList.of("E"), errorStage.list);

  }

  @Test
  public void testStageBeanReleaseClassLoader() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Mockito.when(libraryDef.getClassLoader()).thenReturn(cl);
    StageDefinition stageDef = StageDefinitionExtractor.get().extract(libraryDef, MyTarget.class, "");

    StageConfiguration stageConf =
        new StageConfiguration("i", "l", "n", "v", Collections.<ConfigConfiguration>emptyList(),
                               Collections.<String, Object>emptyMap(),
                               Collections.<String>emptyList(),
                               Collections.<String>emptyList());

    Map<String, Object> constants = ImmutableMap.<String, Object>of("a", 1);
    List<Issue> issues = new ArrayList<>();

    ClassLoaderReleaser releaser = Mockito.mock(ClassLoaderReleaser.class);
    StageBean bean = PipelineBeanCreator.get().createStage(stageDef, releaser, stageConf, constants, issues);

    Mockito.verify(releaser, Mockito.never()).releaseStageClassLoader(Mockito.<ClassLoader>any());
    bean.releaseClassLoader();
    Mockito.verify(releaser, Mockito.times(1)).releaseStageClassLoader(cl);

  }

}
