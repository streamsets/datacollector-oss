/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageLibraryDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.el.ElConstantDefinition;
import com.streamsets.pipeline.el.ElFunctionDefinition;
import com.streamsets.pipeline.lib.el.StringEL;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreTask;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class TestStageRuntime {

  private static final int INT_VALUE = 1;
  private static final long LONG_VALUE = 2;
  private static final String STRING_VALUE = "STRING";
  private static final boolean BOOLEAN_VALUE = true;
  private static final String CHAR_VALUE = "H";

  private static final StageLibraryDefinition MOCK_LIB_DEF =
      new StageLibraryDefinition(TestStageRuntime.class.getClassLoader(), "mock", "MOCK", new Properties());

  public static class TSource extends BaseSource {
    public boolean inited;
    public boolean destroyed;
    public String stringVar;
    public int intVar;
    public long longVar;
    public boolean booleanVar;
    public char charVar;
    public List<String> stringListVar;
    public Map<String, String> stringMapVar;
    public Map<String, String> listOfMapVar;

    @Override
    protected void init() throws StageException {
      inited = true;
    }

    @Override
    public void destroy() {
      destroyed = true;
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  public static class TTarget extends BaseTarget {
    public boolean inited;
    public boolean destroyed;

    @Override
    protected void init() throws StageException {
      inited = true;
    }

    @Override
    public void destroy() {
      destroyed = true;
    }
    @Override
    public void write(Batch batch) throws StageException {
    }
  }

  @SuppressWarnings("unchecked")
  public static StageLibraryTask createMockStageLibrary() {
    StageLibraryTask lib = Mockito.mock(StageLibraryTask.class);
    List<ConfigDefinition> configDefs = new ArrayList<>();
    ConfigDefinition configDef = new ConfigDefinition("string", ConfigDef.Type.STRING, "l1", "d1", "--", true, "g",
                                                      "stringVar", null, "", new ArrayList<>(), 0,
      Collections.<ElFunctionDefinition>emptyList(), Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE,
      Long.MAX_VALUE, "text/plain", 0, ImmutableList.<Class>of(StringEL.class),
      ConfigDef.Evaluation.IMPLICIT, null);
    configDefs.add(configDef);
    configDef = new ConfigDefinition("int", ConfigDef.Type.NUMBER, "l2", "d2", "-1", true, "g", "intVar", null, "",
      new ArrayList<>(), 0, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0,
      ImmutableList.<Class>of(StringEL.class), ConfigDef.Evaluation.IMPLICIT, null);
    configDefs.add(configDef);
    configDef = new ConfigDefinition("long", ConfigDef.Type.NUMBER, "l3", "d3", "-2", true, "g", "longVar", null, "",
      new ArrayList<>(), 0, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0,
      ImmutableList.<Class>of(StringEL.class), ConfigDef.Evaluation.IMPLICIT, null);
    configDefs.add(configDef);
    configDef = new ConfigDefinition("boolean", ConfigDef.Type.BOOLEAN, "l4", "d4", "false", true, "g", "booleanVar",
      null, "", new ArrayList<>(), 0, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0,
      ImmutableList.<Class>of(StringEL.class), ConfigDef.Evaluation.IMPLICIT, null);
    configDefs.add(configDef);
    configDef = new ConfigDefinition("char", ConfigDef.Type.CHARACTER, "l5", "d5", "K", true, "g", "charVar", null, "",
      new ArrayList<>(), 0, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0,
      ImmutableList.<Class>of(StringEL.class), ConfigDef.Evaluation.IMPLICIT, null);
    configDefs.add(configDef);
    configDef = new ConfigDefinition("stringList", ConfigDef.Type.LIST, "l6", "d6", "", false, "g", "stringListVar", null, "",
      new ArrayList<>(), 0, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0,
      ImmutableList.<Class>of(StringEL.class), ConfigDef.Evaluation.IMPLICIT, null);
    configDefs.add(configDef);
    configDef = new ConfigDefinition("stringMap", ConfigDef.Type.MAP, "l7", "d7", "", false, "g", "stringMapVar", null, "",
      new ArrayList<>(), 0, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0,
      ImmutableList.<Class>of(StringEL.class), ConfigDef.Evaluation.IMPLICIT, null);
    configDefs.add(configDef);
    configDef = new ConfigDefinition("listOfMap", ConfigDef.Type.MAP, "l8", "d8", "", false, "g", "listOfMapVar", null, "",
      new ArrayList<>(), 0, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0,
      ImmutableList.<Class>of(StringEL.class), ConfigDef.Evaluation.IMPLICIT, null);
    configDefs.add(configDef);
    StageDefinition sourceDef = new StageDefinition(
        MOCK_LIB_DEF, TSource.class, "source", "1.0.0", "label", "description", StageType.SOURCE, false, true,
      true, configDefs, null/*raw source definition*/,"", null, false, 1, null,
      Arrays.asList(ExecutionMode.CLUSTER, ExecutionMode.STANDALONE));
    StageDefinition targetDef = new StageDefinition(
        MOCK_LIB_DEF, TTarget.class, "target", "1.0.0", "label", "description", StageType.TARGET, false, true,
      true, Collections.<ConfigDefinition>emptyList(), null/*raw source definition*/,"", null, false, 0, null,
      Arrays.asList(ExecutionMode.CLUSTER, ExecutionMode.STANDALONE));
    Mockito.when(lib.getStage(Mockito.eq("library"), Mockito.eq("source"), Mockito.eq("1.0.0"))).thenReturn(sourceDef);
    Mockito.when(lib.getStage(Mockito.eq("library"), Mockito.eq("target"), Mockito.eq("1.0.0"))).thenReturn(targetDef);
    Mockito.when(lib.getStage(Mockito.eq("library"), Mockito.eq("errorTarget"), Mockito.eq("1.0.0"))).thenReturn(targetDef);
    return lib;
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createMockPipelineConfiguration() {
    List<ConfigConfiguration> configs = new ArrayList<>();
    ConfigConfiguration config = new ConfigConfiguration("string", "STRING");
    configs.add(config);
    config = new ConfigConfiguration("int", 1);
    configs.add(config);
    config = new ConfigConfiguration("long", 2);
    configs.add(config);
    config = new ConfigConfiguration("boolean", true);
    configs.add(config);
    config = new ConfigConfiguration("char", "S");
    configs.add(config);
    config = new ConfigConfiguration("stringList", ImmutableList.of("Hello"));
    configs.add(config);
    config = new ConfigConfiguration("stringMap", ImmutableMap.of("key", "Hello"));
    configs.add(config);
    config = new ConfigConfiguration("listOfMap", ImmutableList.of(ImmutableMap.of("key", "key", "value", "Hello")));
    configs.add(config);
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfiguration("isource", "library", "source", "1.0.0",
      configs, null, Collections.<String>emptyList(), ImmutableList.of("a"));
    stages.add(source);
    StageConfiguration stage = new StageConfiguration(
      "itarget", "library", "target", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, ImmutableList.of("a"),
      Collections.<String>emptyList());
    stages.add(stage);
    List<ConfigConfiguration> pipelineConfigs = new ArrayList<>(2);
    pipelineConfigs.add(new ConfigConfiguration("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    pipelineConfigs.add(new ConfigConfiguration("stopPipelineOnError", false));

    StageConfiguration errorStage = new StageConfiguration(
        "errorStrage", "library", "errorTarget", "1.0.0",
        Collections.<ConfigConfiguration>emptyList(), null, Collections.<String>emptyList(),
        Collections.<String>emptyList());

    return new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, UUID.randomUUID(), null, pipelineConfigs, null,
      stages, errorStage);
  }

  public static PipelineConfiguration createMockPipelineConfigurationWithEL() {
    List<ConfigConfiguration> configs = new ArrayList<>();
    ConfigConfiguration config = new ConfigConfiguration("string", "${STRING_VALUE}");
    configs.add(config);
    config = new ConfigConfiguration("int", "${INT_VALUE}");
    configs.add(config);
    config = new ConfigConfiguration("long", "${LONG_VALUE}");
    configs.add(config);
    config = new ConfigConfiguration("boolean", "${BOOLEAN_VALUE}");
    configs.add(config);
    config = new ConfigConfiguration("char", "${CHAR_VALUE}");
    configs.add(config);
    config = new ConfigConfiguration("stringList", ImmutableList.of("${STRING_VALUE}"));
    configs.add(config);
    config = new ConfigConfiguration("stringMap", ImmutableMap.of("key", "${STRING_VALUE}"));
    configs.add(config);
    config = new ConfigConfiguration("listOfMap", ImmutableList.of(ImmutableMap.of("key", "key", "value", "${STRING_VALUE}")));
    configs.add(config);
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfiguration("isource", "library", "source", "1.0.0",
      configs, null, Collections.<String>emptyList(), ImmutableList.of("a"));
    stages.add(source);
    StageConfiguration stage = new StageConfiguration(
      "itarget", "library", "target", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, ImmutableList.of("a"),
      Collections.<String>emptyList());
    stages.add(stage);
    List<ConfigConfiguration> pipelineConfigs = new ArrayList<>(2);
    pipelineConfigs.add(new ConfigConfiguration("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    pipelineConfigs.add(new ConfigConfiguration("stopPipelineOnError", false));

    //Add the constants configuration
    List<Map<String, Object>> constantsConfig = new ArrayList<>();
    Map<String, Object> intConst = new HashMap<>();
    intConst.put("key", "INT_VALUE");
    intConst.put("value", INT_VALUE);
    constantsConfig.add(intConst);
    Map<String, Object> longConst = new HashMap<>();
    longConst.put("key", "LONG_VALUE");
    longConst.put("value", LONG_VALUE);
    constantsConfig.add(longConst);
    Map<String, Object> booleanConst = new HashMap<>();
    booleanConst.put("key", "BOOLEAN_VALUE");
    booleanConst.put("value", BOOLEAN_VALUE);
    constantsConfig.add(booleanConst);
    Map<String, Object> charConst = new HashMap<>();
    charConst.put("key", "CHAR_VALUE");
    charConst.put("value", CHAR_VALUE);
    constantsConfig.add(charConst);
    Map<String, Object> stringConst = new HashMap<>();
    stringConst.put("key", "STRING_VALUE");
    stringConst.put("value", STRING_VALUE);
    constantsConfig.add(stringConst);
    pipelineConfigs.add(new ConfigConfiguration("constants", constantsConfig));

    StageConfiguration errorStage = new StageConfiguration(
      "errorStrage", "library", "errorTarget", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, Collections.<String>emptyList(),
      Collections.<String>emptyList());

    return new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, UUID.randomUUID(), null, pipelineConfigs, null,
      stages, errorStage);
  }

  @Test
  public void testBuilderValidPipeline() throws Exception {
    StageLibraryTask stageLibrary = createMockStageLibrary();
    PipelineConfiguration pipelineConf = createMockPipelineConfiguration();
    StageRuntime.Builder builder = new StageRuntime.Builder(stageLibrary, "name", pipelineConf);
    StageRuntime[] runtimes = builder.build();
    Assert.assertEquals(2, runtimes.length);
    Assert.assertEquals(stageLibrary.getStage("library", "source", "1.0.0"), runtimes[0].getDefinition());
    Assert.assertEquals(pipelineConf.getStages().get(0), runtimes[0].getConfiguration());
    Assert.assertTrue(runtimes[0].getStage() instanceof TSource);
    Assert.assertEquals("STRING", ((TSource) runtimes[0].getStage()).stringVar);
    Assert.assertEquals(1, ((TSource) runtimes[0].getStage()).intVar);
    Assert.assertEquals(2l, ((TSource) runtimes[0].getStage()).longVar);
    Assert.assertEquals(true, ((TSource) runtimes[0].getStage()).booleanVar);
    Assert.assertEquals('S', ((TSource) runtimes[0].getStage()).charVar);
    Assert.assertEquals("isource", runtimes[0].getInfo().getInstanceName());
    Assert.assertEquals("source", runtimes[0].getInfo().getName());
    Assert.assertEquals("1.0.0", runtimes[0].getInfo().getVersion());
    Assert.assertNotNull(runtimes[1].getDefinition());
    Assert.assertNotNull(runtimes[1].getConfiguration());
    Assert.assertTrue(runtimes[1].getStage() instanceof TTarget);
    Assert.assertEquals(stageLibrary.getStage("library", "target", "1.0.0"), runtimes[1].getDefinition());
    Assert.assertEquals(pipelineConf.getStages().get(1), runtimes[1].getConfiguration());
  }

  @Test
  public void testBuilderValidPipelineWithConstants() throws Exception {
    StageLibraryTask stageLibrary = createMockStageLibrary();
    PipelineConfiguration pipelineConf = createMockPipelineConfigurationWithEL();
    StageRuntime.Builder builder = new StageRuntime.Builder(stageLibrary, "name", pipelineConf);
    StageRuntime[] runtimes = builder.build();
    Assert.assertEquals(2, runtimes.length);
    Assert.assertEquals(stageLibrary.getStage("library", "source", "1.0.0"), runtimes[0].getDefinition());
    Assert.assertEquals(pipelineConf.getStages().get(0), runtimes[0].getConfiguration());
    Assert.assertTrue(runtimes[0].getStage() instanceof TSource);
    Assert.assertEquals("STRING", ((TSource) runtimes[0].getStage()).stringVar);
    Assert.assertEquals(1, ((TSource) runtimes[0].getStage()).intVar);
    Assert.assertEquals(2l, ((TSource) runtimes[0].getStage()).longVar);
    Assert.assertEquals(true, ((TSource) runtimes[0].getStage()).booleanVar);
    Assert.assertEquals('H', ((TSource) runtimes[0].getStage()).charVar);
    Assert.assertEquals("STRING", ((TSource) runtimes[0].getStage()).stringListVar.get(0));
    Assert.assertEquals("STRING", ((TSource) runtimes[0].getStage()).stringMapVar.get("key"));
    Assert.assertEquals("STRING", ((TSource) runtimes[0].getStage()).listOfMapVar.get("key"));
    Assert.assertEquals("isource", runtimes[0].getInfo().getInstanceName());
    Assert.assertEquals("source", runtimes[0].getInfo().getName());
    Assert.assertEquals("1.0.0", runtimes[0].getInfo().getVersion());
    Assert.assertNotNull(runtimes[1].getDefinition());
    Assert.assertNotNull(runtimes[1].getConfiguration());
    Assert.assertTrue(runtimes[1].getStage() instanceof TTarget);
    Assert.assertEquals(stageLibrary.getStage("library", "target", "1.0.0"), runtimes[1].getDefinition());
    Assert.assertEquals(pipelineConf.getStages().get(1), runtimes[1].getConfiguration());
  }

  @Test(expected = PipelineRuntimeException.class)
  public void testBuilderInvalidPipeline() throws Exception {
    StageLibraryTask stageLibrary = createMockStageLibrary();
    PipelineConfiguration pipelineConf = createMockPipelineConfiguration();
    pipelineConf.getStages().remove(1);
    StageRuntime.Builder builder = new StageRuntime.Builder(stageLibrary, "name", pipelineConf);
    StageRuntime[] runtimes = builder.build();
  }

  @Test
  public void testBuilderValidPipelineContextInitDestroy() throws Exception {
    StageLibraryTask stageLibrary = createMockStageLibrary();
    PipelineConfiguration pipelineConf = createMockPipelineConfiguration();
    StageRuntime.Builder builder = new StageRuntime.Builder(stageLibrary, "name", pipelineConf);
    StageRuntime[] runtimes = builder.build();
    Assert.assertFalse(((TSource)runtimes[0].getStage()).inited);
    StageContext context = Mockito.mock(StageContext.class);
    runtimes[0].setContext(context);
    Assert.assertEquals(context, runtimes[0].getContext());
    runtimes[0].init();
    Assert.assertTrue(((TSource) runtimes[0].getStage()).inited);
    Assert.assertFalse(((TSource) runtimes[0].getStage()).destroyed);
    runtimes[0].destroy();
    Assert.assertTrue(((TSource) runtimes[0].getStage()).destroyed);
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderValidPipelineContextNotSet() throws Exception {
    StageLibraryTask stageLibrary = createMockStageLibrary();
    PipelineConfiguration pipelineConf = createMockPipelineConfiguration();
    StageRuntime.Builder builder = new StageRuntime.Builder(stageLibrary, "name", pipelineConf);
    StageRuntime[] runtimes = builder.build();
    Assert.assertFalse(((TSource) runtimes[0].getStage()).inited);
    runtimes[0].init();
  }

}
