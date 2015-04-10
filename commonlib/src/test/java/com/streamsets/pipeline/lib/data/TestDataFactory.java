/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.data;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TestDataFactory {

  @Test
  public void testDataFactory() {

    DataFactoryBuilder dataFactoryBuilder = new MockDataFactoryBuilder(getContext(), MockDataFormat.MOCK_DATA);
    DataFactory dataFactory = dataFactoryBuilder
      .setMaxDataLen(1000)
      .setCharset(Charset.defaultCharset())
      .setCompression(Compression.GZIP)
      .setConfig(MockDataFactory.CONFIG1, "myConfig")
      .setConfig(MockDataFactory.CONFIG2, 1000)
      .setMode(MockMode.MODE1)
      .build();

    Assert.assertTrue(dataFactory instanceof MockDataFactory);
    DataFactory.Settings settings = dataFactory.getSettings();
    Assert.assertTrue(settings instanceof MockDataFactory.Settings);

    Assert.assertEquals(Charset.defaultCharset(), settings.getCharset());
    Assert.assertEquals(Compression.GZIP, settings.getCompression());
    Assert.assertEquals(MockDataFormat.MOCK_DATA, settings.getFormat());
    Assert.assertEquals(1000, settings.getMaxRecordLen());

  }

  @Test(expected = IllegalArgumentException.class)
  public void testDataFactorySetUnexpectedMode() {

    DataFactoryBuilder dataFactoryBuilder = new MockDataFactoryBuilder(getContext(), MockDataFormat.MOCK_DATA);
    dataFactoryBuilder
      .setMaxDataLen(1000)
      .setCharset(Charset.defaultCharset())
      .setCompression(Compression.GZIP)
      .setConfig(MockDataFactory.CONFIG1, "myConfig")
      .setConfig(MockDataFactory.CONFIG2, 1000)
      .setMode(JsonMode.ARRAY_OBJECTS)
      .build();

  }

  @Test(expected = IllegalArgumentException.class)
  public void testDataFactorySetUnexpectedConfig() {

    DataFactoryBuilder dataFactoryBuilder = new MockDataFactoryBuilder(getContext(), MockDataFormat.MOCK_DATA);
    dataFactoryBuilder
      .setMaxDataLen(1000)
      .setCharset(Charset.defaultCharset())
      .setCompression(Compression.GZIP)
      .setConfig("myConfig", "myConfig")
      .setConfig(MockDataFactory.CONFIG2, 1000)
      .setMode(JsonMode.ARRAY_OBJECTS)
      .build();
  }

  /**
   * Mock implementation of DataFactory
   */
  static class MockDataFactory extends DataFactory {

    public static final Set<Class<? extends Enum>> MODES;

    public static final String CONFIG1 = "CONFIG1";
    public static final String CONFIG1_DEFAULT = "CONFIG1_DEFAULT";
    public static final String CONFIG2 = "CONFIG2";
    public static final int CONFIG2_DEFAULT = 100;

    public static final Map<String, Object> CONFIGS;

    static {
      Map<String, Object> configs = new HashMap<>();
      configs.put(CONFIG1, CONFIG1_DEFAULT);
      configs.put(CONFIG2, CONFIG2_DEFAULT);
      CONFIGS = Collections.unmodifiableMap(configs);

      Set<Class<? extends Enum>> modes = new HashSet<>();
      modes.add(MockMode.class);
      MODES = modes;
    }

    public MockDataFactory(Settings settings) {
      super(settings);
    }
  }

  enum MockMode {

    MODE1,
    MODE2
    ;
  }

  /**
   * Mock implementation of DataFactoryBuilder
   */
  static class MockDataFactoryBuilder extends
    DataFactoryBuilder<MockDataFactoryBuilder, MockDataFactory, MockDataFormat> {

    public MockDataFactoryBuilder(Stage.Context context, MockDataFormat format) {
      super(context, format);
    }

  }

  /**
   * Mock implementation of DataFormat
   */
  enum MockDataFormat implements DataFormat<MockDataFactory> {

    MOCK_DATA(MockDataFactory.MODES, MockDataFactory.CONFIGS)
    ;

    private final Set<Class<? extends Enum>> modes;
    private Map<String, Object> configs;

    MockDataFormat(Set<Class<? extends Enum>> modes, Map<String, Object> configs) {
      this.modes = modes;
      this.configs = configs;
    }

    @Override
    public Set<Class<? extends Enum>> getModes() {
      return modes;
    }

    @Override
    public Map<String, Object> getConfigs() {
      return configs;
    }

    @Override
    public MockDataFactory create(DataFactory.Settings settings) {
      return new MockDataFactory(settings);
    }
  }

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }
}
