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
package com.streamsets.pipeline.stage.processor.geolocation;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.common.net.InetAddresses;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class TestGeolocationProcessor {

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> ipAddresses() {
    return ImmutableList.of("128.101.101.101", "2602:ae:14a5::");
  }

  @Parameterized.Parameter
  public String ip;

  private File tempDir;
  private File countryDb;
  private File cityDb;

  @Before
  public void setup() throws Exception {
    tempDir = Files.createTempDir();
    countryDb = new File(tempDir, "GeoLite2-Country.mmdb");
    cityDb = new File(tempDir, "GeoLite2-City.mmdb");
    BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(countryDb));
    Resources.copy(Resources.getResource("GeoLite2-Country.mmdb"), out);
    out.flush();
    out.close();
    out = new BufferedOutputStream(new FileOutputStream(cityDb));
    Resources.copy(Resources.getResource("GeoLite2-City.mmdb"), out);
    out.flush();
    out.close();

  }

  @After
  public void tearDown() {
    if (tempDir != null) {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  @Test
  public void testConversion() throws Exception {
    String[] ips = {
      "128.101.101.101",
      "8.8.8.8"
    };
    for (String ipAsString : ips) {
      InetAddress ip = InetAddress.getByName(ipAsString);
      byte[] ipAsBytes = ip.getAddress();
      int ipAsInt = ipAsBytesToInt(ipAsBytes);
      Assert.assertArrayEquals(ipAsBytes, ipAsIntToBytes(ipAsInt));
      Assert.assertArrayEquals(ipAsBytes, ipAsStringToBytes(ipAsString));
      Assert.assertEquals(ipAsString, ipAsIntToString(ipAsInt));
      Assert.assertEquals(ipAsString, ipAsIntToString(ipAsInt));
      Assert.assertEquals(ipAsInt, InetAddresses.coerceToInteger(InetAddresses.forString(ipAsString)));
      Assert.assertArrayEquals(ipAsBytes, ipAsStringToBytes(ipAsString));
    }
  }

  @Test
  public void testIncorrectDatabase() throws Exception {
    List<GeolocationFieldConfig> configs = new ArrayList<>();
    GeolocationFieldConfig config;
    config = new GeolocationFieldConfig();
    config.inputFieldName = "/ipAsInt";
    config.outputFieldName = "/intIpCountry";
    config.targetType = GeolocationField.CITY_NAME;
    configs.add(config);

    List<GeolocationDatabaseConfig> dbConfigs = new ArrayList<>();
    GeolocationDatabaseConfig dbConfig = new GeolocationDatabaseConfig();
    dbConfig.geoIP2DBFile = countryDb.getAbsolutePath();
    dbConfig.geoIP2DBType = GeolocationDBType.CITY;
    dbConfigs.add(dbConfig);

    ProcessorRunner runner = new ProcessorRunner.Builder(GeolocationDProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", configs)
      .addConfiguration("dbConfigs", dbConfigs)
      .addConfiguration("missingAddressAction", GeolocationMissingAddressAction.REPLACE_WITH_NULLS)
      .addOutputLane("a").build();
    List<Stage.ConfigIssue> configErrors = runner.runValidateConfigs();
    Assert.assertEquals(String.valueOf(configErrors), 1, configErrors.size());
    Assert.assertTrue(String.valueOf(configErrors.get(0)),
      String.valueOf(configErrors.get(0)).contains(Errors.GEOIP_05.name()));

    dbConfigs.get(0).geoIP2DBType = GeolocationDBType.COUNTRY;
    runner = new ProcessorRunner.Builder(GeolocationDProcessor.class)
        .addConfiguration("fieldTypeConverterConfigs", configs)
        .addConfiguration("dbConfigs", dbConfigs)
        .addConfiguration("missingAddressAction", GeolocationMissingAddressAction.REPLACE_WITH_NULLS)
        .addOutputLane("a").build();
    configErrors = runner.runValidateConfigs();
    Assert.assertEquals(String.valueOf(configErrors), 1, configErrors.size());
    Assert.assertTrue(String.valueOf(configErrors.get(0)),
        String.valueOf(configErrors.get(0)).contains(Errors.GEOIP_12.name()));
  }

  @Test
  public void testLookup() throws Exception {
    List<GeolocationFieldConfig> configs = new ArrayList<>();
    GeolocationFieldConfig config;
    config = new GeolocationFieldConfig();
    config.inputFieldName = "/ipAsInt";
    config.outputFieldName = "/intIpCountry";
    config.targetType = GeolocationField.COUNTRY_NAME;
    configs.add(config);
    config = new GeolocationFieldConfig();
    config.inputFieldName = "/ipAsIntString";
    config.outputFieldName = "/intStringIpCountry";
    config.targetType = GeolocationField.COUNTRY_NAME;
    configs.add(config);
    config = new GeolocationFieldConfig();
    config.inputFieldName = "/ipAsString";
    config.outputFieldName = "/stringIpCountry";
    config.targetType = GeolocationField.COUNTRY_NAME;
    configs.add(config);

    List<GeolocationDatabaseConfig> dbConfigs = new ArrayList<>();
    GeolocationDatabaseConfig dbConfig = new GeolocationDatabaseConfig();
    dbConfig.geoIP2DBFile = countryDb.getAbsolutePath();
    dbConfig.geoIP2DBType = GeolocationDBType.COUNTRY;
    dbConfigs.add(dbConfig);

    ProcessorRunner runner = new ProcessorRunner.Builder(GeolocationDProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", configs)
      .addConfiguration("dbConfigs", dbConfigs)
      .addConfiguration("missingAddressAction", GeolocationMissingAddressAction.REPLACE_WITH_NULLS)
      .addOutputLane("a").build();
    runner.runInit();
    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("ipAsInt", Field.create(ipAsStringToInt(ip)));
      map.put("ipAsIntString", Field.create(String.valueOf(ipAsStringToInt(ip))));
      map.put("ipAsString", Field.create(ip));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(0, runner.getErrorRecords().size());
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(String.valueOf(result), 6, result.size());
      // IPv6 addresses cannot be represented by an integer.
      if (!ip.contains(":")) {
        Assert.assertEquals("United States", Utils.checkNotNull(result.get("intStringIpCountry"), "intStringIpCountry").getValue());
        Assert.assertEquals("United States", Utils.checkNotNull(result.get("intIpCountry"), "intIpCountry").getValue());
      }
      Assert.assertEquals("United States", Utils.checkNotNull(result.get("stringIpCountry"), "stringIpCountry").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMultiDBLookup() throws Exception {
    List<GeolocationFieldConfig> configs = new ArrayList<>();
    GeolocationFieldConfig config;
    config = new GeolocationFieldConfig();
    config.inputFieldName = "/ipAsInt";
    config.outputFieldName = "/intIpCountry";
    config.targetType = GeolocationField.COUNTRY_NAME;
    configs.add(config);
    config = new GeolocationFieldConfig();
    config.inputFieldName = "/ipAsIntString";
    config.outputFieldName = "/intStringIpCountry";
    config.targetType = GeolocationField.COUNTRY_NAME;
    configs.add(config);
    config = new GeolocationFieldConfig();
    config.inputFieldName = "/ipAsString";
    config.outputFieldName = "/stringIpCountry";
    config.targetType = GeolocationField.COUNTRY_NAME;
    configs.add(config);

    config = new GeolocationFieldConfig();
    config.inputFieldName = "/ipAsInt";
    config.outputFieldName = "/intIpCityName";
    config.targetType = GeolocationField.CITY_NAME;
    configs.add(config);
    config = new GeolocationFieldConfig();
    config.inputFieldName = "/ipAsIntString";
    config.outputFieldName = "/intStringIpCityName";
    config.targetType = GeolocationField.CITY_NAME;
    configs.add(config);
    config = new GeolocationFieldConfig();
    config.inputFieldName = "/ipAsString";
    config.outputFieldName = "/stringIpCityName";
    config.targetType = GeolocationField.CITY_NAME;
    configs.add(config);

    List<GeolocationDatabaseConfig> dbConfigs = new ArrayList<>();
    GeolocationDatabaseConfig dbConfig = new GeolocationDatabaseConfig();
    dbConfig.geoIP2DBFile = countryDb.getAbsolutePath();
    dbConfig.geoIP2DBType = GeolocationDBType.COUNTRY;
    dbConfigs.add(dbConfig);
    dbConfig = new GeolocationDatabaseConfig();
    dbConfig.geoIP2DBFile = cityDb.getAbsolutePath();
    dbConfig.geoIP2DBType = GeolocationDBType.CITY;
    dbConfigs.add(dbConfig);

    ProcessorRunner runner = new ProcessorRunner.Builder(GeolocationDProcessor.class)
        .addConfiguration("fieldTypeConverterConfigs", configs)
        .addConfiguration("dbConfigs", dbConfigs)
        .addConfiguration("missingAddressAction", GeolocationMissingAddressAction.REPLACE_WITH_NULLS)
        .addOutputLane("a").build();
    runner.runInit();
    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("ipAsInt", Field.create(ipAsStringToInt(ip)));
      map.put("ipAsIntString", Field.create(String.valueOf(ipAsStringToInt(ip))));
      map.put("ipAsString", Field.create(ip));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(0, runner.getErrorRecords().size());
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(String.valueOf(result), 9, result.size());
      // IPv6 addresses cannot be represented by an integer.
      if (!ip.contains(":")) {
        Assert.assertEquals("United States",
            Utils.checkNotNull(result.get("intStringIpCountry"), "intStringIpCountry").getValue()
        );
        Assert.assertEquals("United States", Utils.checkNotNull(result.get("intIpCountry"), "intIpCountry").getValue());
        Assert.assertEquals("Minneapolis",
            Utils.checkNotNull(result.get("intStringIpCityName"), "intStringIpCityName").getValue()
        );
        Assert.assertEquals("Minneapolis", Utils.checkNotNull(result.get("intIpCityName"), "intIpCityName").getValue());
        Assert.assertEquals(
          "Minneapolis",
          Utils.checkNotNull(result.get("stringIpCityName"), "stringIpCityName").getValue()
        );
      }
      Assert.assertEquals(
          "United States",
          Utils.checkNotNull(result.get("stringIpCountry"), "stringIpCountry").getValue()
      );
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testInvalidInputField() throws Exception {
    List<GeolocationFieldConfig> configs = new ArrayList<>();
    GeolocationFieldConfig config;
    config = new GeolocationFieldConfig();
    config.inputFieldName = "/notAValidField";
    config.outputFieldName = "/intIpCountry";
    config.targetType = GeolocationField.COUNTRY_NAME;
    configs.add(config);

    List<GeolocationDatabaseConfig> dbConfigs = new ArrayList<>();
    GeolocationDatabaseConfig dbConfig = new GeolocationDatabaseConfig();
    dbConfig.geoIP2DBFile = countryDb.getAbsolutePath();
    dbConfig.geoIP2DBType = GeolocationDBType.COUNTRY;
    dbConfigs.add(dbConfig);

    ProcessorRunner runner = new ProcessorRunner.Builder(GeolocationDProcessor.class)
        .addConfiguration("fieldTypeConverterConfigs", configs)
        .addConfiguration("dbConfigs", dbConfigs)
        .addConfiguration("missingAddressAction", GeolocationMissingAddressAction.REPLACE_WITH_NULLS)
        .addOutputLane("a").build();
    runner.runInit();

    boolean exceptionTriggered = false;
    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("ipAsInt", Field.create(ipAsStringToInt(ip)));
      map.put("ipAsIntString", Field.create(String.valueOf(ipAsStringToInt(ip))));
      map.put("ipAsString", Field.create(ip));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    } catch(OnRecordErrorException ex) {
      Assert.assertTrue(ex.getMessage().contains("GEOIP_11"));
      exceptionTriggered = true;
    } finally {
      runner.runDestroy();
    }

    Assert.assertTrue(exceptionTriggered);
  }


  @Test
  public void testNullInputFieldValue() throws Exception {
    List<GeolocationFieldConfig> configs = new ArrayList<>();
    GeolocationFieldConfig config;
    config = new GeolocationFieldConfig();
    config.inputFieldName = "/ipAsInt";
    config.outputFieldName = "/intIpCountry";
    config.targetType = GeolocationField.COUNTRY_NAME;
    configs.add(config);

    List<GeolocationDatabaseConfig> dbConfigs = new ArrayList<>();
    GeolocationDatabaseConfig dbConfig = new GeolocationDatabaseConfig();
    dbConfig.geoIP2DBFile = countryDb.getAbsolutePath();
    dbConfig.geoIP2DBType = GeolocationDBType.COUNTRY;
    dbConfigs.add(dbConfig);

    ProcessorRunner runner = new ProcessorRunner.Builder(GeolocationDProcessor.class)
        .addConfiguration("fieldTypeConverterConfigs", configs)
        .addConfiguration("dbConfigs", dbConfigs)
        .addConfiguration("missingAddressAction", GeolocationMissingAddressAction.REPLACE_WITH_NULLS)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .addOutputLane("a").build();
    runner.runInit();

    boolean exceptionTriggered = false;
    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("ipAsInt", Field.create((String)null));
      map.put("ipAsIntString", Field.create(String.valueOf(ipAsStringToInt(ip))));
      map.put("ipAsString", Field.create(ip));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));
      runner.runProcess(ImmutableList.of(record));
    } catch(OnRecordErrorException ex) {
      Assert.assertTrue(ex.getMessage().contains("GEOIP_13"));
      exceptionTriggered = true;
    } finally {
      runner.runDestroy();
    }

    Assert.assertTrue(exceptionTriggered);
  }

  @Test
  public void testMissingInputFieldValue() throws Exception {
    List<GeolocationFieldConfig> configs = new ArrayList<>();
    GeolocationFieldConfig config;
    config = new GeolocationFieldConfig();
    config.inputFieldName = "/ipAsInt";
    config.outputFieldName = "/intIpCountry";
    config.targetType = GeolocationField.COUNTRY_NAME;
    configs.add(config);

    List<GeolocationDatabaseConfig> dbConfigs = new ArrayList<>();
    GeolocationDatabaseConfig dbConfig = new GeolocationDatabaseConfig();
    dbConfig.geoIP2DBFile = countryDb.getAbsolutePath();
    dbConfig.geoIP2DBType = GeolocationDBType.COUNTRY;
    dbConfigs.add(dbConfig);

    ProcessorRunner runner = new ProcessorRunner.Builder(GeolocationDProcessor.class)
        .addConfiguration("fieldTypeConverterConfigs", configs)
        .addConfiguration("dbConfigs", dbConfigs)
        .addConfiguration("missingAddressAction", GeolocationMissingAddressAction.REPLACE_WITH_NULLS)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("a").build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    runner.runProcess(ImmutableList.of(record));

    Assert.assertEquals(1, runner.getErrorRecords().size());
  }

  @Test
  public void testMissingAddress() throws Exception {
    String ip = "0.0.0.0";
    List<GeolocationFieldConfig> configs = new ArrayList<>();
    for(int i = 0; i < 2; i++) {
      GeolocationFieldConfig config;
      config = new GeolocationFieldConfig();
      config.inputFieldName = "/ipAsString";
      config.outputFieldName = Utils.format("/ipCountry_{}", i);
      config.targetType = GeolocationField.COUNTRY_NAME;
      configs.add(config);
    }

    List<GeolocationDatabaseConfig> dbConfigs = new ArrayList<>();
    GeolocationDatabaseConfig dbConfig = new GeolocationDatabaseConfig();
    dbConfig.geoIP2DBFile = countryDb.getAbsolutePath();
    dbConfig.geoIP2DBType = GeolocationDBType.COUNTRY;
    dbConfigs.add(dbConfig);

    ProcessorRunner runner = new ProcessorRunner.Builder(GeolocationDProcessor.class)
        .addConfiguration("fieldTypeConverterConfigs", configs)
        .addConfiguration("dbConfigs", dbConfigs)
        .addConfiguration("missingAddressAction", GeolocationMissingAddressAction.REPLACE_WITH_NULLS)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("a").build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("ipAsString", Field.create(ip));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(output.getRecords().get("a").get(0).get("/ipCountry_0").getValue(), null);
    Assert.assertEquals(output.getRecords().get("a").get(0).get("/ipCountry_1").getValue(), null);

    runner = new ProcessorRunner.Builder(GeolocationDProcessor.class)
        .addConfiguration("fieldTypeConverterConfigs", configs)
        .addConfiguration("dbConfigs", dbConfigs)
        .addConfiguration("missingAddressAction", GeolocationMissingAddressAction.TO_ERROR)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("a").build();
    runner.runInit();

    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(1, runner.getErrorRecords().size());
  }

  // SDC-4990
  @Test
  public void testIncorrectIPAddress() throws Exception {
    String ip = "127.0"; // ... 0.1
    List<GeolocationFieldConfig> configs = new ArrayList<>();
    GeolocationFieldConfig config;
    config = new GeolocationFieldConfig();
    config.inputFieldName = "/ipAsString";
    config.outputFieldName = "/ipCountry";
    config.targetType = GeolocationField.COUNTRY_NAME;
    configs.add(config);

    List<GeolocationDatabaseConfig> dbConfigs = new ArrayList<>();
    GeolocationDatabaseConfig dbConfig = new GeolocationDatabaseConfig();
    dbConfig.geoIP2DBFile = countryDb.getAbsolutePath();
    dbConfig.geoIP2DBType = GeolocationDBType.COUNTRY;
    dbConfigs.add(dbConfig);

    ProcessorRunner runner = new ProcessorRunner.Builder(GeolocationDProcessor.class)
        .addConfiguration("fieldTypeConverterConfigs", configs)
        .addConfiguration("dbConfigs", dbConfigs)
        .addConfiguration("missingAddressAction", GeolocationMissingAddressAction.REPLACE_WITH_NULLS)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("a").build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("ipAsString", Field.create(ip));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

    runner = new ProcessorRunner.Builder(GeolocationDProcessor.class)
        .addConfiguration("fieldTypeConverterConfigs", configs)
        .addConfiguration("dbConfigs", dbConfigs)
        .addConfiguration("missingAddressAction", GeolocationMissingAddressAction.TO_ERROR)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("a").build();
    runner.runInit();

    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(1, runner.getErrorRecords().size());
  }

  @Test
  public void testClusterModeHadoopDbFileAbsPath() {
    List<GeolocationFieldConfig> configs = new ArrayList<>();
    GeolocationFieldConfig config;
    config = new GeolocationFieldConfig();
    config.inputFieldName = "/ipAsInt";
    config.outputFieldName = "/intIpCountry";
    config.targetType = GeolocationField.COUNTRY_NAME;
    configs.add(config);

    List<GeolocationDatabaseConfig> dbConfigs = new ArrayList<>();
    GeolocationDatabaseConfig dbConfig = new GeolocationDatabaseConfig();
    dbConfig.geoIP2DBFile = countryDb.getAbsolutePath();
    dbConfig.geoIP2DBType = GeolocationDBType.COUNTRY;
    dbConfigs.add(dbConfig);

    ProcessorRunner runner = new ProcessorRunner.Builder(GeolocationDProcessor.class)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .addConfiguration("dbConfigs", dbConfigs)
      .addConfiguration("missingAddressAction", GeolocationMissingAddressAction.REPLACE_WITH_NULLS)
      .addConfiguration("fieldTypeConverterConfigs", configs)
      .setExecutionMode(ExecutionMode.CLUSTER_BATCH)
      .addOutputLane("a").build();
    try {
      runner.runInit();
      Assert.fail(Utils.format("Expected StageException as absolute database file path '{}' is specified in cluster mode",
          countryDb.getAbsolutePath()));
    } catch (StageException e) {
      Assert.assertTrue(e.getMessage().contains("GEOIP_10"));
    }
  }

  private static int ipAsStringToInt(String s) {
    return InetAddresses.coerceToInteger(InetAddresses.forString(s));
  }

  private static byte[] ipAsIntToBytes(int ip) {
    return new byte[] {
        (byte)(ip >> 24),
        (byte)(ip >> 16),
        (byte)(ip >> 8),
        (byte)(ip & 0xff)
    };
  }

  private static int ipAsBytesToInt(byte[] ip) {
    int result = 0;
    for (byte b: ip) {
      result = result << 8 | (b & 0xFF);
    }
    return result;
  }

  private static String ipAsIntToString(int ip) {
    return String.format("%d.%d.%d.%d",
        (ip >> 24 & 0xff),
        (ip >> 16 & 0xff),
        (ip >> 8 & 0xff),
        (ip & 0xff));
  }

  private static byte[] ipAsStringToBytes(String ip) throws OnRecordErrorException {
    return ipAsIntToBytes(ipAsStringToInt(ip));
  }

  @Test
  public void testUnknownLocation() throws Exception {
    List<GeolocationFieldConfig> configs = new ArrayList<>();
    GeolocationFieldConfig config;
    config = new GeolocationFieldConfig();
    config.inputFieldName = "/ip";
    config.outputFieldName = "/lat";
    config.targetType = GeolocationField.LATITUDE;
    configs.add(config);
    config = new GeolocationFieldConfig();
    config.inputFieldName = "/ip";
    config.outputFieldName = "/lon";
    config.targetType = GeolocationField.LONGITUDE;
    configs.add(config);

    List<GeolocationDatabaseConfig> dbConfigs = new ArrayList<>();
    GeolocationDatabaseConfig dbConfig = new GeolocationDatabaseConfig();
    dbConfig.geoIP2DBFile = cityDb.getAbsolutePath();
    dbConfig.geoIP2DBType = GeolocationDBType.CITY;
    dbConfigs.add(dbConfig);

    ProcessorRunner runner = new ProcessorRunner.Builder(GeolocationDProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", configs)
      .addConfiguration("dbConfigs", dbConfigs)
      .addConfiguration("missingAddressAction", GeolocationMissingAddressAction.IGNORE)
      .addOutputLane("a").build();
    runner.runInit();
    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("ip", Field.create("157.5.65.83"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();

      Assert.assertTrue(result.containsKey("ip"));
      Assert.assertTrue(result.containsKey("lat"));
      Assert.assertTrue(result.containsKey("lon"));
      Assert.assertNull(result.get("lat").getValue());
      Assert.assertNull(result.get("lon").getValue());
    } finally {
      runner.runDestroy();
    }
  }

}
