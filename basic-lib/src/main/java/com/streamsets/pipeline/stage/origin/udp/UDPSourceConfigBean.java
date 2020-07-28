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
package com.streamsets.pipeline.stage.origin.udp;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.CharsetChooserValues;
import com.streamsets.pipeline.config.DatagramMode;
import com.streamsets.pipeline.config.DatagramModeChooserValues;
import com.streamsets.pipeline.lib.parser.net.netflow.NetflowDataParserFactory;
import com.streamsets.pipeline.lib.parser.net.netflow.OutputValuesMode;
import com.streamsets.pipeline.lib.parser.net.netflow.OutputValuesModeChooserValues;
import com.streamsets.pipeline.lib.parser.net.raw.RawDataMode;
import com.streamsets.pipeline.lib.parser.net.raw.RawDataModeChooserValues;
import com.streamsets.pipeline.lib.parser.udp.AbstractParser;
import com.streamsets.pipeline.lib.parser.udp.ParserConfig;
import com.streamsets.pipeline.lib.parser.udp.collectd.CollectdParser;
import com.streamsets.pipeline.lib.parser.udp.netflow.NetflowParser;
import com.streamsets.pipeline.lib.parser.udp.separated.SeparatedDataParser;
import com.streamsets.pipeline.lib.parser.udp.syslog.SyslogParser;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import com.streamsets.pipeline.stage.common.MultipleValuesBehaviorChooserValues;
import io.netty.channel.epoll.Epoll;
import org.apache.commons.lang3.StringEscapeUtils;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.LinkedList;
import java.util.List;

import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.AUTH_FILE_PATH;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.CHARSET;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.CONVERT_TIME;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.EXCLUDE_INTERVAL;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.NETFLOW_MAX_TEMPLATE_CACHE_SIZE;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.NETFLOW_OUTPUT_VALUES_MODE;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.NETFLOW_TEMPLATE_CACHE_TIMEOUT_MS;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.RAW_DATA_MODE;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.RAW_DATA_MULTIPLE_VALUES_BEHAVIOR;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.RAW_DATA_OUTPUT_FIELD_PATH;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.RAW_DATA_SEPARATOR_BYTES;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.TYPES_DB_PATH;

public class UDPSourceConfigBean {
  public static final String DEFAULT_RAW_DATA_MODE_STR = "CHARACTER";
  public static final RawDataMode DEFAULT_RAW_DATA_MODE = RawDataMode.valueOf(DEFAULT_RAW_DATA_MODE_STR);
  public static final String DEFAULT_RAW_DATA_CHARSET = "UTF-8";
  public static final String DEFAULT_RAW_DATA_OUTPUT_FIELD = "/data";
  public static final String DEFAULT_RAW_DATA_MULTI_VALUES_BEHAVIOR_STR = "FIRST_ONLY";
  public static final MultipleValuesBehavior DEFAULT_RAW_DATA_MULTI_VALUES_BEHAVIOR = MultipleValuesBehavior.valueOf(
      DEFAULT_RAW_DATA_MULTI_VALUES_BEHAVIOR_STR
  );
  public static final String DEFAULT_RAW_DATA_SEPARATOR_BYTES = "\\u000A";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Port",
      defaultValue = "[\"9995\"]",
      description = "Port to listen on",
      group = "UDP",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public List<String> ports; // string so we can listen on multiple ports in the future

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Native Transports (epoll)",
      description = "Use multiple receiver threads for each port. Only available on 64-bit Linux systems",
      defaultValue = "false",
      group = "UDP",
      displayPosition = 15,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public boolean enableEpoll;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Number of Receiver Threads",
      description = "Number of receiver threads for each port.  This controls the number of epoll threads bound to" +
          " each port.",
      defaultValue = "1",
      group = "UDP",
      dependsOn = "enableEpoll",
      triggeredByValue = "true",
      displayPosition = 16,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public int numThreads;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      defaultValue = "SYSLOG",
      group = "UDP",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(DatagramModeChooserValues.class)
  public DatagramMode dataFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Batch Size (messages)",
      group = "UDP",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 0,
      max = Integer.MAX_VALUE
  )
  public int batchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Batch Wait Time (ms)",
      description = "Max time to wait for data before sending a batch",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "UDP",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxWaitTime;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTF-8",
      label = "Charset",
      displayPosition = 5,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "SYSLOG",
      dependsOn = "dataFormat",
      triggeredByValue = "SYSLOG"
  )
  @ValueChooserModel(CharsetChooserValues.class)
  public String syslogCharset;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "TypesDB File Path",
      description = "User-specified TypesDB file. Overrides the included version.",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "COLLECTD",
      dependsOn = "dataFormat",
      triggeredByValue = "COLLECTD"
  )
  public String typesDbPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Convert Hi-Res Time & Interval",
      description = "Converts high resolution time format interval and timestamp to unix time in (ms).",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "COLLECTD",
      dependsOn = "dataFormat",
      triggeredByValue = "COLLECTD"
  )
  public boolean convertTime;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Exclude Interval",
      description = "Excludes the interval field from output records.",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "COLLECTD",
      dependsOn = "dataFormat",
      triggeredByValue = "COLLECTD"
  )
  public boolean excludeInterval;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Auth File",
      description = "",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "COLLECTD",
      dependsOn = "dataFormat",
      triggeredByValue = "COLLECTD"
  )
  public String authFilePath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTF-8",
      label = "Charset",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "COLLECTD",
      dependsOn = "dataFormat",
      triggeredByValue = "COLLECTD"
  )
  @ValueChooserModel(CharsetChooserValues.class)
  public String collectdCharset;

  // Netflow v9
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = NetflowDataParserFactory.DEFAULT_OUTPUT_VALUES_MODE_STR,
      label = NetflowDataParserFactory.OUTPUT_VALUES_MODE_LABEL,
      description = NetflowDataParserFactory.OUTPUT_VALUES_MODE_TOOLTIP,
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "NETFLOW_V9",
      dependsOn = "dataFormat",
      triggeredByValue = "NETFLOW"
  )
  @ValueChooserModel(OutputValuesModeChooserValues.class)
  public OutputValuesMode netflowOutputValuesMode = NetflowDataParserFactory.DEFAULT_OUTPUT_VALUES_MODE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = NetflowDataParserFactory.DEFAULT_MAX_TEMPLATE_CACHE_SIZE_STR,
      label = NetflowDataParserFactory.MAX_TEMPLATE_CACHE_SIZE_LABEL,
      description = NetflowDataParserFactory.MAX_TEMPLATE_CACHE_SIZE_TOOLTIP,
      displayPosition = 90,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "NETFLOW_V9",
      dependsOn = "dataFormat",
      triggeredByValue = "NETFLOW"
  )
  public int maxTemplateCacheSize = NetflowDataParserFactory.DEFAULT_MAX_TEMPLATE_CACHE_SIZE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = NetflowDataParserFactory.DEFAULT_TEMPLATE_CACHE_TIMEOUT_MS_STR,
      label = NetflowDataParserFactory.TEMPLATE_CACHE_TIMEOUT_MS_LABEL,
      description = NetflowDataParserFactory.TEMPLATE_CACHE_TIMEOUT_MS_TOOLTIP,
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "NETFLOW_V9",
      dependsOn = "dataFormat",
      triggeredByValue = "NETFLOW"
  )
  public int templateCacheTimeoutMs = NetflowDataParserFactory.DEFAULT_TEMPLATE_CACHE_TIMEOUT_MS;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = DEFAULT_RAW_DATA_MODE_STR,
      label = "Raw Data Mode",
      description = "The mode that controls how the raw packet data should be treated (character-based or binary)." +
          " This selection determines what type of field will be created.",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "RAW_DATA",
      dependsOn = "dataFormat",
      triggeredByValue = "RAW_DATA"
  )
  @ValueChooserModel(RawDataModeChooserValues.class)
  public RawDataMode rawDataMode = DEFAULT_RAW_DATA_MODE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = DEFAULT_RAW_DATA_CHARSET,
      label = "Charset",
      description = "The character set used to interpret character-based separated data.",
      displayPosition = 120,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "RAW_DATA",
      dependsOn = "rawDataMode",
      triggeredByValue = "CHARACTER"
  )
  @ValueChooserModel(CharsetChooserValues.class)
  public String rawDataCharset = DEFAULT_RAW_DATA_CHARSET;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = DEFAULT_RAW_DATA_OUTPUT_FIELD,
      label = "Output field path",
      description = "The output field path to place the separated data values into.",
      displayPosition = 150,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "RAW_DATA",
      dependsOn = "dataFormat",
      triggeredByValue = "RAW_DATA"
  )
  @FieldSelectorModel(singleValued = true)
  public String rawDataOutputField = DEFAULT_RAW_DATA_OUTPUT_FIELD;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Multiple Values Behavior",
      description = "How to handle multiple values produced by the parser after applying the separator.",
      defaultValue = DEFAULT_RAW_DATA_MULTI_VALUES_BEHAVIOR_STR,
      displayPosition = 160,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "RAW_DATA",
      dependsOn = "dataFormat",
      triggeredByValue = "RAW_DATA"
  )
  @ValueChooserModel(MultipleValuesBehaviorChooserValues.class)
  public MultipleValuesBehavior rawDataMultipleValuesBehavior = DEFAULT_RAW_DATA_MULTI_VALUES_BEHAVIOR;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Data Separator",
      description = "The bytes to use to separate data in the UDP packet.  If multiple values are found in a packet" +
          " after applying this separator, then the Multiple Values Behavior setting comes into play..  Specify byte" +
          " literals using using Java Unicode syntax (\"\\uxxxx\").  To capture the entire UDP packet (i.e. do not split" +
          " using any delimiter), leave this blank.  Defaults to line feed (000A).",
      defaultValue = DEFAULT_RAW_DATA_SEPARATOR_BYTES,
      group = "RAW_DATA",
      dependsOn = "dataFormat",
      triggeredByValue = "RAW_DATA",
      displayPosition = 170,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String rawDataSeparatorBytes = DEFAULT_RAW_DATA_SEPARATOR_BYTES;

  private ParserConfig parserConfig = new ParserConfig();
  private boolean privilegedPortUsage;
  private final List<InetSocketAddress> addresses = new LinkedList<>();
  private AbstractParser parser;

  public boolean isPrivilegedPortUsage() {
    return privilegedPortUsage;
  }

  public List<InetSocketAddress> getAddresses() {
    return addresses;
  }

  public AbstractParser getParser() {
    return parser;
  }

  public boolean init(Stage.Context context, List<Stage.ConfigIssue> issues) {

    boolean valid = true;

    switch (dataFormat) {
      case SYSLOG:
        parserConfig.put(CHARSET, syslogCharset);
        break;
      case COLLECTD:
        parserConfig.put(CHARSET, collectdCharset);
        break;
      case RAW_DATA:
        parserConfig.put(CHARSET, rawDataCharset);
        parserConfig.put(RAW_DATA_MODE, rawDataMode);
        parserConfig.put(RAW_DATA_MULTIPLE_VALUES_BEHAVIOR, rawDataMultipleValuesBehavior);
        parserConfig.put(RAW_DATA_OUTPUT_FIELD_PATH, rawDataOutputField);
        parserConfig.put(
            RAW_DATA_SEPARATOR_BYTES,
            StringEscapeUtils.unescapeJava(rawDataSeparatorBytes).getBytes()
        );
        break;
      case NETFLOW:
        parserConfig.put(NETFLOW_OUTPUT_VALUES_MODE, netflowOutputValuesMode);
        parserConfig.put(NETFLOW_MAX_TEMPLATE_CACHE_SIZE, maxTemplateCacheSize);
        parserConfig.put(NETFLOW_TEMPLATE_CACHE_TIMEOUT_MS, templateCacheTimeoutMs);
        break;
      default:
        // NOOP
    }

    parserConfig.put(CONVERT_TIME, convertTime);
    parserConfig.put(TYPES_DB_PATH, typesDbPath);
    parserConfig.put(EXCLUDE_INTERVAL, excludeInterval);
    parserConfig.put(AUTH_FILE_PATH, authFilePath);


    if (enableEpoll && !Epoll.isAvailable()) {
      issues.add(context.createConfigIssue(Groups.UDP.name(), "enableEpoll", Errors.UDP_08));
    }
    if (ports.isEmpty()) {
      issues.add(context.createConfigIssue(Groups.UDP.name(), "ports", Errors.UDP_02));
    } else {
      for (String candidatePort : ports) {
        try {
          int port = Integer.parseInt(candidatePort.trim());
          if (port > 0 && port < 65536) {
            if (port < 1024) {
              privilegedPortUsage = true; // only for error handling purposes
            }
            addresses.add(new InetSocketAddress(port));
          } else {
            issues.add(context.createConfigIssue(Groups.UDP.name(), "ports", Errors.UDP_03, port));
          }
        } catch (NumberFormatException ex) {
          issues.add(context.createConfigIssue(Groups.UDP.name(), "ports", Errors.UDP_03, candidatePort));
        }
      }
    }

    Charset charset;
    switch (dataFormat) {
      case NETFLOW:
        final int maxTemplateCacheSize = parserConfig.getInteger(NETFLOW_MAX_TEMPLATE_CACHE_SIZE);
        final int templateCacheTimeoutMs = parserConfig.getInteger(NETFLOW_TEMPLATE_CACHE_TIMEOUT_MS);
        NetflowDataParserFactory.validateConfigs(
            context,
            issues,
            Groups.NETFLOW_V9.name(),
            "",
            maxTemplateCacheSize,
            templateCacheTimeoutMs
        );
        parser = new NetflowParser(
            context,
            (OutputValuesMode) parserConfig.get(NETFLOW_OUTPUT_VALUES_MODE),
            maxTemplateCacheSize,
            templateCacheTimeoutMs
        );
        break;
      case SYSLOG:
        charset = validateCharset(Groups.SYSLOG.name(), context, issues);
        parser = new SyslogParser(context, charset);
        break;
      case COLLECTD:
        charset = validateCharset(Groups.COLLECTD.name(), context, issues);
        checkCollectdParserConfigs(context, issues);
        if (issues.isEmpty()) {
          parser = new CollectdParser(
              context,
              parserConfig.getBoolean(CONVERT_TIME),
              parserConfig.getString(TYPES_DB_PATH),
              parserConfig.getBoolean(EXCLUDE_INTERVAL),
              parserConfig.getString(AUTH_FILE_PATH),
              charset
          );
        }
        break;
      case RAW_DATA:
        charset = validateCharset(Groups.RAW_DATA.name(), context, issues);
        parser = new SeparatedDataParser(
            context,
            (RawDataMode) parserConfig.get(RAW_DATA_MODE),
            charset,
            parserConfig.getString(RAW_DATA_OUTPUT_FIELD_PATH),
            (MultipleValuesBehavior) parserConfig.get(RAW_DATA_MULTIPLE_VALUES_BEHAVIOR),
            (byte[]) parserConfig.get(RAW_DATA_SEPARATOR_BYTES)
        );
        break;
      default:
        issues.add(context.createConfigIssue(Groups.UDP.name(), "dataFormat",
            Errors.UDP_01, dataFormat));
        break;
    }

    return valid;

  }


  private Charset validateCharset(String groupName, Stage.Context context, List<Stage.ConfigIssue> issues) {
    Charset charset;
    try {
      charset = Charset.forName(parserConfig.getString(CHARSET));
    } catch (UnsupportedCharsetException ex) {
      charset = StandardCharsets.UTF_8;
      issues.add(context.createConfigIssue(groupName, "charset", Errors.UDP_04, charset));
    }
    return charset;
  }

  private void checkCollectdParserConfigs(Stage.Context context, List<Stage.ConfigIssue> issues) {
    String typesDbLocation = parserConfig.getString(TYPES_DB_PATH);
    if (!typesDbLocation.isEmpty()) {
      File typesDbFile = new File(typesDbLocation);
      if (!typesDbFile.canRead() || !typesDbFile.isFile()) {
        issues.add(context.createConfigIssue(Groups.COLLECTD.name(), "typesDbPath", Errors.UDP_05, typesDbLocation)
        );
      }
    }
    String authFileLocation = parserConfig.getString(AUTH_FILE_PATH);
    if (!authFileLocation.isEmpty()) {
      File authFile = new File(authFileLocation);
      if (!authFile.canRead() || !authFile.isFile()) {
        issues.add(
            context.createConfigIssue(Groups.COLLECTD.name(), "authFilePath", Errors.UDP_06, authFileLocation)
        );
      }
    }
  }

}
