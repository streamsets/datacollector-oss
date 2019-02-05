/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.lib;

import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.common.DataFormatConstants;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.flowfile.FlowFileParserFactory;
import com.streamsets.pipeline.stage.common.DataFormatErrors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class OuterDataParserFormatConfig extends DataParserFormatConfig  {

  private static final Logger LOG = LoggerFactory.getLogger(OuterDataParserFormatConfig.class);

  private final DataFormat outerDataFormat;
  private final DataFormat innerDataFormat;
  private final DataParserFormatConfig innerConfig;

  // For NiFi FlowFile
  private FlowFileVersion version;

  public OuterDataParserFormatConfig(
      DataFormat outerDataFormat,
      DataFormat innerDataFormat,
      DataParserFormatConfig dataFormatConfig,
      Object ...args
  ) {
    this.outerDataFormat = outerDataFormat;
    this.innerDataFormat = innerDataFormat;
    this.innerConfig = dataFormatConfig;
    if (outerDataFormat == DataFormat.FLOWFILE) {
      this.version = (FlowFileVersion)args[0];
    }
  }

  /**
   * The init() in this class needs to initialize the embedded DataParserFormatConfig
   * and then initialize DataParserFormatConfig for the parent(custom) data format.
   * @param context
   * @param dataFormat
   * @param stageGroup
   * @param configPrefix
   * @param issues
   * @return
   */
  @Override
  public boolean init(
      ProtoConfigurableEntity.Context context,
      DataFormat dataFormat,
      String stageGroup,
      String configPrefix,
      List<Stage.ConfigIssue> issues
  ) {
    return init(
        context,
        dataFormat,
        stageGroup,
        configPrefix,
        DataFormatConstants.MAX_OVERRUN_LIMIT,
        false,
        issues
    );
  }

  @Override
  public boolean init(
      ProtoConfigurableEntity.Context context,
      DataFormat dataFormat,
      String stageGroup,
      String configPrefix,
      int overrunLimit,
      boolean multiLines,
      List<Stage.ConfigIssue> issues
  ) {
    boolean valid = false;
    DataParserFactoryBuilder builder = null;

    if (dataFormat == null) {
      issues.add(context.createConfigIssue(
          stageGroup,
          configPrefix + "dataFormat",
          DataFormatErrors.DATA_FORMAT_12,
          dataFormat
      ));
      return false;
    }

    // init DataParserFormatConfig for content of FlowFile
    valid = innerConfig.init(context, innerDataFormat, stageGroup, configPrefix, issues);
    if (!valid) {
      return false;
    }

    // Now building FactoryBuilder for outer data format
    builder = new DataParserFactoryBuilder(context, dataFormat.getParserFormat());
    valid = isValid(context, stageGroup, configPrefix, overrunLimit, issues, builder);

    if (valid) {
      if (dataFormat == DataFormat.FLOWFILE) {
        buildFlowFileParser(builder);
      }

      try {
        parserFactory = builder.build();
      } catch (Exception ex) {
        LOG.error("Can't create parserFactory", ex);
        issues.add(context.createConfigIssue(null, null, DataFormatErrors.DATA_FORMAT_06, ex.toString(), ex));
        valid = false;
      }
    }
    return valid;
  }

  protected void buildFlowFileParser(DataParserFactoryBuilder builder) {
    builder.setMaxDataLen(-1);
    // pass the parserFactory for the embedded data format to the FlowFileDataParserFactory
    builder.setConfig(FlowFileParserFactory.CONTENT_PARSER_FACTORY, innerConfig.getParserFactory());
    builder.setConfig(FlowFileParserFactory.FLOWFILE_VERSION, version);
  }
}
