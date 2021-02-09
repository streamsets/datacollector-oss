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
package com.streamsets.service.parser;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.BaseService;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.service.ServiceDef;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.api.service.dataformats.DataParser;
import com.streamsets.pipeline.api.service.dataformats.DataParserException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import com.streamsets.service.lib.ShimUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@ServiceDef(
  provides = DataFormatParserService.class,
  version = 3,
  upgrader = ParserUpgrader.class,
  upgraderDef = "upgrader/DataFormatParserUpgrader.yaml",
  label = "DataFormat Parser"
)
@ConfigGroups(Groups.class)
public class DataParserServiceImpl extends BaseService implements DataFormatParserService {

  @ConfigDef(
    type = ConfigDef.Type.RUNTIME,
    required = false,
    label = "List of formats that should be displayed to the user."
  )
  public String displayFormats = "";

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Data Format",
    displayPosition = 1,
    group = "DATA_FORMAT"
  )
  @ValueChooserModel(value = DataFormatChooserValues.class, filteringConfig = "displayFormats")
  public DataFormat dataFormat;

  @ConfigDefBean
  public DataParserFormatConfig dataFormatConfig;

  @Override
  public List<ConfigIssue> init() {
    // Intentionally erasing the generic type as the init() method current works with Stage.ConfigIssue. This will
    // be changed once we convert all stages to use the service concept.
    List issues = new LinkedList();
    dataFormatConfig.init(
      getContext(),
      dataFormat,
      "DATA_FORMAT",
      "dataFormatConfig.",
      issues
    );

    return issues;
  }

  @Override
  public void destroy() {
  }


  @Override
  public DataParser getParser(String id, InputStream is, String offset) throws DataParserException {
    try {
      return new DataParserWrapper(dataFormatConfig.getParserFactory().getParser(id, is, offset));
    } catch (com.streamsets.pipeline.lib.parser.DataParserException e) {
      throw ShimUtil.convert(e);
    }
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    try {
      return new DataParserWrapper(dataFormatConfig.getParserFactory().getParser(id, reader, offset));
    } catch (com.streamsets.pipeline.lib.parser.DataParserException e) {
      throw ShimUtil.convert(e);
    }
  }

  @Override
  public DataParser getParser(String id, Map<String, Object> metadata, FileRef fileRef) throws DataParserException {
    try {
      return new DataParserWrapper(dataFormatConfig.getParserFactory().getParser(id, metadata, fileRef));
    } catch (com.streamsets.pipeline.lib.parser.DataParserException e) {
      throw ShimUtil.convert(e);
    }
  }

  @Override
  public String getCharset() {
    return dataFormatConfig.charset;
  }

  @Override
  @Deprecated
  public void setStringBuilderPoolSize(int poolSize) {
    this.dataFormatConfig.stringBuilderPoolSize = poolSize;
  }

  @Override
  @Deprecated
  public int getStringBuilderPoolSize() {
    return this.dataFormatConfig.stringBuilderPoolSize;
  }

  @Override
  public boolean isWholeFileFormat() {
    return this.dataFormat == DataFormat.WHOLE_FILE;
  }

  @Override
  public long suggestedWholeFileBufferSize() {
    return this.dataFormatConfig.wholeFileMaxObjectLen;
  }

  @Override
  public Double wholeFileRateLimit() throws StageException  {
    // TODO: There is no point in evaluating this every time, but that is what current code in S3 and other places does,
    // so we moved the logic here as is. Once all stages will be converted over, we will refactore this to evaluate only
    // once during init() and add ConfigIssue if the validation fails.
    ELEval rateLimitElEval = FileRefUtil.createElEvalForRateLimit(getContext());
    ELVars rateLimitElVars = getContext().createELVars();
    return FileRefUtil.evaluateAndGetRateLimit(rateLimitElEval, rateLimitElVars, dataFormatConfig.rateLimit);
  }

  @Override
  public boolean isWholeFileChecksumRequired() {
    return this.dataFormatConfig.verifyChecksum;
  }

  /**
   * Temporary wrapper to change DataGeneratorException from the *.lib.* to *.api.* as it's expected in the
   * service world. This will be removed once all stages gets migrated off the older code to services.
   */
  class DataParserWrapper implements DataParser {

    private com.streamsets.pipeline.lib.parser.DataParser parser;
    DataParserWrapper(com.streamsets.pipeline.lib.parser.DataParser parser) {
      this.parser = parser;
    }

    @Override
    public Record parse() throws IOException, DataParserException {
      try {
        return parser.parse();
      } catch (com.streamsets.pipeline.lib.parser.DataParserException e) {
        throw ShimUtil.convert(e);
      }
    }

    @Override
    public String getOffset() throws DataParserException, IOException {
      try {
        return parser.getOffset();
      } catch (com.streamsets.pipeline.lib.parser.DataParserException e) {
        throw ShimUtil.convert(e);
      }
    }

    @Override
    public void setTruncated() {
      parser.setTruncated();
    }

    @Override
    public void close() throws IOException {
      parser.close();
    }
  }
}
