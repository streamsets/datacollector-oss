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
package com.streamsets.service.generator;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.BaseService;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.service.ServiceDef;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.api.service.dataformats.DataGenerator;
import com.streamsets.pipeline.api.service.dataformats.DataGeneratorException;
import com.streamsets.pipeline.api.service.dataformats.WholeFileChecksumAlgorithm;
import com.streamsets.pipeline.api.service.dataformats.WholeFileExistsAction;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.service.lib.ShimUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

@ServiceDef(
  provides = DataFormatGeneratorService.class,
  version = 1,
  upgrader = GeneratorUpgrader.class,
  upgraderDef = "upgrader/DataFormatGeneratorUpgrader.yaml",
  label = "DataFormat Generator"
)
@ConfigGroups(Groups.class)
public class DataGeneratorServiceImpl extends BaseService implements DataFormatGeneratorService {

  private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorServiceImpl.class);

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
    description = "Data Format",
    displayPosition = 1,
    group = "DATA_FORMAT"
  )
  @ValueChooserModel(value = DataFormatChooserValues.class, filteringConfig = "displayFormats")
  public DataFormat dataFormat;

  @ConfigDefBean(groups = {"DATA_FORMAT"})
  public DataGeneratorFormatConfig dataGeneratorFormatConfig;

  @Override
  public List<ConfigIssue> init() {
    LOG.debug("Initializing dataformat service.");

    // Intentionally erasing the generic type as the init() method current works with Stage.ConfigIssue. This will
    // be changed once we convert all stages to use the service concept.
    List issues = new LinkedList();
    dataGeneratorFormatConfig.init(
      getContext(),
      dataFormat,
      "DATA_FORMAT",
      "dataGeneratorFormatConfig.",
      issues
    );

    return issues;
  }

  @Override
  public void destroy() {
    LOG.debug("Destroying DataFormat service.");
  }

  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException {
    return new DataGeneratorWraper(dataGeneratorFormatConfig.getDataGeneratorFactory().getGenerator(os));
  }

  @Override
  public boolean isPlainTextCompatible() {
    return dataFormat == DataFormat.TEXT || dataFormat == DataFormat.JSON || dataFormat == DataFormat.DELIMITED || dataFormat == DataFormat.XML;
  }

  @Override
  public String getCharset() {
    return dataGeneratorFormatConfig.charset;
  }

  @Override
  public boolean isWholeFileFormat() {
    return dataFormat == DataFormat.WHOLE_FILE;
  }

  @Override
  public String wholeFileFilename(Record record) throws StageException  {
    ELEval eval = getContext().createELEval("fileNameEL");
    ELVars vars = getContext().createELVars();
    RecordEL.setRecordInContext(vars, record);

    return eval.eval(vars, dataGeneratorFormatConfig.fileNameEL, String.class);
  }

  @Override
  public WholeFileExistsAction wholeFileExistsAction() {
    return dataGeneratorFormatConfig.wholeFileExistsAction.toApi();
  }

  @Override
  public boolean wholeFileIncludeChecksumInTheEvents() {
    return dataGeneratorFormatConfig.includeChecksumInTheEvents;
  }

  @Override
  public WholeFileChecksumAlgorithm wholeFileChecksumAlgorithm() {
    return dataGeneratorFormatConfig.checksumAlgorithm.toApi();
  }

  /**
   * Temporary wrapper to change DataGeneratorException from the *.lib.* to *.api.* as it's expected in the
   * service world. This will be removed once all stages gets migrated off the older code to services.
   */
  public static class DataGeneratorWraper implements DataGenerator {

    private final com.streamsets.pipeline.lib.generator.DataGenerator generator;

    DataGeneratorWraper(com.streamsets.pipeline.lib.generator.DataGenerator generator) {
      this.generator = generator;
    }

    @Override
    public void write(Record record) throws IOException, DataGeneratorException {
      try {
        generator.write(record);
      } catch (com.streamsets.pipeline.lib.generator.DataGeneratorException e) {
        throw ShimUtil.convert(e);
      }
    }

    @Override
    public void flush() throws IOException {
      generator.flush();
    }

    @Override
    public void close() throws IOException {
      generator.close();
    }
  }
}
