/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.databricksml;

import com.databricks.ml.local.LocalModel;
import com.databricks.ml.local.ModelFactory;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.json.JsonCharDataGenerator;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.json.JsonCharDataParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

public class DatabricksMLProcessor extends SingleLaneRecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(DatabricksMLProcessor.class);
  private final DatabricksMLProcessorConfigBean conf;
  private LocalModel localModel;

  DatabricksMLProcessor(DatabricksMLProcessorConfigBean conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> configIssues = super.init();
    if (configIssues.isEmpty()) {
      try {
        File databrickMLModelDir = new File(conf.modelPath);
        if (!databrickMLModelDir.isAbsolute()) {
          databrickMLModelDir = new File(getContext().getResourcesDirectory(), conf.modelPath).getAbsoluteFile();
        }
        this.localModel = ModelFactory.loadModel(databrickMLModelDir.getAbsolutePath());
      } catch (Exception ex) {
        configIssues.add(getContext().createConfigIssue(
            Groups.DATABRICKS_ML.name(),
            DatabricksMLProcessorConfigBean.MODEL_PATH_CONFIG,
            Errors.DATABRICKS_ML_00,
            ex
        ));
        return configIssues;
      }

      localModel.setOutputCols(conf.outputColumns.toArray(new String[0]));
    }
    return configIssues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    String inputJsonStr = getModelInput(record);
    String outputJsonStr;
    try {
      // Model input and output is a standard JSON string.
      outputJsonStr = localModel.transform(inputJsonStr);
    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
      throw new OnRecordErrorException(
          Errors.DATABRICKS_ML_04,
          ex.toString(),
          ex
      );
    }
    processModelOutput(record, outputJsonStr);
    batchMaker.addRecord(record);
  }

  private String getModelInput(Record record) throws OnRecordErrorException {
    Field inputField = record.get(conf.inputField);
    if (inputField == null || inputField.getValue() == null) {
      throw new OnRecordErrorException(
          Errors.DATABRICKS_ML_03,
          conf.inputField,
          record.getHeader().getSourceId()
      );
    }

    Record inputFieldRecord = getContext().createRecord("inputFieldRecord");
    inputFieldRecord.set(inputField);
    StringWriter inputFieldStringWriter = new StringWriter();

    try (DataGenerator jsonDataGenerator = new JsonCharDataGenerator(
        getContext(),
        inputFieldStringWriter,
        Mode.MULTIPLE_OBJECTS
    )) {
      jsonDataGenerator.write(inputFieldRecord);
      jsonDataGenerator.flush();
    } catch (IOException | DataGeneratorException ex) {
      LOG.error(ex.getMessage(), ex);
      throw new OnRecordErrorException(
          Errors.DATABRICKS_ML_01,
          record.getHeader().getSourceId(),
          ex.toString(),
          ex
      );
    }

    return inputFieldStringWriter.toString();
  }

  private void processModelOutput(Record record, String outputJsonStr) throws OnRecordErrorException {
    try (OverrunReader reader = new OverrunReader(
        new StringReader(outputJsonStr), -1, false, false
    )) {
      JsonCharDataParser parser = new JsonCharDataParser(
          getContext(),
          "",
          reader,
          0,
          Mode.MULTIPLE_OBJECTS,
          -1
      );
      Field parsed = parser.parseAsField();
      record.set(conf.outputField, parsed);

    } catch (IOException | DataParserException ex) {
      LOG.error(ex.getMessage(), ex);
      throw new OnRecordErrorException(
          Errors.DATABRICKS_ML_02,
          record.getHeader().getSourceId(),
          ex.toString(),
          ex
      );
    }
  }
}
