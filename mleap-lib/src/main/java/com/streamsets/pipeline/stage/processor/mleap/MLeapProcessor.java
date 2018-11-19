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
package com.streamsets.pipeline.stage.processor.mleap;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import ml.combust.mleap.core.types.BasicType;
import ml.combust.mleap.core.types.StructField;
import ml.combust.mleap.core.types.StructType;
import ml.combust.mleap.runtime.MleapContext;
import ml.combust.mleap.runtime.frame.DefaultLeapFrame;
import ml.combust.mleap.runtime.frame.Row;
import ml.combust.mleap.runtime.frame.Transformer;
import ml.combust.mleap.runtime.javadsl.BundleBuilder;
import ml.combust.mleap.runtime.javadsl.ContextBuilder;
import ml.combust.mleap.runtime.javadsl.LeapFrameBuilder;
import ml.combust.mleap.runtime.javadsl.LeapFrameSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MLeapProcessor extends SingleLaneProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(MLeapProcessor.class);
  private final MLeapProcessorConfigBean conf;
  private Map<String, String> fieldNameMap = new HashMap<>();
  private Transformer mLeapPipeline;
  private LeapFrameBuilder leapFrameBuilder;
  private LeapFrameSupport leapFrameSupport;
  private ErrorRecordHandler errorRecordHandler;

  MLeapProcessor(MLeapProcessorConfigBean conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> configIssues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    if (configIssues.isEmpty()) {
      try {
        File mLeapModel = new File(conf.modelPath);
        if (!mLeapModel.isAbsolute()) {
          mLeapModel = new File(getContext().getResourcesDirectory(), conf.modelPath).getAbsoluteFile();
        }
        MleapContext mleapContext = new ContextBuilder().createMleapContext();
        BundleBuilder bundleBuilder = new BundleBuilder();
        mLeapPipeline = bundleBuilder.load(mLeapModel, mleapContext).root();
      } catch (Exception ex) {
        configIssues.add(getContext().createConfigIssue(
            Groups.MLEAP.name(),
            MLeapProcessorConfigBean.MODEL_PATH_CONFIG,
            Errors.MLEAP_00,
            ex
        ));
        return configIssues;
      }

      leapFrameBuilder = new LeapFrameBuilder();
      leapFrameSupport = new LeapFrameSupport();

      for (InputConfig inputConfig : conf.inputConfigs) {
        fieldNameMap.put(inputConfig.pmmlFieldName, inputConfig.fieldName);
      }

      // Validate Input fields
      StructType inputSchema = mLeapPipeline.inputSchema();
      List<StructField> structFieldList = leapFrameSupport.getFields(inputSchema);
      List<String> missingInputFieldNames = structFieldList.stream()
          .filter(structField -> !fieldNameMap.containsKey(structField.name()))
          .map(StructField::name)
          .collect(Collectors.toList());
      if (!missingInputFieldNames.isEmpty()) {
        configIssues.add(getContext().createConfigIssue(
            Groups.MLEAP.name(),
            MLeapProcessorConfigBean.INPUT_CONFIGS_CONFIG,
            Errors.MLEAP_01,
            missingInputFieldNames
        ));
      }

      // Validate Output field names
      if (conf.outputFieldNames.size() == 0) {
        configIssues.add(getContext().createConfigIssue(
            Groups.MLEAP.name(),
            MLeapProcessorConfigBean.OUTPUT_FIELD_NAMES_CONFIG,
            Errors.MLEAP_02
        ));
        return configIssues;
      }

      StructType outputSchema = mLeapPipeline.outputSchema();
      List<StructField> outputStructFieldList = leapFrameSupport.getFields(outputSchema);
      List<String> outputFieldNamesCopy = new ArrayList<>(conf.outputFieldNames);
      outputFieldNamesCopy.removeAll(
          outputStructFieldList.stream().map(StructField::name).collect(Collectors.toList())
      );
      if (outputFieldNamesCopy.size() > 0) {
        configIssues.add(getContext().createConfigIssue(
            Groups.MLEAP.name(),
            MLeapProcessorConfigBean.OUTPUT_FIELD_NAMES_CONFIG,
            Errors.MLEAP_03,
            outputFieldNamesCopy
        ));
      }

    }
    return configIssues;
  }

  @Override
  public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws StageException {
    // MLeap supports scoring multiple point simultaneously (better performance) so processing batch instead of
    // record by record like it is done in other ML Evaluator processors
    List<Record> records = new ArrayList<>();
    batch.getRecords().forEachRemaining(records::add);
    DefaultLeapFrame inputLeapFrame = convertRecordsToLeapFrame(records);
    DefaultLeapFrame outputLeapFrame;
    try {
      outputLeapFrame = mLeapPipeline.transform(inputLeapFrame).get();
    } catch (Exception ex) {
      LOG.error(Utils.format(Errors.MLEAP_05.getMessage(), ex.getMessage()), ex);
      errorRecordHandler.onError(records, new StageException(Errors.MLEAP_05, ex.toString()));
      return;
    }
    processTransformOutput(records, batchMaker, outputLeapFrame);
  }

  private DefaultLeapFrame convertRecordsToLeapFrame(List<Record> records) throws StageException {
    StructType inputSchema = mLeapPipeline.inputSchema();
    List<StructField> structFieldList = leapFrameSupport.getFields(inputSchema);
    List<Row> mLeapRows = new ArrayList<>();

    List<Record> errorRecords = new ArrayList<>();

    for(Record record: records) {
      List<Object> rowValues = new ArrayList<>();
      boolean foundError = false;
      for (StructField structField: structFieldList) {
        Field input = record.get(fieldNameMap.get(structField.name()));
        if (input != null) {
          rowValues.add(input.getValue());
        } else {
          errorRecordHandler.onError(new OnRecordErrorException(
              record,
              Errors.MLEAP_04,
              fieldNameMap.get(structField.name()),
              record.getHeader().getSourceId()
          ));
          errorRecords.add(record);
          foundError = true;
        }
      }

      if (!foundError) {
        mLeapRows.add(leapFrameBuilder.createRowFromIterable(rowValues));
      }
    }

    // Remove all error records from records to list before it used in processTransformOutput
    records.removeAll(errorRecords);

    return leapFrameBuilder.createFrame(mLeapPipeline.inputSchema(), mLeapRows);
  }

  private void processTransformOutput(
      List<Record> records,
      SingleLaneBatchMaker batchMaker,
      DefaultLeapFrame outputFrame
  ) throws StageException {
    List<Row> outputRows = leapFrameSupport.collect(outputFrame);

    StructType outputSchema = outputFrame.schema();
    List<StructField> structFieldList = leapFrameSupport.getFields(outputSchema);

    for(Record record: records) {
      LinkedHashMap<String, Field> outputFieldMap = new LinkedHashMap<>();
      Row mLeapOutputRow = outputRows.get(0);
      try {
        int i = 0;
        for (StructField structField: structFieldList) {
          if (conf.outputFieldNames.contains(structField.name())) {
            outputFieldMap.put(structField.name(), convertToField(mLeapOutputRow, i, structField));
          }
          i++;
        }
        record.set(conf.outputField, Field.createListMap(outputFieldMap));
        batchMaker.addRecord(record);
      } catch (OnRecordErrorException ex) {
        errorRecordHandler.onError(new OnRecordErrorException(
            record,
            ex.getErrorCode(),
            ex.getParams()
         ));
      }
    }
  }

  private Field convertToField(Row mLeapOutputRow, int index, StructField structField) throws OnRecordErrorException {
    String structType = structField.dataType().simpleString();
    switch (structType) {
      case "list":
        List<Row> listValue = mLeapOutputRow.getList(index);
        List<Field> fieldList = new ArrayList<>();
        int i = 0;
        for (Row list : listValue) {
          Field field = getBasicTypeField(list, i++, structField);
          fieldList.add(field);
        }
        return Field.create(fieldList);
      case "scalar":
        return getBasicTypeField(mLeapOutputRow, index, structField);
      default:
        throw new OnRecordErrorException(
            Errors.MLEAP_06,
            structField.name(),
            structType
        );
    }
  }

  private Field getBasicTypeField(
      Row mLeapOutputRow,
      int index,
      StructField structField
  ) throws OnRecordErrorException {
    BasicType basicType = structField.dataType().base();
    switch (basicType.toString()) {
      case "byte":
        return Field.create(mLeapOutputRow.getByte(index));
      case "short":
        return Field.create(mLeapOutputRow.getShort(index));
      case "int":
        return Field.create(mLeapOutputRow.getInt(index));
      case "long":
        return Field.create(mLeapOutputRow.getLong(index));
      case "float":
        return Field.create(mLeapOutputRow.getFloat(index));
      case "double":
        return Field.create(mLeapOutputRow.getDouble(index));
      case "boolean":
        return Field.create(mLeapOutputRow.getBool(index));
      case "string":
        return Field.create(mLeapOutputRow.getString(index));
      case "byte_string":
        return Field.create(Field.Type.BYTE_ARRAY, mLeapOutputRow.getByteString(index));
      default:
        throw new OnRecordErrorException(
            Errors.MLEAP_06,
            structField.name(),
            basicType.toString()
        );
    }
  }

  @Override
  public void destroy() {
    super.destroy();
    if (mLeapPipeline != null) {
      mLeapPipeline.close();
    }
  }
}
