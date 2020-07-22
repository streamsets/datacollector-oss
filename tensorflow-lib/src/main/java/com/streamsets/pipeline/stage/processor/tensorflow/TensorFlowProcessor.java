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
package com.streamsets.pipeline.stage.processor.tensorflow;

import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.util.FieldPathExpressionUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.processor.tensorflow.typesupport.TensorDataTypeSupport;
import com.streamsets.pipeline.stage.processor.tensorflow.typesupport.TensorTypeSupporter;
import org.apache.commons.lang3.tuple.Pair;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;
import org.tensorflow.TensorFlowException;

import java.io.File;
import java.nio.Buffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public final class TensorFlowProcessor extends SingleLaneProcessor {

  private final TensorFlowConfigBean conf;
  private SavedModelBundle savedModel;
  private Session session;
  private Map<Pair<String, Integer>, TensorInputConfig> inputConfigMap = new LinkedHashMap<>();
  private ErrorRecordHandler errorRecordHandler;
  private ELEval fieldPathEval;
  private ELVars fieldPathVars;

  TensorFlowProcessor(TensorFlowConfigBean conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    String[] modelTags = new String[conf.modelTags.size()];
    modelTags = conf.modelTags.toArray(modelTags);

    if (Strings.isNullOrEmpty(conf.modelPath)) {
      issues.add(getContext().createConfigIssue(
          Groups.TENSOR_FLOW.name(),
          TensorFlowConfigBean.MODEL_PATH_CONFIG,
          Errors.TENSOR_FLOW_01
      ));
      return issues;
    }

    try {
      File exportedModelDir = new File(conf.modelPath);
      if (!exportedModelDir.isAbsolute()) {
        exportedModelDir = new File(getContext().getResourcesDirectory(), conf.modelPath).getAbsoluteFile();
      }
      this.savedModel = SavedModelBundle.load(exportedModelDir.getAbsolutePath(), modelTags);
    } catch (TensorFlowException ex) {
      issues.add(getContext().createConfigIssue(
          Groups.TENSOR_FLOW.name(),
          TensorFlowConfigBean.MODEL_PATH_CONFIG,
          Errors.TENSOR_FLOW_02,
          ex
      ));
      return issues;
    }

    this.session = this.savedModel.session();
    this.conf.inputConfigs.forEach(inputConfig -> {
          Pair<String, Integer> key = Pair.of(inputConfig.operation, inputConfig.index);
          inputConfigMap.put(key, inputConfig);
        }
    );

    fieldPathEval = getContext().createELEval("conf.inputConfigs");
    fieldPathVars = getContext().createELVars();

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    return issues;
  }

  @Override
  public void process(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
    if (conf.useEntireBatch) {
      processUseEntireBatch(batch, singleLaneBatchMaker);
    } else {
      processUseRecordByRecord(batch, singleLaneBatchMaker);
    }
  }

  private void processUseEntireBatch(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
    Session.Runner runner = this.session.runner();
    Iterator<Record> batchRecords = batch.getRecords();
    if (batchRecords.hasNext()) {
      Map<Pair<String, Integer>, Tensor> inputs = convertBatch(batch, conf.inputConfigs);
      try {
        for (Map.Entry<Pair<String, Integer>, Tensor> inputMapEntry : inputs.entrySet()) {
          runner.feed(inputMapEntry.getKey().getLeft(),
              inputMapEntry.getKey().getRight(),
              inputMapEntry.getValue()
          );
        }

        for (TensorConfig outputConfig : conf.outputConfigs) {
          runner.fetch(outputConfig.operation, outputConfig.index);
        }

        List<Tensor<?>> tensorOutput = runner.run();
        LinkedHashMap<String, Field> outputTensorFieldMap = createOutputFieldValue(tensorOutput);
        EventRecord eventRecord = TensorFlowEvents.TENSOR_FLOW_OUTPUT_CREATOR.create(getContext()).create();
        eventRecord.set(Field.createListMap(outputTensorFieldMap));
        getContext().toEvent(eventRecord);
      } finally {
        inputs.values().forEach(Tensor::close);
      }

      Iterator<Record> it = batch.getRecords();
      while (it.hasNext()) {
        singleLaneBatchMaker.addRecord(it.next());
      }
    }
  }

  public void processUseRecordByRecord(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      setInputConfigFields(record);
      Session.Runner runner = this.session.runner();

      Map<Pair<String, Integer>, Tensor> inputs = null;
      try {
        inputs = convertRecord(record, conf.inputConfigs);
      } catch (OnRecordErrorException ex) {
        errorRecordHandler.onError(ex);
        continue;
      }

      try {
        for (Map.Entry<Pair<String, Integer>, Tensor> inputMapEntry : inputs.entrySet()) {
          runner.feed(inputMapEntry.getKey().getLeft(),
              inputMapEntry.getKey().getRight(),
              inputMapEntry.getValue()
          );
        }

        for (TensorConfig outputConfig : conf.outputConfigs) {
          runner.fetch(outputConfig.operation, outputConfig.index);
        }

        List<Tensor<?>> tensorOutput = runner.run();
        LinkedHashMap<String, Field> outputTensorFieldMap = createOutputFieldValue(tensorOutput);
        record.set(conf.outputField, Field.create(outputTensorFieldMap));
        singleLaneBatchMaker.addRecord(record);

      } finally {
        inputs.values().forEach(Tensor::close);
      }
    }
  }

  private void setInputConfigFields(Record record) {
    Set<String> fieldPaths = record.getEscapedFieldPaths();
    for (TensorInputConfig inputConfig : conf.inputConfigs) {
      List<String> inputConfigFields = new ArrayList<>();
      for(String f: inputConfig.fields) {
        List<String> matchingFieldPaths = FieldPathExpressionUtil.evaluateMatchingFieldPaths(
            f,
            fieldPathEval,
            fieldPathVars,
            record,
            fieldPaths
        );
        inputConfigFields.addAll(matchingFieldPaths);
      }
      inputConfig.setResolvedFields(inputConfigFields);
    }
  }

  private <T extends TensorDataTypeSupport> void writeRecord(
      Record r,
      List<String> fields,
      Buffer b,
      T dtSupport
  ) throws OnRecordErrorException {
    for (String fieldName : fields) {
      if (r.has(fieldName)) {
        dtSupport.writeField(b, r.get(fieldName));
      } else {
        // the field does not exist.
        throw new OnRecordErrorException(r,
            Errors.TENSOR_FLOW_03,
            r.getHeader().getSourceId(),
            fieldName
        );
      }
    }
  }

  private Map<Pair<String, Integer>, Tensor> convertBatch(
      Batch batch,
      List<TensorInputConfig> inputConfigs
  ) throws StageException {
    Map<Pair<String, Integer>, Buffer> inputBuffer = new LinkedHashMap<>();
    int numberOfRecords = Iterators.size(batch.getRecords());
    Iterator<Record> batchIterator = batch.getRecords();

    while (batchIterator.hasNext()) {
      Record r = batchIterator.next();
      setInputConfigFields(r);
      for (TensorInputConfig inputConfig : inputConfigs) {
        Pair<String, Integer> key = Pair.of(inputConfig.operation, inputConfig.index);
        long[] inputSize = (conf.useEntireBatch)?
            new long[]{1, numberOfRecords, inputConfig.getFields().size()}
            : new long[]{numberOfRecords, inputConfig.getFields().size()};

        TensorDataTypeSupport dtSupport = TensorTypeSupporter.INSTANCE.
            getTensorDataTypeSupport(inputConfig.tensorDataType);
        Buffer b = inputBuffer.computeIfAbsent(
            key,
            k -> dtSupport.allocateBuffer(inputSize)
        );
        try {
          writeRecord(r, inputConfig.getFields(), b, dtSupport);
        } catch (OnRecordErrorException e) {
          errorRecordHandler.onError(e);
        }
      }
    }

    return createInputTensor(inputBuffer, numberOfRecords);
  }

  private Map<Pair<String, Integer>, Tensor> createInputTensor(
      Map<Pair<String, Integer>, Buffer> inputBuffer,
      int numberOfRecords
  )  {
    Map<Pair<String, Integer>, Tensor> inputs = new LinkedHashMap<>();
    inputBuffer.forEach(
        (k, v) -> {
          TensorInputConfig inputConfig = inputConfigMap.get(k);
          TensorDataTypeSupport dtSupport =
              TensorTypeSupporter.INSTANCE.getTensorDataTypeSupport(inputConfig.tensorDataType);
          long[] inputSize = (conf.useEntireBatch)?
              new long[]{1, numberOfRecords, inputConfig.getFields().size()}
              : new long[]{numberOfRecords, inputConfig.getFields().size()};
          //for read
          v.flip();
          inputs.put(k, dtSupport.createTensor(inputSize, v));
        }
    );
    return inputs;
  }

  private Map<Pair<String, Integer>, Tensor> convertRecord(
      Record r,
      List<TensorInputConfig> inputConfigs
  ) throws OnRecordErrorException {
    Map<Pair<String, Integer>, Buffer> inputBuffer = new LinkedHashMap<>();
    int numberOfRecords = 1;
    for (TensorInputConfig inputConfig : inputConfigs) {
      Pair<String, Integer> key = Pair.of(inputConfig.operation, inputConfig.index);
      long[] inputSize = (conf.useEntireBatch)?
          new long[]{1, numberOfRecords, inputConfig.getFields().size()}
          : new long[]{numberOfRecords, inputConfig.getFields().size()};

      TensorDataTypeSupport dtSupport = TensorTypeSupporter.INSTANCE.getTensorDataTypeSupport(inputConfig.tensorDataType);
      Buffer b = inputBuffer.computeIfAbsent(
          key,
          k -> dtSupport.allocateBuffer(inputSize)
      );

      writeRecord(r, inputConfig.getFields(), b, dtSupport);
    }
    return createInputTensor(inputBuffer, numberOfRecords);
  }


  private LinkedHashMap<String, Field> createOutputFieldValue(List<Tensor<?>> tensorOutput) {
    LinkedHashMap<String, Field> outputTensorFieldMap = new LinkedHashMap<>();
    final AtomicInteger tensorIncrementor = new AtomicInteger(0);
    conf.outputConfigs.forEach(outputConfig -> {
      try (Tensor t = tensorOutput.get(tensorIncrementor.getAndIncrement())) {
        TensorDataTypeSupport dtSupport = TensorTypeSupporter.INSTANCE.getTensorDataTypeSupport(t.dataType());
        Field field = dtSupport.createFieldFromTensor(t);
        outputTensorFieldMap.put(outputConfig.operation + "_" + outputConfig.index, field);
      }
    });
    return outputTensorFieldMap;
  }

  @Override
  public void destroy() {
    if (this.savedModel != null) {
      this.savedModel.close();
    }
  }
}
