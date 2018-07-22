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
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.stage.processor.tensorflow.typesupport.TensorDataTypeSupport;
import com.streamsets.pipeline.stage.processor.tensorflow.typesupport.TensorTypeSupporter;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;
import org.tensorflow.TensorFlowException;

import java.nio.Buffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public final class TensorFlowProcessor extends SingleLaneProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(TensorFlowProcessor.class);

  private final TensorFlowConfigBean conf;

  private SavedModelBundle savedModel;
  private Session session;
  private Map<Pair<String, Integer>, TensorInputConfig> inputConfigMap = new LinkedHashMap<>();
  private Map<Pair<String, Integer>, TensorConfig> outputConfigMap = new LinkedHashMap<>();

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
      this.savedModel = SavedModelBundle.load(conf.modelPath, modelTags);
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

    this.conf.outputConfigs.forEach(outputConfig -> {
          Pair<String, Integer> key = Pair.of(outputConfig.operation, outputConfig.index);
          outputConfigMap.put(key, outputConfig);
        }
    );

    return issues;
  }

  private <T extends TensorDataTypeSupport> void writeRecord(Record r, List<String> fields, Buffer b, T dtSupport) {
    for (String fieldName : fields) {
      dtSupport.writeField(b, r.get(fieldName));
    }
  }

  private Map<Pair<String, Integer>, Tensor> convertBatch(Batch batch, List<TensorInputConfig> inputConfigs) {
    Map<Pair<String, Integer>, Tensor> inputs = new LinkedHashMap<>();
    Map<Pair<String, Integer>, Buffer> inputBuffer = new LinkedHashMap<>();

    int numberOfRecords = Iterators.size(batch.getRecords());

    batch.getRecords().forEachRemaining(r -> {
      for (TensorInputConfig inputConfig : inputConfigs) {
        Pair<String, Integer> key = Pair.of(inputConfig.operation, inputConfig.index);
        long[] inputSize = (conf.useEntireBatch)?
            new long[]{1, numberOfRecords, inputConfig.fields.size()}
            : new long[]{numberOfRecords, inputConfig.fields.size()};

        TensorDataTypeSupport dtSupport = TensorTypeSupporter.INSTANCE.getTensorDataTypeSupport(inputConfig.tensorDataType);
        Buffer b = inputBuffer.computeIfAbsent(
            key,
            k -> dtSupport.allocateBuffer(inputSize)
        );
        writeRecord(r, inputConfig.fields, b, dtSupport);
      }
    });

    inputBuffer.forEach(
        (k, v) -> {
          TensorInputConfig inputConfig = inputConfigMap.get(k);
          TensorDataTypeSupport dtSupport =
              TensorTypeSupporter.INSTANCE.getTensorDataTypeSupport(inputConfig.tensorDataType);
          long[] inputSize = (conf.useEntireBatch)?
              new long[]{1, numberOfRecords, inputConfig.fields.size()}
              : new long[]{numberOfRecords, inputConfig.fields.size()};
          //for read
          v.flip();
          inputs.put(k, dtSupport.createTensor(inputSize, v));
        }
    );
    return inputs;
  }

  private Map<Pair<String, Integer>, Tensor> convertRecord(Record r, List<TensorInputConfig> inputConfigs) {
    Map<Pair<String, Integer>, Tensor> inputs = new LinkedHashMap<>();
    Map<Pair<String, Integer>, Buffer> inputBuffer = new LinkedHashMap<>();

    int numberOfRecords = 1;

    for (TensorInputConfig inputConfig : inputConfigs) {
      Pair<String, Integer> key = Pair.of(inputConfig.operation, inputConfig.index);
      long[] inputSize = (conf.useEntireBatch)?
          new long[]{1, numberOfRecords, inputConfig.fields.size()}
          : new long[]{numberOfRecords, inputConfig.fields.size()};

      TensorDataTypeSupport dtSupport = TensorTypeSupporter.INSTANCE.getTensorDataTypeSupport(inputConfig.tensorDataType);
      Buffer b = inputBuffer.computeIfAbsent(
          key,
          k -> dtSupport.allocateBuffer(inputSize)
      );
      writeRecord(r, inputConfig.fields, b, dtSupport);
    }

    inputBuffer.forEach(
        (k, v) -> {
          TensorInputConfig inputConfig = inputConfigMap.get(k);
          TensorDataTypeSupport dtSupport =
              TensorTypeSupporter.INSTANCE.getTensorDataTypeSupport(inputConfig.tensorDataType);
          long[] inputSize = (conf.useEntireBatch)?
              new long[]{1, numberOfRecords, inputConfig.fields.size()}
              : new long[]{numberOfRecords, inputConfig.fields.size()};
          //for read
          v.flip();
          inputs.put(k, dtSupport.createTensor(inputSize, v));
        }
    );
    return inputs;
  }

  @Override
  public void process(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) {
    if (conf.useEntireBatch) {
      processUseEntireBatch(batch, singleLaneBatchMaker);
    } else {
      processUseRecordByRecord(batch, singleLaneBatchMaker);
    }
  }

  public void processUseRecordByRecord(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) {
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      Session.Runner runner = this.session.runner();

      Map<Pair<String, Integer>, Tensor> inputs = convertRecord(record, conf.inputConfigs);
      try {
        for (Map.Entry<Pair<String, Integer>, Tensor> inputMapEntry : inputs.entrySet()) {
          runner = runner.feed(inputMapEntry.getKey().getLeft(),
              inputMapEntry.getKey().getRight(),
              inputMapEntry.getValue()
          );
        }

        for (TensorConfig outputConfig : conf.outputConfigs) {
          runner = runner.fetch(outputConfig.operation, outputConfig.index);
        }

        List<Tensor<?>> tensorOutput = runner.run();
        LinkedHashMap<String, Field> outputTensorFieldMap = new LinkedHashMap<>();
        final AtomicInteger tensorIncrementor = new AtomicInteger(0);

        conf.outputConfigs.forEach(outputConfig -> {
          try (Tensor t = tensorOutput.get(tensorIncrementor.getAndIncrement())) {
            TensorDataTypeSupport dtSupport = TensorTypeSupporter.INSTANCE.getTensorDataTypeSupport(t.dataType());
            Field field = dtSupport.createFieldFromTensor(t);
            outputTensorFieldMap.put(outputConfig.operation + "_" + outputConfig.index, field);
          }
        });

        record.set(conf.outputField, Field.create(outputTensorFieldMap));

        singleLaneBatchMaker.addRecord(record);

      } finally {
        inputs.values().forEach(Tensor::close);
      }
    }
  }


  public void processUseEntireBatch(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) {
    Session.Runner runner = this.session.runner();
    Iterator<Record> batchRecords = batch.getRecords();
    if (batchRecords.hasNext()) {
      Map<Pair<String, Integer>, Tensor> inputs = convertBatch(batch, conf.inputConfigs);
      try {
        for (Map.Entry<Pair<String, Integer>, Tensor> inputMapEntry : inputs.entrySet()) {
          runner = runner.feed(inputMapEntry.getKey().getLeft(),
              inputMapEntry.getKey().getRight(),
              inputMapEntry.getValue()
          );
        }

        for (TensorConfig outputConfig : conf.outputConfigs) {
          runner = runner.fetch(outputConfig.operation, outputConfig.index);
        }

        List<Tensor<?>> tensorOutput = runner.run();
        LinkedHashMap<String, Field> outputTensorFieldMap = new LinkedHashMap<>();
        final AtomicInteger tensorIncrementor = new AtomicInteger(0);

        if (conf.useEntireBatch) {
          conf.outputConfigs.forEach(outputConfig -> {
            try (Tensor t = tensorOutput.get(tensorIncrementor.getAndIncrement())) {
              TensorDataTypeSupport dtSupport = TensorTypeSupporter.INSTANCE.getTensorDataTypeSupport(t.dataType());
              Field field = dtSupport.createFieldFromTensor(t);
              outputTensorFieldMap.put(outputConfig.operation + "_" + outputConfig.index, field);
            }
          });
          EventRecord eventRecord = TensorFlowEvents.TENSOR_FLOW_OUTPUT_CREATOR.create(getContext()).create();
          eventRecord.set(Field.createListMap(outputTensorFieldMap));
          getContext().toEvent(eventRecord);
        } else {
          throw new UnsupportedOperationException();
        }
      } finally {
        inputs.values().forEach(Tensor::close);
      }

      Iterator<Record> it = batch.getRecords();
      while (it.hasNext()) {
        singleLaneBatchMaker.addRecord(it.next());
      }
    }
  }

  @Override
  public void destroy() {
    if (this.savedModel != null) {
      this.savedModel.close();
    }
  }
}
