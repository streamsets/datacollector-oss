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

import org.tensorflow.DataType;

import java.util.ArrayList;
import java.util.List;

class TestTensorFlowProcessorBuilder {
  private TensorFlowConfigBean conf = new TensorFlowConfigBean();

  TestTensorFlowProcessorBuilder() {
    conf.modelTags = new ArrayList<>();
    conf.inputConfigs = new ArrayList<>();
    conf.outputConfigs = new ArrayList<>();
    conf.useEntireBatch = false;
  }

  TestTensorFlowProcessorBuilder modelPath(String modelPath){
    conf.modelPath = modelPath;
    return this;
  }

  TestTensorFlowProcessorBuilder modelTags(List<String> modelTags){
    conf.modelTags = modelTags;
    return this;
  }

  TestTensorFlowProcessorBuilder useEntireBatch(boolean useEntireBatch){
    conf.useEntireBatch = useEntireBatch;
    return this;
  }

  TestTensorFlowProcessorBuilder outputField(String outputField){
    conf.outputField = outputField;
    return this;
  }

  TestTensorFlowProcessorBuilder addInputConfigs(
      String operation,
      int index,
      List<String> fields,
      List<Integer> shape,
      DataType tensorDataType
  ){
    TensorInputConfig inputConfig = new TensorInputConfig();
    inputConfig.operation = operation;
    inputConfig.index = index;
    inputConfig.fields = fields;
    inputConfig.shape = shape;
    inputConfig.tensorDataType = tensorDataType;
    conf.inputConfigs.add(inputConfig);
    return this;
  }

  TestTensorFlowProcessorBuilder addOutputConfigs(
      String operation,
      int index,
      DataType tensorDataType
  ){
    TensorConfig outputConfig = new TensorConfig();
    outputConfig.operation = operation;
    outputConfig.index = index;
    outputConfig.tensorDataType = tensorDataType;
    conf.outputConfigs.add(outputConfig);
    return this;
  }

  TensorFlowProcessor build() {
    return  new TensorFlowProcessor(conf);
  }
}
