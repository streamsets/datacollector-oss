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
package com.streamsets.pipeline.stage.processor.tensorflow.typesupport;

import com.streamsets.pipeline.api.Field;
import org.tensorflow.DataType;
import org.tensorflow.Tensor;

import java.nio.Buffer;

public interface TensorDataTypeSupport<BC extends Buffer, JC> {
  BC allocateBuffer(long[] shape);

  Tensor<JC> createTensor(long[] shape, BC buffer);

  void writeField(BC buffer, Field field);

  Field createFieldFromTensor(Tensor<JC> tensor);

  DataType getDataType();
}
