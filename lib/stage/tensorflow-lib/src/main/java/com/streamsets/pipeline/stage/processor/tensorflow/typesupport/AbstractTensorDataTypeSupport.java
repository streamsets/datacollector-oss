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
import com.streamsets.pipeline.api.impl.Utils;
import org.tensorflow.Tensor;

import java.nio.Buffer;
import java.util.List;

abstract class AbstractTensorDataTypeSupport<BC extends Buffer, JC> implements TensorDataTypeSupport<BC, JC> {

  int calculateCapacityForShape(long[] shape) {
    int capacity = 1;
    for (long aShape : shape) {
      capacity *= aShape;
    }
    return capacity;
  }

  @Override
  public Field createFieldFromTensor(Tensor<JC> tensor) {
    Utils.checkState(tensor.dataType() == getDataType(), "Not a Integer scalar");
    int size = calculateCapacityForShape(tensor.shape());
    if (size > 0) {
      BC buffer = allocateBuffer(tensor.shape());
      return Field.create(Field.Type.LIST, createListField(tensor, buffer));
    }
    return createPrimitiveField(tensor);
  }

  abstract List<Field> createListField(Tensor<JC> tensor, BC buffer);

  abstract Field createPrimitiveField(Tensor<JC> tensor);

}
