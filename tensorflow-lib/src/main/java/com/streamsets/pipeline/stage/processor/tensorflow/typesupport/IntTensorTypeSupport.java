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
import org.tensorflow.DataType;
import org.tensorflow.Tensor;

import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

class IntTensorTypeSupport extends AbstractTensorDataTypeSupport<IntBuffer, Integer>{
  @Override
  public IntBuffer allocateBuffer(long[] shape) {
    return IntBuffer.allocate(calculateCapacityForShape(shape));
  }

  @Override
  public Tensor<Integer> createTensor(long[] shape, IntBuffer buffer) {
    return Tensor.create(shape, buffer);
  }

  @Override
  public void writeField(IntBuffer buffer, Field field) {
    Utils.checkState(
        field.getType() == Field.Type.INTEGER,
        Utils.format("Not a Integer scalar, it is {}", field.getType())
    );
    buffer.put(field.getValueAsInteger());
  }

  @Override
  public List<Field> createListField(Tensor<Integer> tensor, IntBuffer intBuffer) {
    List<Field> fields = new ArrayList<>();
    tensor.writeTo(intBuffer);
    int[] ints = intBuffer.array();
    for (int aInt : ints) {
      fields.add(Field.create(aInt));
    }
    return fields;
  }

  @Override
  public Field createPrimitiveField(Tensor<Integer> tensor) {
    return Field.create(tensor.intValue());
  }

  @Override
  public DataType getDataType() {
    return DataType.INT32;
  }
}
