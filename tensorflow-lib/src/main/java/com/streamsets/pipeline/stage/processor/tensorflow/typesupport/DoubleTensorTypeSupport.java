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

import java.nio.DoubleBuffer;
import java.util.ArrayList;
import java.util.List;

final class DoubleTensorTypeSupport extends AbstractTensorDataTypeSupport<DoubleBuffer, Double> {

  @Override
  public DoubleBuffer allocateBuffer(long[] shape) {
    return DoubleBuffer.allocate(calculateCapacityForShape(shape));
  }

  @Override
  public Tensor<Double> createTensor(long[] shape, DoubleBuffer buffer) {
    return Tensor.create(shape, buffer);
  }

  @Override
  public void writeField(DoubleBuffer buffer, Field field) {
    Utils.checkState(
        field.getType() == Field.Type.DOUBLE,
        Utils.format("Not a Double scalar, it is {}", field.getType())
    );
    buffer.put(field.getValueAsDouble());
  }

  @Override
  public List<Field> createListField(Tensor<Double> tensor, DoubleBuffer doubleBuffer) {
    List<Field> fields = new ArrayList<>();
    tensor.writeTo(doubleBuffer);
    double[] doubles = doubleBuffer.array();
    for (double aDouble : doubles) {
      fields.add(Field.create(aDouble));
    }
    return fields;
  }

  @Override
  public Field createPrimitiveField(Tensor<Double> tensor) {
    return Field.create(tensor.doubleValue());
  }

  @Override
  public DataType getDataType() {
    return DataType.DOUBLE;
  }
}
