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

import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.List;

final class FloatTensorTypeSupport extends AbstractTensorDataTypeSupport<FloatBuffer, Float> {
  @Override
  public FloatBuffer allocateBuffer(long[] shape) {
    return FloatBuffer.allocate(calculateCapacityForShape(shape));
  }

  @Override
  public Tensor<Float> createTensor(long[] shape, FloatBuffer buffer) {
    return Tensor.create(shape, buffer);
  }

  @Override
  public void writeField(FloatBuffer buffer, Field field) {
    Utils.checkState(
        field.getType() == Field.Type.FLOAT,
        Utils.format("Not a Float scalar, it is {}", field.getType())
    );
    buffer.put(field.getValueAsFloat());
  }

  @Override
  public List<Field> createListField(Tensor<Float> tensor, FloatBuffer floatBuffer) {
    List<Field> fields = new ArrayList<>();
    tensor.writeTo(floatBuffer);
    float[] floats = floatBuffer.array();
    for (float aFloat : floats) {
      fields.add(Field.create(aFloat));
    }
    return fields;
  }

  @Override
  public Field createPrimitiveField(Tensor<Float> tensor) {
    return Field.create(tensor.floatValue());
  }

  @Override
  public DataType getDataType() {
    return DataType.FLOAT;
  }
}
