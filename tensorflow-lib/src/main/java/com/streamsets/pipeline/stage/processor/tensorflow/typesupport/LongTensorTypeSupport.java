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

import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;

final class LongTensorTypeSupport extends AbstractTensorDataTypeSupport<LongBuffer, Long> {
  @Override
  public LongBuffer allocateBuffer(long[] shape) {
    return LongBuffer.allocate(calculateCapacityForShape(shape));
  }

  @Override
  public Tensor<Long> createTensor(long[] shape, LongBuffer buffer) {
    return Tensor.create(shape, buffer);
  }

  @Override
  public void writeField(LongBuffer buffer, Field field) {
    Utils.checkState(
        field.getType() == Field.Type.LONG,
        Utils.format("Not a Long scalar, it is {}", field.getType())
    );
    buffer.put(field.getValueAsLong());
  }

  @Override
  public List<Field> createListField(Tensor<Long> tensor, LongBuffer longBuffer) {
    List<Field> fields = new ArrayList<>();
    tensor.writeTo(longBuffer);
    long[] longs = longBuffer.array();
    for (long aLong : longs) {
      fields.add(Field.create(aLong));
    }
    return fields;
  }

  @Override
  public Field createPrimitiveField(Tensor<Long> tensor) {
    return Field.create(tensor.longValue());
  }

  @Override
  public DataType getDataType() {
    return DataType.INT64;
  }
}
