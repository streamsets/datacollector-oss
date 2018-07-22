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

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.streamsets.pipeline.api.impl.Utils;
import org.tensorflow.DataType;

public final class TensorTypeSupporter {
  public static TensorTypeSupporter INSTANCE = new TensorTypeSupporter();

  private static final BiMap<DataType, TensorDataTypeSupport> DATA_TYPE_FIELD_TYPE_MAP =
      new ImmutableBiMap.Builder<DataType, TensorDataTypeSupport>()
          .put(DataType.INT32, new IntTensorTypeSupport())
          .put(DataType.INT64, new LongTensorTypeSupport())
          .put(DataType.FLOAT, new FloatTensorTypeSupport())
          .put(DataType.DOUBLE, new DoubleTensorTypeSupport())
          .build();

  private TensorTypeSupporter() {
  }

  public TensorDataTypeSupport getTensorDataTypeSupport(DataType dataType) {
    return Utils.checkNotNull(DATA_TYPE_FIELD_TYPE_MAP.get(dataType), "Unsupported Data Type :" + dataType);
  }
}
