/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.schemagen.config;

import com.streamsets.pipeline.api.Label;
import org.apache.avro.Schema;

public enum AvroType implements Label {
  BOOLEAN("BOOLEAN", Schema.Type.BOOLEAN),
  INTEGER("INTEGER", Schema.Type.INT),
  LONG("LONG", Schema.Type.LONG),
  FLOAT("FLOAT", Schema.Type.FLOAT),
  DOUBLE("DOUBLE", Schema.Type.DOUBLE),
  STRING("STRING", Schema.Type.STRING),
  ;


  private final String label;
  private final Schema.Type type;

  AvroType(String label, Schema.Type type) {
    this.label = label;
    this.type = type;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public Schema.Type getType() {
    return type;
  }

}
