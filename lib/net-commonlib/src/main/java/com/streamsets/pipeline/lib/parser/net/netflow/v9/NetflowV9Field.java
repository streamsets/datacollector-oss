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

package com.streamsets.pipeline.lib.parser.net.netflow.v9;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.impl.Utils;

public class NetflowV9Field {

  public static final int SCOPE_FIELD_OFFSET = 1000;

  private final NetflowV9FieldTemplate fieldTemplate;
  private final byte[] rawValue;
  private final Field interpretedValueField;

  public NetflowV9Field(NetflowV9FieldTemplate fieldTemplate, byte[] rawValue, Field interpretedValueField) {
    Utils.checkNotNull(fieldTemplate, "fieldTemplate");
    this.fieldTemplate = fieldTemplate;
    this.rawValue = rawValue;
    this.interpretedValueField = interpretedValueField;
  }

  public NetflowV9FieldTemplate getFieldTemplate() {
    return fieldTemplate;
  }

  public byte[] getRawValue() {
    return rawValue;
  }

  public Field getInterpretedValueField() {
    return interpretedValueField;
  }

  public String getSdcFieldName() {
    if (fieldTemplate.getType() != null) {
      return fieldTemplate.getType().name();
    } else {
      return String.format("type_%d", fieldTemplate.getTypeId());
    }
  }
}
