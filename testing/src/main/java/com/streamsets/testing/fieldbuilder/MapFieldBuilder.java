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
package com.streamsets.testing.fieldbuilder;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

public class MapFieldBuilder extends BaseFieldBuilder<MapFieldBuilder> {
  private final ImmutableMap.Builder builder = ImmutableMap.builder();
  private boolean listMap;
  private final String field;
  private final BaseFieldBuilder<? extends BaseFieldBuilder> parentBuilder;

  public static MapFieldBuilder builder() {
    return new MapFieldBuilder(null, null);
  }

  MapFieldBuilder(String field, BaseFieldBuilder<? extends BaseFieldBuilder> parentBuilder) {
    this.field = field;
    this.parentBuilder = parentBuilder;
  }

  @Override
  public BaseFieldBuilder<? extends BaseFieldBuilder> end() {
    return end(new String[0]);
  }

  @Override
  public BaseFieldBuilder<? extends BaseFieldBuilder> end(String... attributes) {
    if (parentBuilder == null) {
      throw new IllegalStateException("Do not call end on the root builder; just call build when finished");
    }
    final Field field = Field.create(Field.Type.MAP, this.builder.build());
    for (Map.Entry<String, String> attr : buildAttributeMap(attributes).entrySet()) {
      field.setAttribute(attr.getKey(), attr.getValue());
    }
    parentBuilder.handleEndChildField(this.field, field);
    return parentBuilder;
  }

  @Override
  protected MapFieldBuilder getInstance() {
    return this;
  }

  public MapFieldBuilder add(String field, String value) {
    builder.put(field, Field.create(value));
    return this;
  }

  public MapFieldBuilder add(String field, Character value) {
    builder.put(field, Field.create(value));
    return this;
  }

  public MapFieldBuilder add(String field, Byte value) {
    builder.put(field, Field.create(value));
    return this;
  }

  public MapFieldBuilder add(String field, Short value) {
    builder.put(field, Field.create(value));
    return this;
  }

  public MapFieldBuilder add(String field, Integer value) {
    builder.put(field, Field.create(value));
    return this;
  }

  public MapFieldBuilder add(String field, Long value) {
    builder.put(field, Field.create(value));
    return this;
  }

  public MapFieldBuilder add(String field, Float value) {
    builder.put(field, Field.create(value));
    return this;
  }

  public MapFieldBuilder add(String field, Double value) {
    builder.put(field, Field.create(value));
    return this;
  }

  public MapFieldBuilder add(String field, Date value) {
    builder.put(field, Field.create(Field.Type.DATETIME, value));
    return this;
  }

  public MapFieldBuilder add(String field, BigDecimal value) {
    builder.put(field, Field.create(value));
    return this;
  }

  public MapFieldBuilder add(String field, byte[] value) {
    builder.put(field, Field.create(value));
    return this;
  }

  public MapFieldBuilder listMap(boolean listMap) {
    this.listMap = listMap;
    return this;
  }

  @Override
  protected void handleEndChildField(String fieldName, Field fieldValue) {
    builder.put(fieldName, fieldValue);
  }

  public Field build() {
    return Field.create(listMap ? Field.Type.LIST_MAP : Field.Type.MAP, builder.build());
  }

}
