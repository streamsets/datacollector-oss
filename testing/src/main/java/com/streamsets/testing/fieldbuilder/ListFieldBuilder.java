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

import com.streamsets.pipeline.api.Field;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ListFieldBuilder extends BaseFieldBuilder<ListFieldBuilder> {
  private final String field;
  private final BaseFieldBuilder<? extends BaseFieldBuilder> parentBuilder;
  private final List<Field> fields = new LinkedList<>();

  ListFieldBuilder(String field, BaseFieldBuilder<? extends BaseFieldBuilder> parentBuilder) {
    this.field = field;
    this.parentBuilder = parentBuilder;
  }

  public ListFieldBuilder add(String... value) {
    Arrays.asList(value).forEach(v -> fields.add(Field.create(v)));
    return this;
  }

  public ListFieldBuilder add(Integer... value) {
    Arrays.asList(value).forEach(v -> fields.add(Field.create(v)));
    return this;
  }

  public ListFieldBuilder add(Character... value) {
    Arrays.asList(value).forEach(v -> fields.add(Field.create(v)));
    return this;
  }

  public ListFieldBuilder add(Byte... value) {
    Arrays.asList(value).forEach(v -> fields.add(Field.create(v)));
    return this;
  }

  public ListFieldBuilder add(Short... value) {
    Arrays.asList(value).forEach(v -> fields.add(Field.create(v)));
    return this;
  }

  public ListFieldBuilder add(Long... value) {
    Arrays.asList(value).forEach(v -> fields.add(Field.create(v)));
    return this;
  }

  public ListFieldBuilder add(Float... value) {
    Arrays.asList(value).forEach(v -> fields.add(Field.create(v)));
    return this;
  }

  public ListFieldBuilder add(Double... value) {
    Arrays.asList(value).forEach(v -> fields.add(Field.create(v)));
    return this;
  }

  public ListFieldBuilder add(Date... value) {
    Arrays.asList(value).forEach(v -> fields.add(Field.create(Field.Type.DATETIME, v)));
    return this;
  }

  public ListFieldBuilder add(BigDecimal... value) {
    Arrays.asList(value).forEach(v -> fields.add(Field.create(v)));
    return this;
  }

  public ListFieldBuilder add(byte[]... value) {
    Arrays.asList(value).forEach(v -> fields.add(Field.create(v)));
    return this;
  }

  @Override
  public ListFieldBuilder startList(String name) {
    return super.startList(name);
  }

  @Override
  protected ListFieldBuilder getInstance() {
    return this;
  }

  @Override
  protected void handleEndChildField(String fieldName, Field fieldValue) {
    fields.add(fieldValue);
  }

  @Override
  public BaseFieldBuilder<? extends BaseFieldBuilder> end() {
    return end(new String[0]);
  }

  @Override
  public BaseFieldBuilder<? extends BaseFieldBuilder> end(String... attributes) {
    final Field field = Field.create(Field.Type.LIST, fields);
    for (Map.Entry<String, String> attr : buildAttributeMap(attributes).entrySet()) {
      field.setAttribute(attr.getKey(), attr.getValue());
    }
    parentBuilder.handleEndChildField(this.field, field);
    return parentBuilder;
  }
}
