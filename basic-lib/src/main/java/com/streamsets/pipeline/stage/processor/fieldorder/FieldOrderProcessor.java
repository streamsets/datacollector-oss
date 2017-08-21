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
package com.streamsets.pipeline.stage.processor.fieldorder;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.streamsets.datacollector.util.EscapeUtil;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.stage.processor.fieldorder.config.ExtraFieldAction;
import com.streamsets.pipeline.stage.processor.fieldorder.config.MissingFieldAction;
import com.streamsets.pipeline.stage.processor.fieldorder.config.OrderConfigBean;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

public class FieldOrderProcessor extends SingleLaneRecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(FieldOrderProcessor.class);

  private OrderConfigBean config;
  private Set<String> fields;
  private Set<String> discardFields;

  private Field defaultField;

  public FieldOrderProcessor(OrderConfigBean config) {
    this.config = config;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    fields = ImmutableSet.copyOf(config.fields);
    discardFields = new ImmutableSet.Builder<String>()
      .add("") // Root item is implicitly ignored
      .addAll(config.discardFields)
      .build();

    if(config.missingFieldAction == MissingFieldAction.USE_DEFAULT) {
      defaultField = Field.create(config.dataType.getType(), config.defaultValue);
    }

    // Validate that discard fields and order fields do not contain duplicate field paths
    Set<String> duplicates = Sets.intersection(ImmutableSet.copyOf(config.fields), ImmutableSet.copyOf(config.discardFields));
    if(!duplicates.isEmpty()) {
      issues.add(getContext().createConfigIssue("ORDER", "config.fields", Errors.FIELD_ORDER_003, StringUtils.join(duplicates, ",")));
    }

    return issues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Set<String> paths = Sets.difference(record.getEscapedFieldPaths(), discardFields);

    Set<String> missingFields = Sets.difference(fields, paths);
    if(!missingFields.isEmpty() && config.missingFieldAction == MissingFieldAction.TO_ERROR) {
      throw new OnRecordErrorException(record, Errors.FIELD_ORDER_001, StringUtils.join(missingFields, ", "));
    }

    Set<String> extraFields = Sets.difference(paths, fields);
    if(!extraFields.isEmpty() && config.extraFieldAction == ExtraFieldAction.TO_ERROR) {
      throw new OnRecordErrorException(record, Errors.FIELD_ORDER_002, StringUtils.join(extraFields, ", "));
    }

    switch (config.outputType) {
      case LIST:
        orderToList(record);
        break;
      case LIST_MAP:
        orderToListMap(record);
        break;
      default:
        throw new IllegalArgumentException("Unknown output type: " + config.outputType);
    }
    batchMaker.addRecord(record);
  }

  private void orderToListMap(Record record) {
    LinkedHashMap<String, Field> list = new LinkedHashMap<>(fields.size());
    for(String fieldPath : config.fields) {
      list.put(
        toFieldName(fieldPath),
        record.has(fieldPath) ? record.get(fieldPath) : defaultField
      );
    }

    record.set(Field.create(Field.Type.LIST_MAP, list));
  }

  private void orderToList(Record record) {
    List<Field> list = new ArrayList<>(fields.size());
    for(String fieldPath : config.fields) {
      list.add(
        record.has(fieldPath) ? record.get(fieldPath) : defaultField
      );
    }

    record.set(Field.create(Field.Type.LIST, list));
  }

  // Remove initial "/", replace all other slashes with dots
  private String toFieldName(String fieldPath) {
    String path = fieldPath.substring(1).replaceAll("/", ".");
    path = "/" + path;
    return EscapeUtil.getLastFieldNameFromPath(path);
  }

}
