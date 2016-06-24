/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.fieldrenamer;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.lib.util.FieldRegexUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Processor for renaming fields. The processor supported specifying regex
 *
 * This processor works similarly to how a Linux mv command would. Semantics are as follows:
 *   1. The source fields are matched from the from fields expression. If multiple rename configurations
 *      fromFieldExpression matches the same from field, the success of the processor depends
 *      on MultipleFromFieldsMatchHandling Configuration.
 *   2. If source field does not exist, success of processor depends on the
 *      non existing from Field Error Handling configuration.
 *   3. If source field exists and target field does not exist, source field will be renamed to
 *      the target field's name. Source field will be removed at the end of the operation.
 *   4. If source field exists and target field exists, the ExistingToFieldHandling configuration
 *      will determine how and whether or not the operation can succeed.
 */
public class FieldRenamerProcessor extends SingleLaneRecordProcessor {
  private final List<FieldRenamerConfig> renameMapping;
  private final FieldRenamerProcessorErrorHandler errorHandler;
  private final Map<Pattern, String> fromPatternToFieldExpMapping;

  public FieldRenamerProcessor(
      List<FieldRenamerConfig> renameMapping,
      FieldRenamerProcessorErrorHandler errorHandler
  ) {
    this.renameMapping = Utils.checkNotNull(renameMapping, "Rename mapping cannot be null");
    this.errorHandler = errorHandler;
    this.fromPatternToFieldExpMapping = new LinkedHashMap<>();
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    for (FieldRenamerConfig renameConfig : renameMapping) {
      String fromFieldPathRegex = FieldRegexUtil.patchUpFieldPathRegex(renameConfig.fromFieldExpression);
      try {
        Pattern pattern = Pattern.compile(fromFieldPathRegex);
        fromPatternToFieldExpMapping.put(pattern, renameConfig.toFieldExpression);
      } catch (PatternSyntaxException e) {
        issues.add(
            getContext().createConfigIssue(
                Groups.RENAME.name(),
                "renameMapping",
                Errors.FIELD_RENAMER_02,
                renameConfig.fromFieldExpression
            )
        );
      }
    }
    return issues;
  }

  private void populateFromAndToFieldsMap(
      Pattern fromFieldPattern,
      String toFieldExpression,
      Set<String> fieldPaths,
      Set<String> fieldsThatDoNotExist,
      Map<String, List<String>> multipleRegexMatchingSameFields,
      Map<String, String> fromFieldToFieldMap
  ) {
    boolean hasSourceFieldsMatching = false;
    for(String existingFieldPath : fieldPaths) {
      Matcher matcher = fromFieldPattern.matcher(existingFieldPath);
      if (matcher.matches()) {
        hasSourceFieldsMatching = true;
        String toFieldPath = matcher.replaceAll(toFieldExpression);
        if (fromFieldToFieldMap.containsKey(existingFieldPath)) {
          List<String> patterns =  multipleRegexMatchingSameFields.get(existingFieldPath);
          if (patterns == null) {
            patterns = new ArrayList<>();
          }
          patterns.add(fromFieldPattern.pattern());
          multipleRegexMatchingSameFields.put(existingFieldPath, patterns);
          //We should not process the field.
          fromFieldToFieldMap.remove(existingFieldPath);
        } else {
          fromFieldToFieldMap.put(existingFieldPath, toFieldPath);
        }
      }
    }
    if (!hasSourceFieldsMatching) {
      fieldsThatDoNotExist.add(fromFieldPattern.pattern());
    }
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Set<String> fieldsThatDoNotExist = new HashSet<>();
    Set<String> fieldsRequiringOverwrite = new HashSet<>();
    Map<String, List<String>> multipleRegexMatchingSameFields = new HashMap<>();
    //So that the ordering of fieldPaths will be preserved
    Map<String, String> fromFieldToFieldMap = new LinkedHashMap<>();

    for (Map.Entry<Pattern, String> fromPatternToFieldExpEntry : fromPatternToFieldExpMapping.entrySet()) {
      populateFromAndToFieldsMap(
          fromPatternToFieldExpEntry.getKey(),
          fromPatternToFieldExpEntry.getValue(),
          record.getEscapedFieldPaths(),
          fieldsThatDoNotExist,
          multipleRegexMatchingSameFields,
          fromFieldToFieldMap
      );
    }

    for (Map.Entry<String, String> fromFieldToFieldEntry : fromFieldToFieldMap.entrySet()) {
      String fromFieldName = fromFieldToFieldEntry.getKey();
      String toFieldName = fromFieldToFieldEntry.getValue();
      // The fromFieldName will always exist, not need to check for its existence.
      // If the source field exists and the target does not, we need to replace
      // We can also replace in this case if overwrite existing is set to true
      Field fromField = record.get(fromFieldName);
      if (record.has(toFieldName)) {
        switch (errorHandler.existingToFieldHandling) {
          case TO_ERROR:
            fieldsRequiringOverwrite.add(toFieldName);
            break;
          case CONTINUE:
            break;
          case APPEND_NUMBERS:
            int i = 1;
            for (;record.has(toFieldName+String.valueOf(i));i++);
            toFieldName = toFieldName+String.valueOf(i);
            //Fall through so as to edit.
          case REPLACE:
            record.set(toFieldName, fromField);
            record.delete(fromFieldName);
            break;
        }
      } else {
        record.set(toFieldName, fromField);
        record.delete(fromFieldName);
      }
    }

    if (errorHandler.nonExistingFromFieldHandling == OnStagePreConditionFailure.TO_ERROR
        && !fieldsThatDoNotExist.isEmpty()) {
      throw new OnRecordErrorException(Errors.FIELD_RENAMER_00, record.getHeader().getSourceId(),
          Joiner.on(", ").join(fieldsThatDoNotExist));
    }

    if (errorHandler.multipleFromFieldsMatching == OnStagePreConditionFailure.TO_ERROR
        && !multipleRegexMatchingSameFields.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, List<String>> multipleRegexMatchingSameFieldEntry : multipleRegexMatchingSameFields.entrySet()) {
        sb.append(
            "Field: " + multipleRegexMatchingSameFieldEntry.getKey() + " Regex: " +
                Joiner.on(".").join(multipleRegexMatchingSameFieldEntry.getValue())
        );
      }
      throw new OnRecordErrorException(record, Errors.FIELD_RENAMER_03, sb.toString());

    }

    if (!fieldsRequiringOverwrite.isEmpty()) {
      throw new OnRecordErrorException(record, Errors.FIELD_RENAMER_01,
          Joiner.on(", ").join(fieldsRequiringOverwrite),
          record.getHeader().getSourceId());
    }

    batchMaker.addRecord(record);
  }
}
