/**
 * Licensed to the Apache Software Foundation (ASF) under one
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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Processor for renaming fields.
 *
 * This processor works similarly to how a Linux mv command would. Semantics are as follows:
 *   1. If source field does not exist, success of processor depends on the
 *      precondition-failure configuration.
 *   2. If source field exists and target field does not exist, source field will be renamed to
 *      the target field's name. Source field will be removed at the end of the operation.
 *   3. If source field exists and target field exists, the overwrite configuration will determine
 *      whether or not the operation can succeed.
 */
public class FieldRenamerProcessor extends SingleLaneRecordProcessor {
  private final List<FieldRenamerConfig> renameMapping;
  private final OnStagePreConditionFailure onStagePreConditionFailure;
  private final boolean overwriteExisting;

  public FieldRenamerProcessor (List<FieldRenamerConfig> renameMapping,
      OnStagePreConditionFailure onStagePreConditionFailure,
      boolean overwriteExisting
  ) {
    this.renameMapping = Utils.checkNotNull(renameMapping, "Rename mapping cannot be nulll");
    this.onStagePreConditionFailure = onStagePreConditionFailure;
    this.overwriteExisting = overwriteExisting;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Set<String> fieldsThatDoNotExist = new HashSet<>();
    Set<String> fieldsRequiringOverwrite = new HashSet<>();
    for (FieldRenamerConfig renameConfig : renameMapping) {
      String fromFieldName = renameConfig.fromField;
      String toFieldName = renameConfig.toField;

      if (!record.has(fromFieldName)) {
        // Source field does not exist, so generate an error for non-existent source field
        fieldsThatDoNotExist.add(fromFieldName);
      } else if ((record.has(fromFieldName) && !record.has(toFieldName)) || overwriteExisting) {
        // If the source field exists and the target does not, we need to replace
        // We can also replace in this case if overwrite existing is set to true
        Field fromField = record.get(renameConfig.fromField);
        record.set(renameConfig.toField, fromField);
        record.delete(renameConfig.fromField);
      } else {
        // Target field exist, but overwrite existing is false, so generate an error
        fieldsRequiringOverwrite.add(renameConfig.toField);
      }
    }

    if (onStagePreConditionFailure == OnStagePreConditionFailure.TO_ERROR && !fieldsThatDoNotExist.isEmpty()) {
     throw new OnRecordErrorException(Errors.FIELD_RENAMER_00, record.getHeader().getSourceId(),
       Joiner.on(", ").join(fieldsThatDoNotExist));
    }

    if (!overwriteExisting && !fieldsRequiringOverwrite.isEmpty()) {
      throw new OnRecordErrorException(Errors.FIELD_RENAMER_01,
        Joiner.on(", ").join(fieldsRequiringOverwrite),
          record.getHeader().getSourceId());
    }
    batchMaker.addRecord(record);
  }
}
