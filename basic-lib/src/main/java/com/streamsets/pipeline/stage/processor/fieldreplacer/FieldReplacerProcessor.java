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
package com.streamsets.pipeline.stage.processor.fieldreplacer;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.lib.el.FieldEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.util.FieldPathExpressionUtil;
import com.streamsets.pipeline.lib.util.FieldUtils;
import com.streamsets.pipeline.stage.processor.fieldreplacer.config.ReplaceRule;
import com.streamsets.pipeline.stage.processor.fieldreplacer.config.ReplacerConfigBean;

import java.util.List;

public class FieldReplacerProcessor extends SingleLaneRecordProcessor {

  private final ReplacerConfigBean conf;
  private ELEval pathEval;
  private ELEval replacementEval;

  public FieldReplacerProcessor(ReplacerConfigBean conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    pathEval = getContext().createELEval("fields");
    replacementEval = getContext().createELEval("replacement");

    return issues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    ELVars vars = getContext().createELVars();
    RecordEL.setRecordInContext(vars, record);

    for(ReplaceRule rule : conf.rules) {
      // Generate list of applicable paths to given expression
      List<String> fieldPaths = FieldPathExpressionUtil.evaluateMatchingFieldPaths(
        rule.fields,
        pathEval,
        vars,
        record,
        record.getEscapedFieldPaths()
      );

      if(fieldPaths.isEmpty() && conf.onStagePreConditionFailure == OnStagePreConditionFailure.TO_ERROR) {
        throw new OnRecordErrorException(record, Errors.FIELD_REPLACER_00, rule.fields);
      }

      // Perform the replacement
      for(String path : fieldPaths) {
        if(!record.has(path) && conf.onStagePreConditionFailure == OnStagePreConditionFailure.TO_ERROR) {
          throw new OnRecordErrorException(record, Errors.FIELD_REPLACER_00, rule.fields);
        }

        Field field = record.get(path);
        FieldEL.setFieldInContext(vars, path, null, field);

        if(rule.setToNull) {
          record.set(path, Field.create(field.getType(), null));
        } else {
          Object result = replacementEval.eval(vars, rule.replacement, Object.class);
          record.set(path, Field.create(FieldUtils.getTypeFromObject(result), result));
        }

      }
    }

    batchMaker.addRecord(record);
  }

}
