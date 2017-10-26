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
package com.streamsets.pipeline.stage.processor.fieldfilter;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.FieldPathExpressionUtil;
import com.streamsets.pipeline.lib.util.FieldRegexUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FieldFilterProcessor extends SingleLaneRecordProcessor {

  private final FilterOperation filterOperation;
  private final List<String> fields;
  private ELEval fieldPathEval;
  private ELVars fieldPathVars;


  public FieldFilterProcessor(FilterOperation filterOperation, List<String> fields) {
    this.filterOperation = filterOperation;
    this.fields = fields;
  }

  @Override
  protected List<ConfigIssue> init() {
    fieldPathEval = getContext().createELEval("fields");
    fieldPathVars = getContext().createELVars();
    return super.init();
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Set<String> fieldPaths = record.getEscapedFieldPaths();
    List<String> list;
    switch(filterOperation) {
      case REMOVE:
        list = new ArrayList<>();
        for(String field : fields) {
          List<String> matchingFieldPaths = FieldPathExpressionUtil.evaluateMatchingFieldPaths(
              field, fieldPathEval, fieldPathVars,
              record
          );
          list.addAll(matchingFieldPaths);
        }
        break;
      case REMOVE_NULL:
        list = new ArrayList<>();
        for (String field : fields) {
          List<String> matchingFieldPaths = FieldPathExpressionUtil.evaluateMatchingFieldPaths(
              field, fieldPathEval, fieldPathVars,
              record
          );
          for (String fieldPath : matchingFieldPaths) {
            if (fieldPaths.contains(fieldPath) && record.get(fieldPath).getValue() == null) {
              list.add(fieldPath);
            }
          }
        }
        break;
      case KEEP:
        //Algorithm:
        // - Get all possible field paths in the record
        //
        // - Remove arguments fields which must be retained, its parent fields and the child fields from above set
        //   (Account for presence of wild card characters while doing so) The remaining set of fields is what must be
        //   removed from the record.
        //
        // - Sort this set before deleting fields. Last element of a list must be removed first.

        Set<String> fieldsToRemove = new HashSet<>();
        //List all the possible field paths in this record
        fieldsToRemove.addAll(fieldPaths);

        for(String field : fields) {
          //Keep parent fields

          //get the parent fieldPaths for each of the fields to keep
          List<String> parentFieldPaths = getParentFields(field);
          //remove parent paths from the fieldsToRemove set
          //Note that parent names could contain wild card characters
          for(String parentField : parentFieldPaths) {
            List<String> matchingFieldPaths = FieldRegexUtil.getMatchingFieldPaths(parentField, fieldPaths);
            fieldsToRemove.removeAll(matchingFieldPaths);
          }

          //Keep the field itself

          //remove the field path itself from the fieldsToRemove set
          //Consider wild card characters

          List<String> matchingFieldPaths = FieldPathExpressionUtil.evaluateMatchingFieldPaths(
              field, fieldPathEval, fieldPathVars,
              record
          );
          fieldsToRemove.removeAll(matchingFieldPaths);
          //Keep the children of the field

          //For each of the fieldPaths that match the argument field path, generate all the child paths
          List<String> childFieldsToRemove = new ArrayList<>();
          for(String matchingFieldPath : matchingFieldPaths) {
            for(String fieldToRemove : fieldsToRemove) {
              // this change is due to SDC-3970 and SDC-3942 (and others).
              // for example, if the record has field1, field12, and field13 and the user
              // specifies "keep only field1", using startsWith will also match field12 and field13. so
              // in this case we need "equals".
              //
              // for the old way, startsWith is appropriate when we have
              // different path structures, or "nested" (multiple dimensioned) index structures.
              //  eg: /USA[0]/SanFrancisco/folsom/streets[0] must still match:
              //      /USA[0]/SanFrancisco/folsom/streets[0][0]   hence: startsWith.
              if (StringUtils.countMatches(fieldToRemove, "/") == StringUtils.countMatches(matchingFieldPath, "/")
                  && StringUtils.countMatches(fieldToRemove, "[") == StringUtils.countMatches(matchingFieldPath, "[")) {
                if (fieldToRemove.equals(matchingFieldPath)) {
                  childFieldsToRemove.add(fieldToRemove);
                }
              } else {
                if (fieldToRemove.startsWith(matchingFieldPath)) {
                  childFieldsToRemove.add(fieldToRemove);
                }
              }
            }
          }

          fieldsToRemove.removeAll(childFieldsToRemove);
        }
        list = new ArrayList<>(fieldsToRemove);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected Filter Operation '{}'", filterOperation.name()));
    }
    Collections.sort(list);
    for (int i = list.size()-1; i >= 0; i--) {
      record.delete(list.get(i));
    }
    batchMaker.addRecord(record);
  }

  private List<String> getParentFields(String fieldPath) {
    List<String> parentFields = new ArrayList<>();
    int index = 0;
    while(index < fieldPath.length()) {
      char c = fieldPath.charAt(index);
      switch(c) {
        case '/':
        case '[':
          parentFields.add(fieldPath.substring(0, index));
          break;
        default:
          break;
      }
      index++;
    }
    return parentFields;
  }

}
