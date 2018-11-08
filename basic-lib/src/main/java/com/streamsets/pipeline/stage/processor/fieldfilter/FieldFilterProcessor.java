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
import java.util.List;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;

public class FieldFilterProcessor extends SingleLaneRecordProcessor {

  private final FilterOperation filterOperation;
  private final List<String> fields;
  private final String constant;
  private ELEval fieldPathEval;
  private ELVars fieldPathVars;


  public FieldFilterProcessor(FilterOperation filterOperation, List<String> fields, String constant) {
    this.filterOperation = filterOperation;
    this.fields = fields;
    this.constant = constant;
  }

  @Override
  protected List<ConfigIssue> init() {
    fieldPathEval = getContext().createELEval("fields");
    fieldPathVars = getContext().createELVars();
    return super.init();
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    // use List to preserve the order of list fieldPaths - need to watch out for duplicates though
    List<String> allFieldPaths = record.getEscapedFieldPathsOrdered();
    // use LinkedHashSet to preserve order and dedupe as we go
    LinkedHashSet<String> fieldsToRemove;
    switch(filterOperation) {
      case REMOVE:
        fieldsToRemove = new LinkedHashSet<>();
        for(String field : fields) {
          List<String> matchingFieldPaths = FieldPathExpressionUtil.evaluateMatchingFieldPaths(
              field,
              fieldPathEval,
              fieldPathVars,
              record,
              allFieldPaths
          );
          fieldsToRemove.addAll(matchingFieldPaths);
        }
        break;
      case REMOVE_NULL:
        fieldsToRemove = new LinkedHashSet<>();
        for (String field : fields) {
          List<String> matchingFieldPaths = FieldPathExpressionUtil.evaluateMatchingFieldPaths(
              field,
              fieldPathEval,
              fieldPathVars,
              record,
              allFieldPaths
          );
          for (String fieldPath : matchingFieldPaths) {
            if (record.has(fieldPath) && record.get(fieldPath).getValue() == null) {
              fieldsToRemove.add(fieldPath);
            }
          }
        }
        break;
      case REMOVE_EMPTY:
        fieldsToRemove = new LinkedHashSet<>();
        for (String field : fields) {
          List<String> matchingFieldPaths = FieldPathExpressionUtil.evaluateMatchingFieldPaths(
              field,
              fieldPathEval,
              fieldPathVars,
              record,
              allFieldPaths
          );
          for (String fieldPath : matchingFieldPaths) {
            if (record.has(fieldPath)
                && record.get(fieldPath).getValue() != null && record.get(fieldPath).getValue().equals("")) {
              fieldsToRemove.add(fieldPath);
            }
          }
        }
        break;
      case REMOVE_NULL_EMPTY:
        fieldsToRemove = new LinkedHashSet<>();
        for (String field : fields) {
          List<String> matchingFieldPaths = FieldPathExpressionUtil.evaluateMatchingFieldPaths(
              field,
              fieldPathEval,
              fieldPathVars,
              record,
              allFieldPaths
          );
          for (String fieldPath : matchingFieldPaths) {
            if (record.has(fieldPath)
                && (record.get(fieldPath).getValue() == null || record.get(fieldPath).getValue().equals(""))) {
              fieldsToRemove.add(fieldPath);
            }
          }
        }
        break;
      case REMOVE_CONSTANT:
        fieldsToRemove = new LinkedHashSet<>();
        for (String field : fields) {
          List<String> matchingFieldPaths = FieldPathExpressionUtil.evaluateMatchingFieldPaths(
              field,
              fieldPathEval,
              fieldPathVars,
              record,
              allFieldPaths
          );
          for (String fieldPath : matchingFieldPaths) {
            if (record.has(fieldPath)
                && record.get(fieldPath).getValue() != null && record.get(fieldPath).getValue().equals(constant)) {
              fieldsToRemove.add(fieldPath);
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
        // - Keep fieldsToRemove in order - sorting is too costly
        //List all the possible field paths in this record
        fieldsToRemove = new LinkedHashSet<>(allFieldPaths);
        for(String field : fields) {
          //Keep parent fields
          //get the parent fieldPaths for each of the fields to keep
          List<String> parentFieldPaths = getParentFields(field);
          //remove parent paths from the fieldsToRemove set
          //Note that parent names could contain wild card characters
          for(String parentField : parentFieldPaths) {
            List<String> matchingFieldPaths = FieldRegexUtil.getMatchingFieldPaths(parentField, allFieldPaths);
            fieldsToRemove.removeAll(matchingFieldPaths);
          }

          //Keep the field itself
          //remove the field path itself from the fieldsToRemove set
          //Consider wild card characters
          List<String> matchingFieldPaths = FieldPathExpressionUtil.evaluateMatchingFieldPaths(
              field,
              fieldPathEval,
              fieldPathVars,
              record,
              allFieldPaths
          );
          fieldsToRemove.removeAll(matchingFieldPaths);

          //Keep the children of the field
          //For each of the fieldPaths that match the argument field path, remove all the child paths
          // Remove children at the end to avoid ConcurrentModificationException
          Set<String> childrenToKeep = new HashSet<>();
          for(String matchingFieldPath : matchingFieldPaths) {
            for(String fieldToRemove : fieldsToRemove) {
              // for the old way, startsWith is appropriate when we have
              // different path structures, or "nested" (multiple dimensioned) index structures.
              //  eg: /USA[0]/SanFrancisco/folsom/streets[0] must still match:
              //      /USA[0]/SanFrancisco/folsom/streets[0][0]   hence: startsWith.
              if (StringUtils.countMatches(fieldToRemove, "/") == StringUtils.countMatches(matchingFieldPath, "/")
                  && StringUtils.countMatches(fieldToRemove, "[") == StringUtils.countMatches(matchingFieldPath, "[")) {
                if (fieldToRemove.equals(matchingFieldPath)) {
                  childrenToKeep.add(fieldToRemove);
                }
              } else {
                if (fieldToRemove.startsWith(matchingFieldPath)) {
                  childrenToKeep.add(fieldToRemove);
                }
              }
            }
          }
          fieldsToRemove.removeAll(childrenToKeep);
        }
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected Filter Operation '{}'", filterOperation.name()));
    }
    // We don't sort because we maintained list fields in ascending order (but not a full ordering)
    // Instead we just iterate in reverse to delete
    Iterator<String> itr = (new LinkedList<>(fieldsToRemove)).descendingIterator();
    while(itr.hasNext()) {
      record.delete(itr.next());
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
