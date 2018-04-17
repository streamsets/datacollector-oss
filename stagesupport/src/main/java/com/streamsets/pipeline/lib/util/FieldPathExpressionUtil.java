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

package com.streamsets.pipeline.lib.util;

import com.streamsets.datacollector.record.PathElement;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.FieldEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A utility class for evaluating field path expressions, in order to find fields in a record that match a given
 * expression. Intended to replace {@link FieldRegexUtil} (which it delegates to, in some cases, for performance
 * reasons) in functionality.
 */
public class FieldPathExpressionUtil {
  public static final Logger LOG = LoggerFactory.getLogger(FieldPathExpressionUtil.class);
  public static final String FIELD_PATH_EXPRESSION_PATTERN_STR = "^\\s*(\\$\\{.*\\})\\s*$";
  public static final Pattern FIELD_PATH_EXPRESSION_PATTERN = Pattern.compile(FIELD_PATH_EXPRESSION_PATTERN_STR);

  /**
   * Evaluates a given field path expression against a given record and returns all field paths that satisfy the
   * expression upon evaluation.  Analagous in function to {@link FieldRegexUtil#getMatchingFieldPaths(String, Set)},
   * and even identical in behavior to it when the given {@code fieldExpression} does not contain any EL expressions.
   *
   * @param fieldExpression the field path expression to evaluate
   * @param elEval the {@link ELEval} instance in which to evaluate the expression
   * @param elVars the {@link ELVars} instance to use when evaluating the expression
   * @param record the record against which to evaluate the expression; all returned values will be valid field paths
   *   within it
   * @return a {@link List} of field paths satisfying the given expression within the given {@code record}
   * @throws ELEvalException
   */
  public static List<String> evaluateMatchingFieldPaths(
      String fieldExpression,
      ELEval elEval,
      ELVars elVars,
      Record record
  ) throws ELEvalException {
    if (isFieldPathExpressionFast(fieldExpression)) {
      // this field path expression actually does contain an EL expression, so need to evaluate against all fields
      return evaluateMatchingFieldPathsImpl(fieldExpression, elEval, elVars, record);
    } else {
      // else it does NOT contain one, so the field regex util (which is faster) can be used
      return FieldRegexUtil.getMatchingFieldPaths(fieldExpression, record.getEscapedFieldPaths());
    }
  }

  private static List<String> evaluateMatchingFieldPathsImpl(
      String fieldExpression,
      ELEval elEval,
      ELVars elVars,
      Record record
  ) throws ELEvalException {
    RecordEL.setRecordInContext(elVars, record);
    List<String> matchingPaths = new LinkedList<>();
    for (String fieldPath : record.getEscapedFieldPaths()) {
      if (pathMatches(record, fieldPath, fieldExpression, elEval, elVars)) {
        matchingPaths.add(fieldPath);
      }
    }
    return matchingPaths;
  }

  /**
   * Evaluates a field expression against a specific field in a record to see if it matches
   *
   * @param record the record which contains the specified field
   * @param fieldPath the path to the specified field
   * @param fieldExpression the field expression to evaluate against the specified record/field
   * @param elEval the {@link ELEval} instance in which to evaluate the expression
   * @param elVars the {@link ELVars} instance to use when evaluating the expression
   * @return true if the expression matches the field, false otherwise
   * @throws ELEvalException if an error occurs during EL evaluation
   */
  private static boolean pathMatches(
      Record record,
      String fieldPath,
      String fieldExpression,
      ELEval elEval,
      ELVars elVars
  ) throws ELEvalException {
    List<PathElement> actualPathElements = PathElement.parse(fieldPath, true);
    List<PathElement> matcherPathElements = PathElement.parse(fieldExpression, false, true);

    Iterator<PathElement> currentPathIter = actualPathElements.iterator();
    Iterator<PathElement> matcherPathIter = matcherPathElements.iterator();

    PathElement currentMatcher = null;
    Field currentField = null;
    StringBuilder currentFieldPath = new StringBuilder();

    while (matcherPathIter.hasNext()) {
      currentMatcher = matcherPathIter.next();
      PathElement currentPath;
      switch (currentMatcher.getType()) {
        case MAP:
          if (!currentPathIter.hasNext()) {
            // we are expecting to match a MAP, but there are no more elements in the path
            return false;
          }
          currentPath = currentPathIter.next();

          // see if the name matches the pattern
          String childName = currentPath.getName();

          String patternName = currentMatcher.getName();
          if (FieldRegexUtil.hasWildCards(patternName)) {
            patternName = FieldRegexUtil.transformFieldPathRegex(patternName);
          }
          if (childName.matches(patternName)) {
            currentField = currentField.getValueAsMap().get(childName);
            currentFieldPath.append("/");
            currentFieldPath.append(childName);
          } else {
            return false;
          }

          break;
        case LIST:
          // see if the index matches the pattern

          if (!currentPathIter.hasNext()) {
            // we are expecting to match a LIST, but there are no more elements in the path
            return false;
          }
          currentPath = currentPathIter.next();

          int childIndex = currentPath.getIndex();
          final int matchInd = currentMatcher.getIndex();
          if (matchInd == PathElement.WILDCARD_INDEX_ANY_LENGTH
              || (matchInd == PathElement.WILDCARD_INDEX_SINGLE_CHAR && childIndex < 10)
              || matchInd == childIndex) {
            currentField = currentField.getValueAsList().get(childIndex);
            currentFieldPath.append("[");
            currentFieldPath.append(childIndex);
            currentFieldPath.append("]");
          } else {
            return false;
          }
          break;
        case ROOT:
          if (!currentPathIter.hasNext()) {
            // we are expecting to match the ROOT, but there are no more elements in the path
            return false;
          }
          currentPath = currentPathIter.next();
          if (currentPath.getType() != PathElement.Type.ROOT) {
            // we are expecting to match the ROOT, the root element wasn't for some reason
            return false;
          }
          currentField = record.get();
          break;
        case FIELD_EXPRESSION:
          // see if the current field matches the given expression
          FieldEL.setFieldInContext(elVars, currentFieldPath.toString(), currentField);
          String expression = currentMatcher.getName();
          final boolean result = elEval.eval(elVars, expression, Boolean.class);
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Result of evaluating expression {} on field {} with path {} was {}",
                expression,
                currentField,
                currentFieldPath,
                result
            );
          }
          if (!result) {
            return false;
          }
          break;
      }
    }

    return !currentPathIter.hasNext();
  }

  /**
   * Checks whether a given expression is a valid field path expression (i.e. contains an EL expression)
   *
   * @param expression the expression to test
   * @return true if a valid field path expression, false otherwise
   */
  public static boolean isFieldPathExpression(String expression) {
    return FIELD_PATH_EXPRESSION_PATTERN.matcher(expression).matches();
  }

  /**
   * Quickly checks whether a given expression is a likely a valid field path expression (i.e. contains an EL
   * expression)
   *
   * @param expression the expression to test
   * @return true if a valid field path expression, false otherwise
   */
  public static boolean isFieldPathExpressionFast(String expression) {
    return expression.contains("${");
  }
}
