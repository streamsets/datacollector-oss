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
package com.streamsets.pipeline.stage.processor.fieldrenamer;

import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.lib.util.FieldRegexUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
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
  private static final Logger LOG = LoggerFactory.getLogger(FieldRenamerProcessor.class);
  private final FieldRenamerProcessorErrorHandler errorHandler;
  private final FieldRenamerPathComparator comparator;

  //So that the ordering of fieldPaths will be preserved
  private Map<Pattern, String> fromPatternToFieldExpMapping;

  private final List<FieldRenamerConfig> renameMapping;

  private ELEval targetFieldEval;
  private LoadingCache<Set<String>, CachedResults> cache;
  private int count = 0;

  public FieldRenamerProcessor(
      List<FieldRenamerConfig> renameMapping,
      FieldRenamerProcessorErrorHandler errorHandler
  ) {
    this.renameMapping = Utils.checkNotNull(renameMapping, "Rename mapping cannot be null");
    this.errorHandler = errorHandler;
    this.fromPatternToFieldExpMapping = new LinkedHashMap<>();
    this.comparator = new FieldRenamerPathComparator();
  }

  /**
   * Add backslash to escape the | character within quoted sections of the
   * input string.  This prevents the | from being processed as part of a regex.
   * @param input Contains the field name to process.
   * @return New string
   */
  private static String escapeQuotedSubstring(String input) {
    String[] parts = input.split("'");
    StringBuilder output = new StringBuilder(input.length() * 2);
    for (int i = 0; i < parts.length; i++) {
      if ((i % 2) == 1) {
        output.append("'").append(parts[i].replace("|", "\\|")).append("'");
      } else {
        output.append(parts[i]);
      }
    }
    return output.toString();
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    for (FieldRenamerConfig renameConfig : renameMapping) {
      String fromFieldPathRegex = FieldRegexUtil.patchUpFieldPathRegex(renameConfig.fromFieldExpression);
      fromFieldPathRegex = escapeQuotedSubstring(fromFieldPathRegex);
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

    targetFieldEval = getContext().createELEval("toFieldExpression");

    cache = CacheBuilder
        .newBuilder()
        .maximumSize(500)
        .recordStats()
        .build(new CacheLoader<Set<String>, CachedResults>() {
      @Override
      public CachedResults load(Set<String> fieldPaths) throws Exception {
        return findFields(fieldPaths);
      }
    });

    return issues;
  }

  static class FieldRenamerPathComparator implements Comparator<String> {
    //Reverse order for array indices so they get selected/deleted correctly (we should delete from the highest index).
    //rest ordering is incoming order.
    // (We actually do care about incoming order as user can write them in a a particular logic)
    @Override
    public int compare(String o1, String o2) {
      boolean isO1ArrayField = o1.contains("[") && o1.contains("]");
      boolean isO2ArrayField = o2.contains("[") && o2.contains("]");
      if (isO1ArrayField && isO2ArrayField) {
        int i = 0;
        for (; i < o1.length() && i < o2.length() && o1.charAt(i) == o2.charAt(i); i++);
        //Matched the longest common prefix.
        if (i != o1.length() && i != o2.length() && o1.charAt(i-1) == '[') {
          int o1ArrayIndexEnd = i, o2ArrayIndexEnd = i;
          for (;o1.charAt(o1ArrayIndexEnd) != ']'; o1ArrayIndexEnd++);
          for (;o2.charAt(o2ArrayIndexEnd) != ']'; o2ArrayIndexEnd++);
          return Integer.valueOf(
              Integer.parseInt(o2.substring(i, o2ArrayIndexEnd))
          ).compareTo(
              Integer.parseInt(o1.substring(i, o1ArrayIndexEnd))
          );
        }
        //if any of the string length is crossed or only some word prefix is matched (i.e last match  is not [)
        //We will use the incoming order as is.
      }
      //This will make sure the incoming order is preserved
      return 1;
    }
  }

  private void populateFromAndToFieldsMap(
      Pattern fromFieldPattern,
      String toFieldExpression,
      Set<String> fieldPaths,
      Set<String> fieldsThatDoNotExist,
      Map<String, Set<String>> multipleRegexMatchingSameFields,
      Map<String, String> matchedByRegularExpression,
      Map<String, String> fromFieldToFieldMap
  ) throws ELEvalException {
    boolean hasSourceFieldsMatching = false;
    //Making sure array fields  matched by regular expressions
    //are always sorted in descending order so as to preserve ordering
    Map<String, String> tobeAdded  = new TreeMap<>(comparator);
    for(String existingFieldPath : fieldPaths) {
      Matcher matcher = fromFieldPattern.matcher(existingFieldPath);
      if (matcher.matches()) {
        hasSourceFieldsMatching = true;
//        String toFieldPath = matcher.replaceAll(toFieldExpression);
        String toFieldPath = toFieldExpression;
        for(int i = 0; i <= matcher.groupCount(); i++) {
          toFieldPath = toFieldPath.replace("$" + i,  matcher.group(i));
        }

        toFieldPath = targetFieldEval.eval(getContext().createELVars(), toFieldPath, String.class);
        if (fromFieldToFieldMap.containsKey(existingFieldPath)) {
          Set<String> patterns =  multipleRegexMatchingSameFields.get(existingFieldPath);
          if (patterns == null) {
            patterns = new LinkedHashSet<>();
          }
          patterns.add(fromFieldPattern.pattern());
          patterns.add(matchedByRegularExpression.get(existingFieldPath));
          multipleRegexMatchingSameFields.put(existingFieldPath, patterns);
        } else {
          tobeAdded.put(existingFieldPath, toFieldPath);
          matchedByRegularExpression.put(existingFieldPath, fromFieldPattern.pattern());
        }
      }
    }
    fromFieldToFieldMap.putAll(tobeAdded);
    if (!hasSourceFieldsMatching) {
      fieldsThatDoNotExist.add(fromFieldPattern.pattern());
    }
  }

  private class CachedResults {
    Set<String> fieldsRequiringOverwrite;
    Set<String> fieldsThatDoNotExist;
    Map<String, Set<String>> multipleRegexMatchingSameFields;
    //So that the ordering of fieldPaths will be preserved
    Map<String, String> fromFieldToFieldMap;

    public CachedResults(
        Set<String> fieldsRequiringOverwrite,
        Set<String> fieldsThatDoNotExist,
        Map<String, Set<String>> multipleRegexMatchingSameFields,
        Map<String, String> fromFieldToFieldMap
    ) {

      this.multipleRegexMatchingSameFields = multipleRegexMatchingSameFields;
      this.fieldsRequiringOverwrite = fieldsRequiringOverwrite;
      this.fieldsThatDoNotExist = fieldsThatDoNotExist;
      this.fromFieldToFieldMap = fromFieldToFieldMap;

    }

  }

  private CachedResults findFields(Set<String> newPaths) throws ELEvalException {

    Set<String> fieldsRequiringOverwrite = new HashSet<>();
    Set<String> fieldsThatDoNotExist = new HashSet<>();
    Map<String, Set<String>> multipleRegexMatchingSameFields = new HashMap<>();
    Map<String, String> matchedByRegularExpression = new HashMap<>();

    //So that the ordering of fieldPaths will be preserved
    Map<String, String> fromFieldToFieldMap = new LinkedHashMap<>();

    for (Map.Entry<Pattern, String> fromPatternToFieldExpEntry : fromPatternToFieldExpMapping.entrySet()) {
      populateFromAndToFieldsMap(
          fromPatternToFieldExpEntry.getKey(),
          fromPatternToFieldExpEntry.getValue(),
          newPaths,
          fieldsThatDoNotExist,
          multipleRegexMatchingSameFields,
          matchedByRegularExpression,
          fromFieldToFieldMap
      );
    }

    //We should not process fields which are matched by multiple regex
    for (String fieldsMatchedByMultipleExpression : multipleRegexMatchingSameFields.keySet()) {
      fromFieldToFieldMap.remove(fieldsMatchedByMultipleExpression);
    }

    return new CachedResults(fieldsRequiringOverwrite,
        fieldsThatDoNotExist,
        multipleRegexMatchingSameFields,
        fromFieldToFieldMap);
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {


    Map<String, String> fromFieldToFieldMap;
    Set<String> fieldsRequiringOverwrite;
    Set<String> fieldsThatDoNotExist;
    Map<String, Set<String>> multipleRegexMatchingSameFields;

    CachedResults data;
    try {
      data = cache.get(record.getEscapedFieldPaths());
      count++;
      if(count == 50000) {
        count = 0;
        CacheStats stats = cache.stats();
        LOG.debug("cache_stats: hits {} misses {} rate {} ",
            stats.hitCount(),
            stats.missCount(),
            stats.hitRate());
      }
    } catch (ExecutionException ex) {
      LOG.error(Errors.FIELD_RENAMER_05.getMessage(), ex.toString(), ex);
      throw new StageException(Errors.FIELD_RENAMER_05, ex.toString(), ex);
    }

    fromFieldToFieldMap = data.fromFieldToFieldMap;
    fieldsRequiringOverwrite = data.fieldsRequiringOverwrite;
    fieldsThatDoNotExist = data.fieldsThatDoNotExist;
    multipleRegexMatchingSameFields = data.multipleRegexMatchingSameFields;

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
            for (; record.has(toFieldName + i); i++);
            toFieldName = toFieldName + i;
            //Fall through so as to edit.
          case REPLACE:
            // No need to bother if the field is being renamed to itself; plus, we don't want to delete it either
            if (!toFieldName.equals(fromFieldName)) {
              record.set(toFieldName, fromField);
              record.delete(fromFieldName);
            }
            break;
        }
      } else {
        try {
          record.set(toFieldName, fromField);
        } catch (IllegalArgumentException e) {
          throw new OnRecordErrorException(record, Errors.FIELD_RENAMER_04, toFieldName, e.toString());
        }
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
      for (Map.Entry<String, Set<String>> multipleRegexMatchingSameFieldEntry
          : multipleRegexMatchingSameFields.entrySet()) {
        sb.append(" Field: ")
            .append(multipleRegexMatchingSameFieldEntry.getKey())
            .append(" Regex: ")
            .append(Joiner.on(",").join(multipleRegexMatchingSameFieldEntry.getValue()));
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
