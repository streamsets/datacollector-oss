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
package com.streamsets.pipeline.stage.processor.fuzzy;

import com.google.api.client.util.Lists;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.lib.fuzzy.FuzzyMatch;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

public class FuzzyFieldProcessor extends SingleLaneRecordProcessor {
  private static final String HEADER = "header";
  private static final String VALUE = "value";
  private static final String SCORE = "score";

  private final List<String> rootFieldPaths;
  private final List<String> outputFieldNames;
  private final int matchThreshold;
  private final boolean allCandidates;
  private final boolean inPlace;
  private final boolean preserveUnmatchedFields;

  public FuzzyFieldProcessor(
      List<String> rootFieldPaths,
      List<String> outputFieldNames,
      int matchThreshold,
      boolean allCandidates,
      boolean inPlace,
      boolean preserveUnmatchedFields
  ) {
    this.rootFieldPaths = rootFieldPaths;
    this.outputFieldNames = outputFieldNames;
    this.matchThreshold = matchThreshold;
    this.allCandidates = allCandidates;
    this.inPlace = inPlace;
    this.preserveUnmatchedFields = preserveUnmatchedFields;
  }

  @Override
  public void destroy() {
    super.destroy();
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    for (String rootFieldPath : rootFieldPaths) {
      final Field rootField = record.get(rootFieldPath);
      final Map<String, Field> rootFieldMap = flattenRootField(rootField);
      final Set<String> originalFieldNames = rootFieldMap.keySet();

      SortedSetMultimap<String, MatchCandidate> candidates = findCandidatesFor(rootFieldMap);

      Map<String, Field> newFields = new HashMap<>();

      for (Map.Entry<String, Collection<MatchCandidate>> entry : candidates.asMap().entrySet()) {
        Collection<MatchCandidate> candidatesForKey = entry.getValue();

        Iterable<Field> fieldIterable = Iterables.transform(candidatesForKey, new Function<MatchCandidate, Field>() {
          @Nullable
          @Override
          public Field apply(MatchCandidate input) {
            Field field;
            originalFieldNames.remove(input.getFieldPath());

            if (inPlace) {
              field = input.getField();
            } else {
              Map<String, Field> map = new HashMap<>();
              map.put(HEADER, Field.create(input.getFieldPath()));
              map.put(VALUE, input.getField());
              map.put(SCORE, Field.create(input.getScore()));
              field = Field.create(map);
            }
            return field;
          }
        });

        List<Field> fieldCandidates = Lists.newArrayList(fieldIterable);
        // Flatten this is there's we're only keeping a single candidate
        if (allCandidates) {
          newFields.put(entry.getKey(), Field.create(fieldCandidates));
        } else {
          newFields.put(entry.getKey(), fieldCandidates.get(0));
        }
      }

      if (preserveUnmatchedFields) {
        for (Map.Entry<String, Field> entry : rootFieldMap.entrySet()) {
          newFields.put(entry.getKey(), entry.getValue());
        }
      }

      record.set(rootFieldPath, Field.create(newFields));
    }

    batchMaker.addRecord(record);
  }

  private boolean greaterScore(NavigableSet<MatchCandidate> matchCandidates, int score) {
    boolean isMaxScore = false;
    if (matchCandidates.isEmpty()) {
      isMaxScore = true;
    } else if (matchCandidates.last().getScore() < score) {
      isMaxScore = true;
    }
    return isMaxScore;
  }

  private Map<String, Field> flattenRootField(Field rootField) throws StageException {
    Map<String, Field> fields;
    switch (rootField.getType()) {
      case LIST:
        // Flatten CSV input types.
        List<Field> fieldList = rootField.getValueAsList();
        fields = new HashMap<>();
        for (Field field : fieldList) {
          Map<String, Field> fieldWithHeader = field.getValueAsMap();
          if (!fieldWithHeader.containsKey(HEADER) || !fieldWithHeader.containsKey(VALUE)) {
            throw new StageException(Errors.FUZZY_00, rootField.getType());
          }
          fields.put(fieldWithHeader.get(HEADER).getValueAsString(), fieldWithHeader.get(VALUE));
        }
        break;
      case MAP:
        fields = rootField.getValueAsMap();
        break;
      default:
        throw new StageException(Errors.FUZZY_00, rootField.getType());
    }
    return fields;
  }

  private TreeMultimap<String, MatchCandidate> findCandidatesFor(Map<String, Field> fields) throws StageException {
    TreeMultimap<String, MatchCandidate> candidates = TreeMultimap.create();

    for (String outputField : outputFieldNames) {
      for (Map.Entry<String, Field> entry : fields.entrySet()) {
        String fieldPath = entry.getKey();
        Field field = entry.getValue();
        int score = FuzzyMatch.getRatio(outputField, fieldPath, true);
        if (score >= matchThreshold) {
          if (greaterScore(candidates.get(outputField), score) || allCandidates) {
            if (!allCandidates) {
              // If not storing all candidates we must clear any existing candidates for this key
              // since a Multimap won't replace multiple values for a key.
              candidates.get(outputField).clear();
            }
            candidates.put(outputField, new MatchCandidate(outputField, score, fieldPath, field));
          }
        }
      }
    }

    return candidates;
  }
}
