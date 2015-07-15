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

public class FuzzyFieldProcessor extends SingleLaneRecordProcessor {
  private static final String HEADER = "header";
  private static final String VALUE = "value";
  private static final String SCORE = "score";

  private final List<String> rootFieldPaths;
  private final List<String> outputFieldNames;
  private final int matchThreshold;
  private final boolean allCandidates;
  private final boolean inPlace;

  public FuzzyFieldProcessor(
      List<String> rootFieldPaths,
      List<String> outputFieldNames,
      int matchThreshold,
      boolean allCandidates,
      boolean inPlace
  ) {
    this.rootFieldPaths = rootFieldPaths;
    this.outputFieldNames = outputFieldNames;
    this.matchThreshold = matchThreshold;
    this.allCandidates = allCandidates;
    this.inPlace = inPlace;
  }

  @Override
  public void destroy() {
    super.destroy();
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    for (String rootFieldPath : rootFieldPaths) {
      Field rootField = record.get(rootFieldPath);
      SortedSetMultimap<String, MatchCandidate> candidates = findCandidatesFor(rootField);

      Map<String, Field> newFields = new HashMap<>();

      for (Map.Entry<String, Collection<MatchCandidate>> entry : candidates.asMap().entrySet()) {
        Collection<MatchCandidate> candidatesForKey = entry.getValue();

        Iterable<Field> fieldIterable = Iterables.transform(candidatesForKey, new Function<MatchCandidate, Field>() {
          @Nullable
          @Override
          public Field apply(MatchCandidate input) {
            Field field;
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

  private TreeMultimap<String, MatchCandidate> findCandidatesFor(Field rootField) throws StageException {
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

    TreeMultimap<String, MatchCandidate> candidates = TreeMultimap.create();

    for (String outputField : outputFieldNames) {
      for (Map.Entry<String, Field> entry : fields.entrySet()) {
        String fieldPath = entry.getKey();
        Field field = entry.getValue();
        int score = FuzzyMatch.getRatio(outputField, fieldPath);
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
