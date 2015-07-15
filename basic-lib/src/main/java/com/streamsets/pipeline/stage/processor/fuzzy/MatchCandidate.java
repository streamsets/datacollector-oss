package com.streamsets.pipeline.stage.processor.fuzzy;

import com.streamsets.pipeline.api.Field;

class MatchCandidate implements Comparable<MatchCandidate> {
  private final String forFieldName;
  private final int score;
  private final String fieldPath;
  private final Field field;

  public MatchCandidate(String forFieldName, int score, String fieldPath, Field field) {
    this.forFieldName = forFieldName;
    this.score = score;
    this.fieldPath = fieldPath;
    this.field = field;
  }

  public String getForFieldName() {
    return forFieldName;
  }

  public int getScore() {
    return score;
  }

  public Field getField() {
    return field;
  }

  public String getFieldPath() {
    return fieldPath;
  }

  @Override
  public int compareTo(MatchCandidate other) {
    return Integer.compare(this.score, other.getScore());
  }
}
