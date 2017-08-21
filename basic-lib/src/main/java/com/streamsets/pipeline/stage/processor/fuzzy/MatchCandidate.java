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

  public boolean equals(MatchCandidate other) {
    return compareTo(other) == 0;
  }
}
