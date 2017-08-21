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
package com.streamsets.pipeline.stage.origin.elasticsearch;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class ElasticsearchSourceOffset implements Comparable<ElasticsearchSourceOffset> {
  private String scrollId;
  private String timeOffset;

  public ElasticsearchSourceOffset() {
    this(null, null);
  }

  public ElasticsearchSourceOffset(String scrollId) {
    this(scrollId, null);
  }

  public ElasticsearchSourceOffset(String scrollId, String timeOffset) {
    this.scrollId = scrollId;
    this.timeOffset = timeOffset;
  }

  public String getScrollId() {
    return scrollId;
  }

  public String getTimeOffset() {
    return timeOffset;
  }

  public void setScrollId(String scrollId) {
    this.scrollId = scrollId;
  }

  public void setTimeOffset(String timeOffset) {
    this.timeOffset = timeOffset;
  }

  @Override
  /*
   * Note: this class has a natural ordering that is inconsistent with equals.
   */
  public int compareTo(@NotNull ElasticsearchSourceOffset otherOffset) {
    if (timeOffset == null) {
      if (otherOffset.getTimeOffset() == null) {
        return 0;
      }
      return -1;
    }

    if (otherOffset.getTimeOffset() == null) {
      return 1;
    }

    return timeOffset.compareTo(otherOffset.getTimeOffset());
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ElasticsearchSourceOffset)) {
      return false;
    }

    ElasticsearchSourceOffset otherOffset = (ElasticsearchSourceOffset) obj;

    boolean scrollIdEqual;
    if (scrollId != null) {
      scrollIdEqual = scrollId.equals(otherOffset.getScrollId());
    } else {
      scrollIdEqual = otherOffset.getScrollId() == null;
    }

    boolean timeOffsetEqual;
    if (timeOffset != null) {
      timeOffsetEqual = timeOffset.equals(otherOffset.getTimeOffset());
    } else {
      timeOffsetEqual = otherOffset.getTimeOffset() == null;
    }

    return scrollIdEqual && timeOffsetEqual;
  }

  @Override
  public int hashCode() {
    return Objects.hash(scrollId, timeOffset);
  }
}
