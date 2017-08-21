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
package com.streamsets.pipeline.stage.origin.http;

import com.streamsets.pipeline.api.BatchMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HttpSourceOffset {
  private static final Logger LOG = LoggerFactory.getLogger(HttpSourceOffset.class);

  private String url;
  private String parameterHash;
  private long time;
  private int startAt;

  HttpSourceOffset(String url, String parameterHash, long time, int startAt) {
    this.url = url;
    this.parameterHash = parameterHash;
    this.time = time;
    this.startAt = startAt;
  }

  String getUrl() {
    return url;
  }

  String getParameterHash() {
    return parameterHash;
  }

  long getTime() {
    return time;
  }

  int getStartAt() {
    return startAt;
  }

  @Override
  public String toString() {
    return "url::" + url + "::params::" + parameterHash + "::time::" + time + "::startAt::" + startAt;
  }

  /**
   * Parses an HttpSourceOffset from a string, e.g. from lastSourceOffset in
   * {@link HttpClientSource#produce(String, int, BatchMaker)}
   * @param s lastSourceOffset to parse
   * @return new instance of HttpSourceOffset
   */
  static HttpSourceOffset fromString(String s) {
    LOG.debug("Parsing HttpSourceOffset from '{}'", s);

    String[] parts = s.split("::");
    if (parts.length < 8) {
      throw new IllegalArgumentException("Offset must have at least 8 parts");
    }

    return new HttpSourceOffset(parts[1], parts[3], Long.parseLong(parts[5]), Integer.parseInt(parts[7]));
  }

  /**
   * Increment the {@link startAt} portion of the offset.
   * @param i number by which to increment startAt
   */
  public void incrementStartAt(int i) {
    this.startAt += i;
  }
}
