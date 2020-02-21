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
package com.streamsets.pipeline.stage.origin.mysql.filters;

import com.streamsets.pipeline.stage.origin.event.EnrichedEvent;

/**
 * Filters out certain events.
 */
public interface Filter {
  /**
   * Returns <code>true</code> if event passes the filter and should go on, or <code>false</code>
   * if event is filtered out.
   *
   * @param event
   * @return {@link Result#PASS} if event passes the filter and should go on,
   * {@link Result#DISCARD} event is filtered out.
   */
  Result apply(EnrichedEvent event);

  Filter and(Filter filter);

  Filter or(Filter filter);

  enum Result {
    PASS,
    DISCARD
  }
}
