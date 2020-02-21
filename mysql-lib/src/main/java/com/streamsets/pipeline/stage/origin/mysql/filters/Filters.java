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
 * Filters utility methods.
 */
public class Filters {
  private Filters() {
  }

  public static final Filter PASS = new Filter() {
    @Override
    public Filter.Result apply(EnrichedEvent event) {
      return Filter.Result.PASS;
    }

    @Override
    public Filter and(Filter filter) {
      return Filters.and(this, filter);
    }

    @Override
    public Filter or(Filter filter) {
      return Filters.or(this, filter);
    }

    @Override
    public String toString() {
      return "Filters.PASS";
    }
  };

  public static final Filter DISCARD = new Filter() {
    @Override
    public Filter.Result apply(EnrichedEvent event) {
      return Result.DISCARD;
    }

    @Override
    public Filter and(Filter filter) {
      return Filters.and(this, filter);
    }

    @Override
    public Filter or(Filter filter) {
      return Filters.or(this, filter);
    }

    @Override
    public String toString() {
      return "Filters.DISCARD";
    }
  };

  public static Filter and(Filter filter1, Filter filter2) {
    return new AndFilter(filter1, filter2);
  }

  public static Filter or(Filter filter1, Filter filter2) {
    return new OrFilter(filter1, filter2);
  }

  private static class AndFilter implements Filter {
    private final Filter filter1;
    private final Filter filter2;

    private AndFilter(Filter filter1, Filter filter2) {
      this.filter1 = filter1;
      this.filter2 = filter2;
    }

    @Override
    public Result apply(EnrichedEvent event) {
      return filter1.apply(event) == Result.PASS ? filter2.apply(event) : Result.DISCARD;
    }

    @Override
    public Filter and(Filter filter) {
      return new AndFilter(this, filter);
    }

    @Override
    public Filter or(Filter filter) {
      return new OrFilter(this, filter);
    }

    @Override
    public String toString() {
      return "AndFilter{" +
          "filter1=" + filter1 +
          ", filter2=" + filter2 +
          '}';
    }
  }

  private static class OrFilter implements Filter {
    private final Filter filter1;
    private final Filter filter2;

    private OrFilter(Filter filter1, Filter filter2) {
      this.filter1 = filter1;
      this.filter2 = filter2;
    }

    @Override
    public Result apply(EnrichedEvent event) {
      return filter1.apply(event) == Result.PASS ? Result.PASS : filter2.apply(event);
    }

    @Override
    public Filter and(Filter filter) {
      return new AndFilter(this, filter);
    }

    @Override
    public Filter or(Filter filter) {
      return new OrFilter(this, filter);
    }

    @Override
    public String toString() {
      return "OrFilter{" +
          "filter1=" + filter1 +
          ", filter2=" + filter2 +
          '}';
    }
  }
}
