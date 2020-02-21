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

import java.util.regex.Pattern;

import com.streamsets.pipeline.stage.origin.event.EnrichedEvent;

/**
 * Ignores events for given table. Database and table name are case-insensitive.
 */
public class IgnoreTableFilter implements Filter {
  private final Pattern tableName;
  private final Pattern dbName;

  public IgnoreTableFilter(String dbAndTable) {
    int i = dbAndTable.indexOf('.');
    if (i == -1) {
      throw new IllegalArgumentException("IgnoreTableFilter should have format 'db.tableName'");
    }
    String db = dbAndTable.substring(0, i);
    String table = dbAndTable.substring(i + 1, dbAndTable.length());
    tableName = Pattern.compile(table.trim().toLowerCase().replaceAll("%", ".*"));
    dbName = Pattern.compile(db.trim().toLowerCase().replaceAll("%", ".*"));
  }

  @Override
  public Result apply(EnrichedEvent event) {
    if (dbName.matcher(event.getTable().getDatabase().toLowerCase().trim()).matches() &&
        tableName.matcher(event.getTable().getName().toLowerCase().trim()).matches()) {
      return Result.DISCARD;
    }
    return Result.PASS;
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
    return "IgnoreTableFilter{" +
        "tableName=" + tableName +
        ", dbName=" + dbName +
        '}';
  }
}
