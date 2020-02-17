/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.restapi.rbean.rest;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaginationInfo {
  private List<String> orderBy = Collections.emptyList();
  private Map<String, String[]> filterBy = Collections.emptyMap();
  private long total = -1;
  private long offset;
  private long len = -1;

  public List<String> getOrderBy() {
    return ImmutableList.copyOf(orderBy);
  }

  public PaginationInfo setOrderBy(List<String> orderBy) {
    Preconditions.checkNotNull(orderBy, "orderBy cannot be NULL");
    this.orderBy = new ArrayList<>(orderBy);
    return this;
  }

  public Map<String, String[]> getFilterBy() {
    return filterBy;
  }

  public PaginationInfo setFilterBy(Map<String, String[]> filterBy) {
    Preconditions.checkNotNull(filterBy, "filterBy cannot be NULL");
    this.filterBy = new HashMap<>(filterBy);
    return this;
  }

  public long getTotal() {
    return total;
  }

  public PaginationInfo setTotal(long total) {
    this.total = total;
    return this;
  }

  public long getOffset() {
    return offset;
  }

  public PaginationInfo setOffset(long offset) {
    this.offset = offset;
    return this;
  }

  public long getLen() {
    return len;
  }

  public PaginationInfo setLen(long len) {
    this.len = len;
    return this;
  }

  @Override
  public String toString() {
    return "PaginationInfo{" +
        "orderBy=" +
        orderBy +
        ", filterBy=" +
        filterBy +
        ", total=" +
        total +
        ", offset=" +
        offset +
        ", len=" +
        len +
        '}';
  }

}
