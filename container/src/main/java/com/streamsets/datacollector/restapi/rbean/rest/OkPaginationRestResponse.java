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

import java.util.List;

public class OkPaginationRestResponse<R> extends AbstractRestResponse<List<R>, OkPaginationRestResponse<R>> {
  public static final String TYPE = "PAGINATED_DATA";

  private final String breadcrumbValue;

  private PaginationInfo paginationInfo;

  private OkPaginationRestResponse() {
    super(TYPE);
    this.breadcrumbValue = null;
  }

  public OkPaginationRestResponse(PaginationInfo paginationInfo) {
    this(paginationInfo, null);
  }

  public OkPaginationRestResponse(PaginationInfo paginationInfo, String breadcrumbValue) {
    super(TYPE);
    setPaginationInfo(paginationInfo);
    this.breadcrumbValue = breadcrumbValue;
  }

  public OkPaginationRestResponse<R> setPaginationInfo(PaginationInfo paginationInfo) {
    Preconditions.checkNotNull(paginationInfo, "paginationInfo cannot be NULL");
    this.paginationInfo = paginationInfo;
    return this;
  }

  public PaginationInfo getPaginationInfo() {
    return paginationInfo;
  }

  public String getBreadcrumbValue() {
    return breadcrumbValue;
  }

}
