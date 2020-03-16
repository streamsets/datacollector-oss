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

import com.google.common.base.Splitter;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

/**
 * Binds an injector to convert pagination query string parameters into a PaginationInfo bean.
 */
public class PaginationInfoInjectorBinder extends AbstractBinder {
  public static final String ORDER_BY_PARAM = "orderBy";
  public static final String FILTER_BY_PARAM_PREFIX = "filterBy:";
  public static final String OFFSET_PARAM = "offset";
  public static final String LEN_PARAM = "len";

  private static final Logger LOG = LoggerFactory.getLogger(PaginationInfoInjectorBinder.class);

  @Override
  protected void configure() {
    bindFactory(PaginationInfoInjector.class).to(PaginationInfo.class).in(RequestScoped.class);
  }

  public static class PaginationInfoInjector implements Factory<PaginationInfo> {

    private final HttpServletRequest request;

    @Inject
    public PaginationInfoInjector(HttpServletRequest request) {
      this.request = request;
    }

    private long getNumber(String value, long defaultValue, String param, HttpServletRequest request) {
      if (value != null) {
        try {
          return Long.parseLong(value.trim());
        } catch (NumberFormatException ex) {
          LOG.warn("Could not parse '{}' from '{}', error: {}", param, request.getRequestURL() + "?" + request.getQueryString(), ex);
          return defaultValue;
        }
      } else {
        return defaultValue;
      }
    }

    @Override
    public PaginationInfo provide() {
      String orderByData = "";
      long offset = 0;
      long len = -1;
      Map<String, String[]> filterByData = new HashMap<>();
      if (request != null) {
        Map<String, String[]> parameterMap = request.getParameterMap();
        String[] values = parameterMap.get(ORDER_BY_PARAM);
        orderByData = (values == null) ? "" : values[0];


        for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
          String paramName = entry.getKey();
          if (paramName.startsWith(FILTER_BY_PARAM_PREFIX)) {
            String name = paramName.substring(FILTER_BY_PARAM_PREFIX.length());
            filterByData.put(name, entry.getValue());
          }
        }

        values = parameterMap.get(OFFSET_PARAM);
        String value = (values == null) ? null : values[0];
        offset = getNumber(value, 0, "offset", request);

        values = parameterMap.get(LEN_PARAM);
        value = (values == null) ? null : values[0];
        len = getNumber(value, -1, "len", request);
      }
      PaginationInfo paginationInfo = new PaginationInfo();
      paginationInfo.setOrderBy(Splitter.on(",").trimResults().omitEmptyStrings().splitToList(orderByData));
      paginationInfo.setFilterBy(filterByData);
      paginationInfo.setOffset(offset);
      paginationInfo.setLen(len);
      paginationInfo.setTotal(-1);
      return paginationInfo;
    }

    @Override
    public void dispose(PaginationInfo paginationInfo) {

    }

  }
}
