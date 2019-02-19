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
package com.streamsets.pipeline.solr.api;

import com.streamsets.pipeline.api.StageException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface SdcSolrTarget {
  public final static String NAME = "name";
  public final static String REQUIRED = "required";

  public void init() throws Exception;

  public void destroy() throws IOException;

  public void add(Map<String, Object> fieldMap) throws StageException;

  public void add(List<Map<String, Object>> fieldMaps) throws StageException;

  public void commit() throws StageException;

  public String getVersion();

  public List<String> getRequiredFieldNamesMap();

  public List<String> getOptionalFieldNamesMap();

}
