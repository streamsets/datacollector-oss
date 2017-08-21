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
package com.streamsets.pipeline.solr.impl;

import com.streamsets.pipeline.solr.api.SdcSolrTestUtil;
import com.streamsets.pipeline.solr.api.SdcSolrTestUtilFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Solr04TestUtilFactory extends SdcSolrTestUtilFactory {
  private final static Logger LOG = LoggerFactory.getLogger(Solr04TestUtilFactory.class);
  public Solr04TestUtilFactory() {
  }

  @Override
  public SdcSolrTestUtil create(String url) {
    try {
      return new Solr04TestUtil(url);
    } catch (Exception e) {
      LOG.error("create error: '{}'", e.toString(), e);
    }
    return null;
  }
}
