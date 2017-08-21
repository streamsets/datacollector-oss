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

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;

import java.io.IOException;

public class Solr04ServerUtil extends SolrJettyTestBase {
  SolrServer solrClient;

  public Solr04ServerUtil(String url) throws Exception {
    try {
      solrClient = new HttpSolrServer(url);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void deleteByQuery(String q) throws SolrServerException, IOException {
    solrClient.deleteByQuery(q);
  }

  public QueryResponse query(SolrQuery q) throws SolrServerException {
    return solrClient.query(q);
  }

  public void destory() {
    if(solrClient != null) {
      solrClient.shutdown();
    }
  }
}
