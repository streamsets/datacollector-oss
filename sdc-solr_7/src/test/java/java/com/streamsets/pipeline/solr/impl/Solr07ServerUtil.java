/*
 * Copyright 2018 StreamSets Inc.
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
package java.com.streamsets.pipeline.solr.impl;

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;

import java.io.IOException;

public class Solr07ServerUtil extends SolrJettyTestBase {
  private HttpSolrClient client;

  public Solr07ServerUtil(String url) throws Exception  {
    try {
      // setup the client...
      client = getHttpSolrClient(url);
      client.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
    }
    catch( Exception ex ) {
      throw new RuntimeException( ex );
    }
  }

  public void destroy() {
    if(client != null) {
      try {
        client.close();
      } catch (IOException ex) {

      }
    }
  }

  public void deleteByQuery(String q) throws SolrServerException, IOException {
    client.deleteByQuery(q);
  }

  public QueryResponse query(SolrQuery q) throws SolrServerException, IOException {
    return client.query(q);
  }
}
