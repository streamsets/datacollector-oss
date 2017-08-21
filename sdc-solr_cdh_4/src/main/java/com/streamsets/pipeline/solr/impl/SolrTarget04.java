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

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.solr.api.Errors;
import com.streamsets.pipeline.solr.api.SdcSolrTarget;
import com.streamsets.pipeline.solr.api.SolrInstanceAPIType;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SolrTarget04 implements SdcSolrTarget {
  private final static Logger LOG = LoggerFactory.getLogger(SolrTarget04.class);
  private SolrServer solrClient;

  private String solrURI;
  private String zookeeperConnect;
  private String defaultCollection;
  private String instanceType;
  private boolean kerberosAuth;
  private boolean skipValidation;
  private static final String VERSION = "4.4.0";


  public SolrTarget04(
      String instanceType,
      String solrURI,
      String zookeeperConnect,
      String defaultCollection,
      boolean kerberosAuth,
      boolean skipValidation
  ) {
    this.instanceType = instanceType;
    this.solrURI = solrURI;
    this.zookeeperConnect = zookeeperConnect;
    this.defaultCollection = defaultCollection;
    this.kerberosAuth = kerberosAuth;
    this.skipValidation = skipValidation;
  }

  public void init() throws Exception {
    solrClient = getSolrClient();
    if (!skipValidation) {
      solrClient.ping();
    }
  }

  private SolrServer getSolrClient() throws MalformedURLException {
    if(kerberosAuth) {
      // set kerberos before create SolrClient
      addSecurityProperties();
    }

    if (SolrInstanceAPIType.SINGLE_NODE.toString().equals(this.instanceType)) {
      return new HttpSolrServer(this.solrURI);
    } else {
      CloudSolrServer cloudSolrClient = new CloudSolrServer(this.zookeeperConnect);
      cloudSolrClient.setDefaultCollection(this.defaultCollection);
      return cloudSolrClient;
    }
  }

  private void addSecurityProperties() {
    HttpClientUtil.setConfigurer(new SdcKrb5HttpClientConfigurer());
  }

  public void destroy() throws IOException{
    if(this.solrClient != null) {
      this.solrClient.shutdown();
    }
  }

  public void add(Map<String, Object> fieldMap) throws StageException {
    SolrInputDocument document = createDocument(fieldMap);
    try {
      this.solrClient.add(document);
    } catch (SolrServerException | IOException ex) {
      throw new StageException(Errors.SOLR_04, ex.toString(), ex);
    }
  }

  @SuppressWarnings("unchecked")
  public void add(List<Map<String, Object>> fieldMaps) throws StageException {
    List<SolrInputDocument> documents = new ArrayList();
    for(Map<String, Object> fieldMap : fieldMaps) {
      SolrInputDocument document = createDocument(fieldMap);
      documents.add(document);
    }

    try {
      this.solrClient.add(documents);
    } catch (SolrServerException | IOException | HttpSolrServer.RemoteSolrException ex) {
      throw new StageException(Errors.SOLR_04, ex.toString(), ex);
    }
  }

  @SuppressWarnings("unchecked")
  private SolrInputDocument createDocument(Map<String, Object> fieldMap) {
    SolrInputDocument document = new SolrInputDocument();
    for(Map.Entry<String, Object> entry : fieldMap.entrySet()) {
      document.addField(entry.getKey(), entry.getValue());
    }
    return document;
  }

  public void commit() throws StageException {
    try {
      this.solrClient.commit();
    } catch (SolrServerException | IOException ex) {
      throw new StageException(Errors.SOLR_05, ex.toString(), ex);
    }
  }

  public void rollback() throws StageException {
    try {
      this.solrClient.rollback();
    } catch (SolrServerException | IOException ex) {
      throw new StageException(Errors.SOLR_05, ex.toString(), ex);
    }
  }

  public String getVersion() {
    return this.VERSION;
  }

}
