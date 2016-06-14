/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SolrTarget06 implements SdcSolrTarget {
  private final static Logger LOG = LoggerFactory.getLogger(SolrTarget06.class);
  private SolrClient solrClient;

  private String solrURI;
  private String zookeeperConnect;
  private String defaultCollection;
  private String instanceType;
  private static final String VERSION ="6.1.0";


  public SolrTarget06(
      String instanceType,
      String solrURI,
      String zookeeperConnect,
      String defaultCollection
  ) {
    this.instanceType = instanceType;
    this.solrURI = solrURI;
    this.zookeeperConnect = zookeeperConnect;
    this.defaultCollection = defaultCollection;
  }

  public void init() throws Exception {
    solrClient = getSolrClient();
    solrClient.ping();
  }

  private SolrClient getSolrClient() throws MalformedURLException {
    if (SolrInstanceAPIType.SINGLE_NODE.toString().equals(this.instanceType)) {
      return new HttpSolrClient.Builder(this.solrURI).build();
    } else {
      CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder().withZkHost(this.zookeeperConnect).build();
      cloudSolrClient.setDefaultCollection(this.defaultCollection);
      return cloudSolrClient;
    }
  }

  public void destroy() throws IOException{
    if(this.solrClient != null) {
      this.solrClient.close();
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

  public void add(List<Map<String, Object>> fieldMaps) throws StageException {
    List<SolrInputDocument> documents = new ArrayList();
    for(Map<String, Object> fieldMap : fieldMaps) {
      SolrInputDocument document = createDocument(fieldMap);
      documents.add(document);
    }

    try {
      this.solrClient.add(documents);
    } catch (SolrServerException | IOException | HttpSolrClient.RemoteSolrException ex) {
      throw new StageException(Errors.SOLR_04, ex.toString(), ex);
    }
  }

  private SolrInputDocument createDocument(Map<String, Object> fieldMap) {
    SolrInputDocument document = new SolrInputDocument();
    for (String key : fieldMap.keySet()) {
      document.addField(key, fieldMap.get(key));
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
