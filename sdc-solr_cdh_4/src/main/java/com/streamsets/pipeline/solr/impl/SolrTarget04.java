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
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SolrTarget04 implements SdcSolrTarget {
  private final static Logger LOG = LoggerFactory.getLogger(SolrTarget04.class);
  private static final String VERSION = "4.4.0";
  private static final String SCHEMA_PATH = "/schema";
  private static final String NAME = "name";

  private SolrServer solrClient;

  private final String solrURI;
  private final String zookeeperConnect;
  private final String defaultCollection;
  private final String instanceType;
  private final boolean kerberosAuth;
  private final boolean skipValidation;
  private final boolean waitFlush;
  private final boolean waitSearcher;
  private final boolean softCommit;
  private final boolean ignoreOptionalFields;
  private final int connectionTimeout;
  private final int socketTimeout;
  private List<String> requiredFieldNamesMap;
  private List<String> optionalFieldNamesMap;

  public SolrTarget04(
      String instanceType,
      String solrURI,
      String zookeeperConnect,
      String defaultCollection,
      boolean kerberosAuth,
      boolean skipValidation,
      boolean waitFlush,
      boolean waitSearcher,
      boolean softCommit,
      boolean ignoreOptionalFields,
      int connectionTimeout,
      int socketTimeout
  ) {
    this.instanceType = instanceType;
    this.solrURI = solrURI;
    this.zookeeperConnect = zookeeperConnect;
    this.defaultCollection = defaultCollection;
    this.kerberosAuth = kerberosAuth;
    this.skipValidation = skipValidation;
    this.waitFlush = waitFlush;
    this.waitSearcher = waitSearcher;
    this.softCommit = softCommit;
    this.ignoreOptionalFields = ignoreOptionalFields;
    this.requiredFieldNamesMap = new ArrayList<>();
    this.optionalFieldNamesMap = new ArrayList<>();
    this.connectionTimeout = connectionTimeout;
    this.socketTimeout = socketTimeout;
  }

  public void init() throws Exception {
    solrClient = getSolrClient();
    if (!skipValidation) {
      solrClient.ping();
    }
    getRequiredFieldNames();
    if (!ignoreOptionalFields) {
      getOptionalFieldNames();
    }
  }

  public List<String> getRequiredFieldNamesMap() {
    return requiredFieldNamesMap;
  }

  private void getRequiredFieldNames() throws SolrServerException, IOException {
    QueryRequest request = new QueryRequest();
    request.setPath(SCHEMA_PATH);
    NamedList queryResponse = solrClient.request(request);

    SimpleOrderedMap simpleOrderedMap = (SimpleOrderedMap) queryResponse.get("schema");
    ArrayList<SimpleOrderedMap> fields = (ArrayList<SimpleOrderedMap>) simpleOrderedMap.get("fields");

    for (SimpleOrderedMap field : fields) {
      if (field.get(REQUIRED) != null && field.get(REQUIRED).equals(true)) {
        requiredFieldNamesMap.add(field.get(NAME).toString());
      }
    }
  }

  private void getOptionalFieldNames() throws SolrServerException, IOException {
    QueryRequest request = new QueryRequest();
    request.setPath(SCHEMA_PATH);
    NamedList queryResponse = solrClient.request(request);

    SimpleOrderedMap simpleOrderedMap = (SimpleOrderedMap) queryResponse.get("schema");
    ArrayList<SimpleOrderedMap> fields = (ArrayList<SimpleOrderedMap>) simpleOrderedMap.get("fields");

    for (SimpleOrderedMap field : fields) {
      if (field.get(REQUIRED) == null || field.get(REQUIRED).equals(false)) {
        optionalFieldNamesMap.add(field.get(NAME).toString());
      }
    }
  }

  public List<String> getOptionalFieldNamesMap() {
    return optionalFieldNamesMap;
  }

  private SolrServer getSolrClient() throws MalformedURLException {
    if(kerberosAuth) {
      // set kerberos before create SolrClient
      addSecurityProperties();
    }

    if (SolrInstanceAPIType.SINGLE_NODE.toString().equals(this.instanceType)) {
      HttpSolrServer httpSolrServer = new HttpSolrServer(this.solrURI);
      httpSolrServer.setConnectionTimeout(connectionTimeout);
      httpSolrServer.setSoTimeout(socketTimeout);
      return httpSolrServer;
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
      this.solrClient.commit(waitFlush, waitSearcher, softCommit);
    } catch (SolrServerException | IOException ex) {
      throw new StageException(Errors.SOLR_05, ex.toString(), ex);
    }
  }

  public String getVersion() {
    return this.VERSION;
  }

}
