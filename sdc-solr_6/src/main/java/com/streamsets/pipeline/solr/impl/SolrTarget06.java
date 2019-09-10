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
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaRepresentation;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
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
  private final static String VERSION = "6.1.0";
  private SolrClient solrClient;

  private final String solrURI;
  private final String zookeeperConnect;
  private final String defaultCollection;
  private final String instanceType;
  private final boolean kerberosAuth;
  private final boolean skipValidation;
  private final boolean ignoreOptionalFields;
  private final boolean waitFlush;
  private final boolean waitSearcher;
  private final boolean softCommit;
  private final int connectionTimeout;
  private final int socketTimeout;
  private List<String> requiredFieldNamesMap;
  private List<String> optionalFieldNamesMap;

  public SolrTarget06(
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
    SchemaRequest schemaRequest = new SchemaRequest();
    SchemaResponse schemaResponse = schemaRequest.process(solrClient);
    SchemaRepresentation schemaRepresentation = schemaResponse.getSchemaRepresentation();
    List<Map<String, Object>> fields = schemaRepresentation.getFields();
    for (Map<String, Object> field : fields) {
      if (field.containsKey(REQUIRED) && field.get(REQUIRED).equals(true)) {
        requiredFieldNamesMap.add(field.get(NAME).toString());
      }
    }
  }

  public List<String> getOptionalFieldNamesMap() {
    return optionalFieldNamesMap;
  }

  private void getOptionalFieldNames() throws SolrServerException, IOException {
    SchemaRequest schemaRequest = new SchemaRequest();
    SchemaResponse schemaResponse = schemaRequest.process(solrClient);
    SchemaRepresentation schemaRepresentation = schemaResponse.getSchemaRepresentation();
    List<Map<String, Object>> fields = schemaRepresentation.getFields();
    for (Map<String, Object> field : fields) {
      if (!field.containsKey(REQUIRED) || field.get(REQUIRED).equals(false)) {
        optionalFieldNamesMap.add(field.get(NAME).toString());
      }
    }
  }

  private SolrClient getSolrClient() throws MalformedURLException {
    if(kerberosAuth) {
      // set kerberos before create SolrClient
      addSecurityProperties();
    }

    if (SolrInstanceAPIType.SINGLE_NODE.toString().equals(this.instanceType)) {
      HttpSolrClient solrClient = new HttpSolrClient.Builder(this.solrURI).build();
      solrClient.setConnectionTimeout(this.connectionTimeout);
      solrClient.setSoTimeout(this.socketTimeout);
      return solrClient;
    } else {
      CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder().withZkHost(this.zookeeperConnect).build();
      cloudSolrClient.setDefaultCollection(this.defaultCollection);
      return cloudSolrClient;
    }
  }

  private void addSecurityProperties() {
    HttpClientUtil.setConfigurer(new SdcKrb5HttpClientConfigurer());
  }

  public void destroy() throws IOException{
    if(this.solrClient != null) {
      this.solrClient.close();
    }
  }

  @SuppressWarnings("unchecked")
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
    } catch (SolrServerException | IOException | HttpSolrClient.RemoteSolrException ex) {
      throw new StageException(Errors.SOLR_04, ex.toString(), ex);
    }
  }

  private SolrInputDocument createDocument(Map<String, Object> fieldMap) {
    SolrInputDocument document = new SolrInputDocument();
    for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
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
