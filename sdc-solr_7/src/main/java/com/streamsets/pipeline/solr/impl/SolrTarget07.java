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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class is extremely similar to SolrTarget06, but as we want to avoid dependencies between different version of
 * the implementation, we choose to duplicate some code, but keep them independent.
 */

public class SolrTarget07 implements SdcSolrTarget {
  private final static String VERSION = "7.0.0";
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

  public SolrTarget07(
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

  @Override
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

  @Override
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
        requiredFieldNamesMap.add(field.get(NAME).toString());
      }
    }
  }

  private SolrClient getSolrClient() {
    if (kerberosAuth) {
      // set kerberos before create SolrClient
      addSecurityProperties();
    }

    if (SolrInstanceAPIType.SINGLE_NODE.toString().equals(this.instanceType)) {
      return new HttpSolrClient.Builder(this.solrURI).withConnectionTimeout(this.connectionTimeout).
              withSocketTimeout(this.socketTimeout).build();
    } else {
      CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder().withZkHost(this.zookeeperConnect).build();
      cloudSolrClient.setDefaultCollection(this.defaultCollection);
      return cloudSolrClient;
    }
  }

  private void addSecurityProperties() {
    HttpClientUtil.setHttpClientBuilder(SdcSolrHttpClientBuilder.create());
  }

  @Override
  public void destroy() throws IOException {
    if (this.solrClient != null) {
      this.solrClient.close();
    }
  }

  @Override
  public void add(Map<String, Object> fieldMap) throws StageException {
    SolrInputDocument document = createDocument(fieldMap);
    try {
      this.solrClient.add(document);
    } catch (SolrServerException | IOException ex) {
      throw new StageException(Errors.SOLR_04, ex.toString(), ex);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void add(List<Map<String, Object>> fieldMaps) throws StageException {
    List<SolrInputDocument> documents = new ArrayList();
    for (Map<String, Object> fieldMap : fieldMaps) {
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

  @Override
  public void commit() throws StageException {
    try {
      this.solrClient.commit(waitFlush, waitSearcher, softCommit);
    } catch (SolrServerException | IOException ex) {
      throw new StageException(Errors.SOLR_05, ex.toString(), ex);
    }
  }

  @Override
  public String getVersion() {
    return VERSION;
  }
}
