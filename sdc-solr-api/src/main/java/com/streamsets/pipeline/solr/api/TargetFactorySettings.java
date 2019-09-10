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

public class TargetFactorySettings {
  private String instanceType;
  private String solrURI;
  private String zookeeperConnect;
  private String defaultCollection;
  private boolean kerberosAuth;
  private boolean skipValidation;
  private final boolean waitFlush;
  private final boolean waitSearcher;
  private final boolean softCommit;
  private boolean ignoreOptionalFields;
  private final int connectionTimeout;
  private final int socketTimeout;

  public TargetFactorySettings (
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
    this.connectionTimeout = connectionTimeout;
    this.socketTimeout = socketTimeout;
  }

  public String getInstanceType() {
    return instanceType;
  }

  public String getSolrURI() {
    return solrURI;
  }

  public String getZookeeperConnect() {
    return zookeeperConnect;
  }

  public String getDefaultCollection() {
    return defaultCollection;
  }

  public boolean getKerberosAuth() {
    return kerberosAuth;
  }

  public boolean getSkipValidation() {
    return skipValidation;
  }

  public boolean isWaitFlush() {
    return waitFlush;
  }

  public boolean isWaitSearcher() {
    return waitSearcher;
  }

  public boolean isSoftCommit() {
    return softCommit;
  }

  public boolean getIgnoreOptionalFields() {
    return ignoreOptionalFields;
  }

  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  public int getSocketTimeout() {
    return socketTimeout;
  }

}
