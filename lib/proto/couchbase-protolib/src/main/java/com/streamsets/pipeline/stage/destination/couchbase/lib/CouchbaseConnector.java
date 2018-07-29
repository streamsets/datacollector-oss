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
package com.streamsets.pipeline.stage.destination.couchbase.lib;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.streamsets.pipeline.api.StageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CouchbaseConnector is singleton class that manages all connection requirements to a
 * Couchbase bucket.
 *
 * CouchbaseConnector handles all CRUD operations for the Couchbase Destination.
 */
public class CouchbaseConnector {
  private Cluster cluster;
  private Bucket bucket;

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseConnector.class);

  /**
   * CouchbaseConnector
   *
   * Constructor methods which takes standard connection parameters for a Couchbase Cluster (Version 4)
   *
   *
   * @param  url URL Endpoint to the Couchbase Cluster.
   * @param  bucket Couchbase Bucket Name
   * @param  bucketPassword Couchbase Bucket password
   */
  private CouchbaseConnector(String url, String bucket, String bucketPassword) throws StageException {
    connectToCouchbaseServer(url, bucket, bucketPassword);
  }

  /**
   * CouchbaseConnector
   *
   * Constructor methods which takes standard connection parameters for a Couchbase Cluster (Version 5)
   *
   * @param  urlString URL Endpoint to the Couchbase Cluster.
   * @param  bucketString Couchbase Bucket Name
   * @param  userName Couchbase UserName
   * @param  userPassword Couchbase Couchbase User password
   */
  private CouchbaseConnector(String urlString, String bucketString, String userName, String userPassword) throws StageException {
    connectToCouchbaseServer(urlString, bucketString, userName, userPassword);
  }

  /**
   * connectToCouchbaseServer
   *
   * Connection method to version 4
   *
   *
   * @param  url URL Endpoint to the Couchbase Cluster.
   * @param  bucketName Couchbase Bucket Name
   * @param  bucketPassword Couchbase Bucket password
   */
  private void connectToCouchbaseServer(String url, String bucketName, String bucketPassword) throws StageException {
    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
      .build();

    // Init Couchbase
    cluster  = CouchbaseCluster.create(env, url);

    try {
      bucket = cluster.openBucket(bucketName, bucketPassword);
    } catch (Exception e) {
      throw new StageException(Errors.ERROR_03, e.toString(), e);
    }
  }

  /**
   * connectToCouchbaseServer
   * <p>
   * Connection method to version 5
   * <p>
   *
   * @param  url URL Endpoint to the Couchbase Cluster.
   * @param  bucketName Couchbase Bucket Name
   * @param  userName Couchbase UserName
   * @param  userPassword Couchbase Couchbase User password
   */
  private void connectToCouchbaseServer(String url, String bucketName, String userName, String userPassword) throws StageException {
    // Init Couchbase
    cluster  = CouchbaseCluster.create(url);
    cluster.authenticate(userName, userPassword);

    try {
      bucket = cluster.openBucket(bucketName);
    } catch (Exception e) {
      throw new StageException(Errors.ERROR_03, e.toString(), e);
    }
  }

  /**
   * connectToCouchbaseServer
   *
   * Get an instance to connect to Couchbase Version 4
   *
   * @param  url URL Endpoint to the Couchbase Cluster.
   * @param  bucket Couchbase Bucket Name
   * @param  password Couchbase Bucket password
   */
  public static CouchbaseConnector getInstance(String url, String bucket, String password) throws StageException {
    return new CouchbaseConnector(url, bucket, password);
  }

  /**
   * connectToCouchbaseServer
   *
   * Get an instance to connect to Couchbase Version 5
   *
   * @param  url URL Endpoint to the Couchbase Cluster.
   * @param  bucket Couchbase Bucket Name
   * @param  userName Couchbase Username
   * @param  userPassword Couchbase User Password
   */
  public static CouchbaseConnector getInstance(String url, String bucket, String userName, String userPassword) throws StageException {
    return new CouchbaseConnector(url, bucket, userName, userPassword);
  }

  /**
   * writeToBucket
   *
   * writeToBucket synchronously upserts JSON documents into Couchbase
   *
   * @param  documentKey Unique key of the JSON Document.
   * @param  jsonObject The JSON Object i.e document body.
   */
  public void writeToBucket(String documentKey, JsonObject jsonObject) {
    JsonDocument document = JsonDocument.create(documentKey, jsonObject);
    bucket.upsert(document);
  }

  public void close() {
    if(bucket != null) {
      bucket.close();
    }
    if(bucket != null) {
      cluster.disconnect();
    }
  }
}
