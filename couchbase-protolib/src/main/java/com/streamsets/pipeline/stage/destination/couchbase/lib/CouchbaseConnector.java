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

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.util.retry.RetryBuilder;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.JsonStringDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.streamsets.pipeline.stage.destination.couchbase.CouchbaseConnectorTarget;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;


/**
 * CouchbaseConnector is singleton class that manages all connection requirements to a
 * Couchbase bucket. 
 * 
 * CouchbaseConnector handles all CRUD operations for the Couchbase Destination.
 */
public class CouchbaseConnector {
    private Cluster cluster;
    private Bucket bucket;
    
   
    
    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseConnectorTarget.class);
    
    
    /**
     * CouchbaseConnector                           
     * <p>
     * Constructor methods which takes standard connection parameters for a Couchbase Cluster (Version 4)
     * <p>
     *
     * @param  url URL Endpoint to the Couchbase Cluster.          
     * @param  bucket Couchbase Bucket Name
     * @param  bucketPassword Couchbase Bucket password
     */
    private CouchbaseConnector(String url, String bucket, String bucketPassword) {
        connectToCouchbaseServer(url, bucket, bucketPassword);
       
    }
    
    /**
     * CouchbaseConnector                           
     * <p>
     * Constructor methods which takes standard connection parameters for a Couchbase Cluster (Version 5)
     * <p>
     *
     * @param  urlString URL Endpoint to the Couchbase Cluster.
     * @param  bucketString Couchbase Bucket Name
     * @param  userName Couchbase UserName
     * @param  userPassword Couchbase Couchbase User password
     */
    private CouchbaseConnector(String urlString, String bucketString, String userName, String userPassword) {
        connectToCouchbaseServer(urlString, bucketString, userName, userPassword);
    }
    
    
    /**
     * connectToCouchbaseServer                           
     * <p>
     * Connection method to version 4
     * <p>
     *
     * @param  url URL Endpoint to the Couchbase Cluster.          
     * @param  bucketName Couchbase Bucket Name
     * @param  bucketPassword Couchbase Bucket password
     */
    private void connectToCouchbaseServer(String url, String bucketName, String bucketPassword) {
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .build();
        
        //Init Couchbase
        cluster  = CouchbaseCluster.create(env, url);
        
        try {
            bucket = cluster.openBucket(bucketName, bucketPassword);
            LOG.info("Connected to Couchbase version 4");
        } catch (Exception e) {
            LOG.info("Exception" + e + " occurred while connecting to Couchbase version 4");
            e.printStackTrace();
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
    private void connectToCouchbaseServer(String url, String bucketName, String userName, String userPassword) {
      //  CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
        //        .build();
        
        //Init Couchbase
        cluster  = CouchbaseCluster.create(url);
        cluster.authenticate(userName, userPassword);
        try {
            bucket = cluster.openBucket(bucketName);
            LOG.info("Connected to Couchbase version 5");
        } catch (Exception e) {
            LOG.info("Exception" + e + " occurred while connecting to Couchbase version 5");
            e.printStackTrace();
        }
       
        
    }

    /**
     * connectToCouchbaseServer                           
     * <p>
     * Get an instance to connect to Couchbase Version 4
     * <p>
     *
     * @param  url URL Endpoint to the Couchbase Cluster.          
     * @param  bucket Couchbase Bucket Name
     * @param  password Couchbase Bucket password
     */
    public static CouchbaseConnector getInstance(String url, String bucket, String password) {
        
        return new CouchbaseConnector(url, bucket, password);
    }
    
        /**
     * connectToCouchbaseServer                           
     * <p>
     * Get an instance to connect to Couchbase Version 5
     * <p>
     *
     * @param  url URL Endpoint to the Couchbase Cluster.          
     * @param  bucket Couchbase Bucket Name
     * @param  userName Couchbase Username
     * @param  userPassword Couchbase User Password
     */
    public static CouchbaseConnector getInstance(String url, String bucket, String userName, String userPassword) {
        
        return new CouchbaseConnector(url, bucket, userName, userPassword);
    }
        
     /**
     * writeToBucket 
     * <p>
     * writeToBucket synchronously upserts JSON documents into Couchbase
     * <p>
     *
     * @param  documentKey Unique key of the JSON Document.          
     * @param  jsonObject The JSON Object i.e document body.
     */    
    
    public void writeToBucket(String documentKey, JsonObject jsonObject) {
        
        JsonDocument document = JsonDocument.create(documentKey, jsonObject);
        bucket.upsert(document);
    }
    
    public void writeToBucket(String jsonObject) {
        
        JsonStringDocument doc = JsonStringDocument.create(jsonObject);
        LOG.info("Upserting JSON Document - " + jsonObject);
        bucket.upsert(doc);
    }
        
    public void writeToBucket(List<JsonDocument> docs) {
        
        for (JsonDocument document : docs) {
            bucket.upsert(document);
        }
        
    }
    
     /**
     * bulkSet 
     * <p>
     * bulkSet asynchronously bulk upserts JSON documents into Couchbase.
     * <p>
     *
     * @param  docs A List of JsonDocuments.          
     */ 
    public void bulkSet(List<JsonDocument> docs) {
        final AsyncBucket asyncBucket = bucket.async();
        
        Observable
                .from(docs)
                .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
                    @SuppressWarnings("unchecked")
                    @Override
                    public Observable<JsonDocument> call(JsonDocument document) {
                        return asyncBucket.upsert(document)
                                .retryWhen(RetryBuilder
                                    .anyOf(BackpressureException.class)
                                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, 10))
                                    .max(10)
                                    .build());
                    }
                })
                .last()
                .toBlocking()
                .single();           
    }
    
    public N1qlQueryResult queryBucket(String documentType) {
    
        // Perform a N1QL Query
        N1qlQueryResult result = bucket.query(N1qlQuery.simple("SELECT * FROM " + documentType));
    
        return result;
    }
    
    public boolean closeConnection()
    {
        return bucket.close();
    }
             
}
