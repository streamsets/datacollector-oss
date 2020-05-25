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
package com.streamsets.pipeline.lib.couchbase;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.BucketDoesNotExistException;
import com.couchbase.client.java.error.InvalidPasswordException;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.destination.couchbase.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

/**
 * CouchbaseConnector provides a singleton per pipeline for all connection objects to a Couchbase bucket
 */
public class CouchbaseConnector {
  private CouchbaseEnvironment env;
  private Cluster cluster;
  private AsyncBucket bucket;
  private volatile boolean isClosed;

  private static final String INSTANCE = "couchbase_client";

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseConnector.class);

  /**
   * Instantiates Couchbase connection objects and returns any encountered issues
   *
   * @param config the stage configuration
   * @param issues the list of configuration issues
   * @param context the context for the stage
   */
  private CouchbaseConnector(BaseCouchbaseConfig config, List<Stage.ConfigIssue> issues, Stage.Context context) {

    DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder();

    if(config.couchbase.kvTimeout > 0) {
      LOG.debug("Setting SDK Environment parameter kvTimeout to {}", config.couchbase.kvTimeout);
      builder.kvTimeout(config.couchbase.kvTimeout);
    }

    if(config.couchbase.connectTimeout > 0) {
      LOG.debug("Setting SDK Environment parameter connectTimeout to {}", config.couchbase.connectTimeout);
      builder.connectTimeout(config.couchbase.connectTimeout);
    }

    if(config.couchbase.disconnectTimeout > 0) {
      LOG.debug("Setting SDK Environment parameter disconnectTimeout to {}", config.couchbase.disconnectTimeout);
      builder.disconnectTimeout(config.couchbase.disconnectTimeout);
    }

    if(config.couchbase.tls.tlsEnabled) {
      LOG.debug("Enabling TLS");
      builder.sslEnabled(true);

      if (config.couchbase.tls.useRemoteKeyStore) {
        LOG.debug("Using remote keystore");
      } else {
        LOG.debug("Using keystore: {}", config.couchbase.tls.keyStoreFilePath);
      }
      builder.sslKeystore(config.couchbase.tls.getKeyStore());

      if (config.couchbase.tls.useRemoteTrustStore) {
        LOG.debug("Using remote truststore");
      } else {
        LOG.debug("Using truststore: {}", config.couchbase.tls.keyStoreFilePath);
      }
      builder.sslTruststore(config.couchbase.tls.getTrustStore());
    }

    if(config.couchbase.envConfigs != null) {
      for (EnvConfig envConfig : config.couchbase.envConfigs) {
        // This check is to keep an unpopulated config from raising an error
        if (envConfig.name.isEmpty()) {
          continue;
        }

        try {
          Object value;

          switch (envConfig.type) {
            case INT:
              value = Integer.parseInt(envConfig.value);
              break;
            case LONG:
              value = Long.parseLong(envConfig.value);
              break;
            case DOUBLE:
              value = Double.parseDouble(envConfig.value);
              break;
            case BOOLEAN:
              value = Boolean.parseBoolean(envConfig.value);
              break;
            default:
              value = envConfig.value;
              break;
          }
          LOG.debug("Setting custom SDK Environment parameter, name: {}, type: {}, value: {}",
              envConfig.name,
              envConfig.type.getClassName(),
              envConfig.value
          );
          builder.getClass().getMethod(envConfig.name, envConfig.type.getClassName()).invoke(builder, value);
        } catch (NoSuchMethodException e) {
          issues.add(context.createConfigIssue(Groups.COUCHBASE.name(),
              "config.envConfigs",
              Errors.COUCHBASE_05,
              e.toString(),
              e
          ));
        } catch (Exception e) {
          issues.add(context.createConfigIssue(Groups.COUCHBASE.name(),
              "config.envConfigs",
              Errors.COUCHBASE_06,
              e.toString(),
              e
          ));
        }
      }
    }

    LOG.debug("Creating CouchbaseEnvironment");
    env = builder.build();

    // Explicitly starting the RxJava scheduler threads in case the pipeline had been previously stopped.
    // Required to prevent deadlocking the next pipeline start.
    // Schedulers.start();

    LOG.debug("Connecting to cluster with nodes {}", config.couchbase.nodes);
    cluster = CouchbaseCluster.create(env, config.couchbase.nodes);

    if(config.credentials.version == AuthenticationType.USER) {
      try {
        LOG.debug("Using user authentication");
        cluster.authenticate(config.credentials.userName.get(), config.credentials.userPassword.get());
      } catch (Exception e) {
        issues.add(context.createConfigIssue(Groups.CREDENTIALS.name(), "config.userPassword", Errors.COUCHBASE_03, e.toString(), e));
      }
    }

    try {
      if (config.credentials.version == AuthenticationType.BUCKET) {
        try {
          LOG.debug("Using bucket authentication");
          LOG.debug("Opening bucket {}", config.couchbase.bucket);
          bucket = cluster.openBucket(config.couchbase.bucket, config.credentials.bucketPassword.get()).async();
        } catch (InvalidPasswordException e) {
          issues.add(context.createConfigIssue(Groups.CREDENTIALS.name(), "config.bucketPassword", Errors.COUCHBASE_03, e.toString(), e));
        }
      } else {
        try {
          LOG.debug("Opening bucket {}", config.couchbase.bucket);
          bucket = cluster.openBucket(config.couchbase.bucket).async();
        } catch (InvalidPasswordException e) {
          issues.add(context.createConfigIssue(Groups.CREDENTIALS.name(), "config.userPassword", Errors.COUCHBASE_03, e.toString(), e));
        }
      }
    } catch (BucketDoesNotExistException e) {
      issues.add(context.createConfigIssue(Groups.COUCHBASE.name(), "config.bucket", Errors.COUCHBASE_04, e.toString(), e));
    } catch (Exception e) {
      issues.add(context.createConfigIssue(Groups.COUCHBASE.name(), "config.nodes", Errors.COUCHBASE_02, e.toString(), e));
    }
  }

  /**
   * Maintains a singleton instance of the CouchbaseConnector object per pipeline
   *
   * @param config the stage configuration
   * @param issues the list of configuration issues
   * @param context the context for the stage
   * @return a singleton instance for Couchbase connections
   */
  public static synchronized CouchbaseConnector getInstance(BaseCouchbaseConfig config, List<Stage.ConfigIssue> issues, Stage.Context context) {

    Map<String, Object> runnerSharedMap = context.getStageRunnerSharedMap();

    if(runnerSharedMap.containsKey(INSTANCE)) {
      LOG.debug("Using existing instance of CouchbaseConnector");
    } else {
      LOG.debug("CouchbaseConnector not yet instantiated. Creating new instance");
      validateConfig(config, issues, context);

      if(issues.isEmpty()) {
        runnerSharedMap.put(INSTANCE, new CouchbaseConnector(config, issues, context));
      }
    }

    return (CouchbaseConnector) runnerSharedMap.get(INSTANCE);
  }

  /**
   * Returns a reference to the Couchbase bucket
   * @return the instantiated bucket
   */
  public AsyncBucket bucket() {
    return this.bucket;
  }

  /**
   * Returns the computation scheduler for this instance of the CouchbaseEnvironment
   * @return the current computation scheduler
   */
  public rx.Scheduler getScheduler() {
    return this.env.scheduler();
  }

  /**
   * Disconnects from Couchbase and releases all resources
   */
  public synchronized void close() {

    if(!isClosed) {
      if(bucket != null) {
        LOG.debug("Closing Couchbase bucket");
        bucket.close();
      }

      if(cluster != null) {
        LOG.debug("Disconnecting Couchbase cluster");
        cluster.disconnect();
      }

      if(env != null) {
        LOG.debug("Shutting down Couchbase environment");
        env.shutdown();
      }

      // Explicitly shutdown the RxJava scheduler threads. Not doing so will leak threads when a pipeline stops.
      // Note: this disallows restarting scheduler threads without also explicitly calling Schedulers.start()
      // LOG.debug("Stopping RxJava schedulers");
      // Schedulers.shutdown();
      isClosed = true;
    }
  }

  /**
   * Validates connection configurations that don't require runtime exception handling
   * @param config the stage configuration
   * @param issues the list of configuration issues
   * @param context the context for the stage
   */
  private static void validateConfig(BaseCouchbaseConfig config, List<Stage.ConfigIssue> issues, Stage.Context context){
    if(config.couchbase.nodes == null) {
      issues.add(context.createConfigIssue(Groups.COUCHBASE.name(), "config.couchbase.nodes", Errors.COUCHBASE_29));
    }

    if(config.couchbase.kvTimeout < 0) {
      issues.add(context.createConfigIssue(Groups.COUCHBASE.name(), "config.couchbase.kvTimeout", Errors.COUCHBASE_30));
    }

    if(config.couchbase.connectTimeout < 0) {
      issues.add(context.createConfigIssue(Groups.COUCHBASE.name(), "config.couchbase.connectTimeout", Errors.COUCHBASE_31));
    }

    if(config.couchbase.disconnectTimeout < 0) {
      issues.add(context.createConfigIssue(Groups.COUCHBASE.name(), "config.couchbase.disconnectTimeout", Errors.COUCHBASE_32));
    }

    if(config.couchbase.tls.tlsEnabled) {
      config.couchbase.tls.init(context, Groups.COUCHBASE.name(), "config.couchbase.tls.", issues);
    }

    if(config.credentials.version == null) {
      issues.add(context.createConfigIssue(Groups.CREDENTIALS.name(), "config.credentials.version", Errors.COUCHBASE_33));
    }

    if(config.credentials.version == AuthenticationType.USER) {
      if(config.credentials.userName == null) {
        issues.add(context.createConfigIssue(Groups.CREDENTIALS.name(), "config.credentials.userName", Errors.COUCHBASE_34));
      }

      if(config.credentials.userPassword == null) {
        issues.add(context.createConfigIssue(Groups.CREDENTIALS.name(), "config.credentials.userPassword", Errors.COUCHBASE_35));
      }
    }
  }
}