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
package com.streamsets.pipeline.stage.common.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MongoDBConfig {

  public static final String CONFIG_PREFIX = "configBean.";
  public static final String MONGO_CONFIG_PREFIX = CONFIG_PREFIX + "mongoConfig.";

  private MongoClient mongoClient;
  private MongoDatabase mongoDatabase;
  private MongoCollection mongoCollection;

  // Basic configs

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      label = "Connection String",
      description = "Use format mongodb://host1[:port1][,host2[:port2],...[,hostN[:portN]]]" +
          "[/[database][?options]]",
      required = true,
      group = "MONGODB",
      displayPosition = 10
  )
  public String connectionString;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Enable Single Mode",
      description = "Connects to the first MongoDB server in the connection string",
      displayPosition = 11,
      group = "MONGODB"
  )
  public boolean isSingleMode;

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      label = "Database",
      required = true,
      group = "MONGODB",
      displayPosition = 20
  )
  public String database;

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      label = "Collection",
      required = true,
      group = "MONGODB",
      displayPosition = 30
  )
  public String collection;

  @ConfigDef(
      type = ConfigDef.Type.MODEL,
      label = "Authentication Type",
      defaultValue = "NONE",
      required = true,
      group = "CREDENTIALS",
      displayPosition = 40
  )
  @ValueChooserModel(AuthenticationTypeChooserValues.class)
  public AuthenticationType authenticationType;

  @ConfigDef(
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      required = true,
      dependsOn = "authenticationType",
      triggeredByValue = {"USER_PASS","LDAP"},
      group = "CREDENTIALS",
      displayPosition = 50
  )
  public CredentialValue username;

  @ConfigDef(
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      required = true,
      dependsOn = "authenticationType",
      triggeredByValue = {"USER_PASS","LDAP"},
      group = "CREDENTIALS",
      displayPosition = 60
  )
  public CredentialValue password;

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      label = "Authentication Source",
      description = "For delegated authentication, specify alternate database name. Leave blank for normal authentication",
      required = false,
      dependsOn = "authenticationType",
      triggeredByValue = "USER_PASS",
      group = "CREDENTIALS",
      displayPosition = 65
  )
  public String authSource = "";

  // Advanced configs

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Connections Per Host",
      description = "Sets the maximum number of connections per host",
      defaultValue = "100",
      required = false,
      group = "ADVANCED",
      displayPosition = 10
  )
  public int connectionsPerHost = 100;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Min Connections Per Host",
      description = "Sets the minimum number of connections per host",
      defaultValue = "0",
      required = false,
      group = "ADVANCED",
      displayPosition = 20
  )
  public int minConnectionsPerHost = 0;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Connect Timeout",
      description = "Sets the connection timeout",
      defaultValue = "10000",
      required = false,
      group = "ADVANCED",
      displayPosition = 30
  )
  public int connectTimeout = 10000;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Max Connection Idle Time",
      description = "Sets the maximum idle time for a pooled connection",
      defaultValue = "0",
      required = false,
      group = "ADVANCED",
      displayPosition = 40
  )
  public int maxConnectionIdleTime = 0;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Max Connection Life Time",
      description = "Sets the maximum life time for a pooled connection",
      defaultValue = "0",
      required = false,
      group = "ADVANCED",
      displayPosition = 50
  )
  public int maxConnectionLifeTime = 0;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Max Wait Time",
      description = "Sets the maximum time that a thread will block waiting for a connection",
      defaultValue = "120000",
      required = false,
      group = "ADVANCED",
      displayPosition = 60
  )
  public int maxWaitTime = 120000;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Server Selection Timeout",
      description = "Sets the server selection timeout in milliseconds, " +
          "which defines how long the driver will wait for server selection to succeed before throwing an exception",
      defaultValue = "30000",
      required = false,
      group = "ADVANCED",
      displayPosition = 70
  )
  public int serverSelectionTimeout = 30000;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Threads Allowed To Block For Connection Multiplier",
      defaultValue = "5",
      required = false,
      group = "ADVANCED",
      displayPosition = 80
  )
  public int threadsAllowedToBlockForConnectionMultiplier = 5;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Heartbeat Frequency",
      description = "Sets the heartbeat frequency",
      defaultValue = "10000",
      required = false,
      group = "ADVANCED",
      displayPosition = 90
  )
  public int heartbeatFrequency = 10000;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Min Heartbeat Frequency",
      description = "Sets the minimum heartbeat frequency",
      defaultValue = "500",
      required = false,
      group = "ADVANCED",
      displayPosition = 100
  )
  public int minHeartbeatFrequency = 500;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Heartbeat Connect Timeout",
      description = "Sets the connect timeout for connections used for the cluster heartbeat",
      defaultValue = "20000",
      required = false,
      group = "ADVANCED",
      displayPosition = 110
  )
  public int heartbeatConnectTimeout = 20000;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Heartbeat Socket Timeout",
      description = "Sets the socket timeout for connections used for the cluster heartbeat",
      defaultValue = "20000",
      required = false,
      group = "ADVANCED",
      displayPosition = 120
  )
  public int heartbeatSocketTimeout = 20000;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Local Threshold",
      description = "Sets the local threshold",
      defaultValue = "15",
      required = false,
      group = "ADVANCED",
      displayPosition = 130
  )
  public int localThreshold = 15;

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      label = "Required Replica Set Name",
      description = "Sets the required replica set name for the cluster",
      defaultValue = "",
      required = false,
      group = "ADVANCED",
      displayPosition = 140
  )
  public String requiredReplicaSetName = "";

  @ConfigDef(
      type = ConfigDef.Type.BOOLEAN,
      label = "Cursor Finalizer Enabled",
      description = "Sets whether cursor finalizers are enabled",
      defaultValue = "true",
      required = false,
      group = "ADVANCED",
      displayPosition = 150
  )
  public boolean cursorFinalizerEnabled = true;

  @ConfigDef(
      type = ConfigDef.Type.BOOLEAN,
      label = "Socket Keep Alive",
      description = "Sets whether socket keep alive is enabled",
      defaultValue = "false",
      required = false,
      group = "ADVANCED",
      displayPosition = 160
  )
  public boolean socketKeepAlive = false;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Socket Timeout",
      description = "Sets the socket timeout",
      defaultValue = "0",
      required = false,
      group = "ADVANCED",
      displayPosition = 170
  )
  public int socketTimeout = 0;

  @ConfigDef(
      type = ConfigDef.Type.BOOLEAN,
      label = "SSL Enabled",
      description = "Sets whether to use SSL",
      defaultValue = "false",
      required = false,
      group = "ADVANCED",
      displayPosition = 180
  )
  public boolean sslEnabled = false;

  @ConfigDef(
      type = ConfigDef.Type.BOOLEAN,
      label = "SSL Invalid Host Name Allowed",
      description = "Define whether invalid host names should be allowed",
      defaultValue = "false",
      required = false,
      group = "ADVANCED",
      displayPosition = 190
  )
  public boolean sslInvalidHostNameAllowed = false;

  public void init(
      Stage.Context context,
      List<Stage.ConfigIssue> issues,
      ReadPreference readPreference,
      WriteConcern writeConcern
  ) {
    mongoClient = createClient(context, issues, readPreference, writeConcern);
    if (!issues.isEmpty()) {
      return;
    }

    mongoDatabase = createMongoDatabase(context, issues, readPreference, writeConcern);
    if (!issues.isEmpty()) {
      return;
    }

    mongoCollection = createMongoCollection(context, issues, readPreference, writeConcern);
  }

  public MongoClient getMongoClient() {
    return mongoClient;
  }

  public MongoDatabase getMongoDatabase() {
    return mongoDatabase;
  }

  public MongoCollection getMongoCollection() {
    return mongoCollection;
  }

  private MongoClient createClient(
      Stage.Context context,
      List<Stage.ConfigIssue> issues,
      ReadPreference readPreference,
      WriteConcern writeConcern
  ) {
    MongoClientOptions.Builder optionBuilder = new MongoClientOptions.Builder()
        .connectionsPerHost(connectionsPerHost)
        .connectTimeout(connectTimeout)
        .cursorFinalizerEnabled(cursorFinalizerEnabled)
        .heartbeatConnectTimeout(heartbeatConnectTimeout)
        .heartbeatFrequency(heartbeatFrequency)
        .heartbeatSocketTimeout(heartbeatSocketTimeout)
        .localThreshold(localThreshold)
        .maxConnectionIdleTime(maxConnectionIdleTime)
        .maxConnectionLifeTime(maxConnectionLifeTime)
        .maxWaitTime(maxWaitTime)
        .minConnectionsPerHost(minConnectionsPerHost)
        .minHeartbeatFrequency(minHeartbeatFrequency)
        .serverSelectionTimeout(serverSelectionTimeout)
        .socketKeepAlive(socketKeepAlive)
        .socketTimeout(socketTimeout)
        .sslEnabled(sslEnabled)
        .sslInvalidHostNameAllowed(sslInvalidHostNameAllowed)
        .threadsAllowedToBlockForConnectionMultiplier(threadsAllowedToBlockForConnectionMultiplier);

    // the default value of requiredReplicaSetName is null, so it should be set only if a non-empty string is provided
    if (!requiredReplicaSetName.isEmpty()) {
      optionBuilder = optionBuilder.requiredReplicaSetName(requiredReplicaSetName);
    }
    // read preference is only set by the source
    if (readPreference != null) {
      optionBuilder = optionBuilder.readPreference(readPreference);
    }
    // write concern is only set by the target
    if (writeConcern != null) {
      optionBuilder = optionBuilder.writeConcern(writeConcern);
    }

    MongoClientURI mongoURI;
    List<ServerAddress> servers = new ArrayList<>();
    try {
      mongoURI = new MongoClientURI(connectionString, optionBuilder);
    } catch (IllegalArgumentException e) {
      issues.add(context.createConfigIssue(
          Groups.MONGODB.name(),
          MONGO_CONFIG_PREFIX + "connectionString",
          Errors.MONGODB_00,
          e.toString()
      ));
      return null;
    }

    validateServerList(context, mongoURI.getHosts(), servers, issues);
    if (!issues.isEmpty()) {
      return null;
    }

    MongoClient mongoClient = null;
    List<MongoCredential> credentials;
    try {
      credentials = createCredentials();
    } catch (StageException ex) {
      issues.add(context.createConfigIssue(
          Groups.MONGODB.name(),
          MONGO_CONFIG_PREFIX + "connectionString",
          Errors.MONGODB_34,
          ex.toString()
      ));
      return null;
    }

    if (credentials.isEmpty()) {
      Optional.ofNullable(mongoURI.getCredentials()).ifPresent(credentials::add);
    }

    try {
      if(isSingleMode) {
        mongoClient = new MongoClient(servers.get(0), credentials, mongoURI.getOptions());
      } else {
        mongoClient = new MongoClient(servers, credentials, mongoURI.getOptions());
      }
    } catch (MongoException e) {
      issues.add(context.createConfigIssue(
          Groups.MONGODB.name(),
          MONGO_CONFIG_PREFIX + "connectionString",
          Errors.MONGODB_01,
          e.toString()
      ));
    }

    return mongoClient;
  }

  private MongoDatabase createMongoDatabase(
      Stage.Context context,
      List<Stage.ConfigIssue> issues,
      ReadPreference readPreference,
      WriteConcern writeConcern
  ) {
    MongoDatabase mongoDatabase = null;
    try {
      if (readPreference != null) {
        mongoDatabase = mongoClient.getDatabase(database).withReadPreference(readPreference);
      } else if (writeConcern != null) {
        mongoDatabase = mongoClient.getDatabase(database).withWriteConcern(writeConcern);
      }
    } catch (MongoClientException e) {
      issues.add(context.createConfigIssue(
          Groups.MONGODB.name(),
          MONGO_CONFIG_PREFIX + "database",
          Errors.MONGODB_02,
          database,
          e.toString()
      ));
    }
    return mongoDatabase;
  }

  private MongoCollection createMongoCollection(
      Stage.Context context,
      List<Stage.ConfigIssue> issues,
      ReadPreference readPreference,
      WriteConcern writeConcern
  ) {
    MongoCollection mongoCollection = null;
    try {
      if (readPreference != null) {
        mongoCollection = mongoDatabase.getCollection(collection).withReadPreference(readPreference);
      } else if (writeConcern != null) {
        mongoCollection = mongoDatabase.getCollection(collection).withWriteConcern(writeConcern);
      }
    } catch (MongoClientException e) {
      issues.add(context.createConfigIssue(
          Groups.MONGODB.name(),
          MONGO_CONFIG_PREFIX + "collection",
          Errors.MONGODB_03,
          collection,
          e.toString()
      ));
    }
    return mongoCollection;
  }

  private List<MongoCredential> createCredentials() throws StageException {
    MongoCredential credential = null;
    List<MongoCredential> credentials = new ArrayList<>(1);
    String authdb = (authSource.isEmpty() ? database : authSource);
    switch (authenticationType) {
      case USER_PASS:
        credential = MongoCredential.createCredential(username.get(), authdb, password.get().toCharArray());
        break;
      case LDAP:
        credential = MongoCredential.createCredential(username.get(), "$external", password.get().toCharArray());
        break;
      case NONE:
      default:
        break;
    }

    if (credential != null) {
      credentials.add(credential);
    }
    return credentials;
  }

  private void validateServerList(
      Stage.Context context,
      List<String> hosts,
      List<ServerAddress> servers,
      List<Stage.ConfigIssue> issues
  ) {
    // Validate each host in the connection string is valid. MongoClient will not tell us
    // if something is wrong when we try to open it.
    for (String host : hosts) {
      String[] hostport = host.split(":");
      if (hostport.length != 2) {
        issues.add(context.createConfigIssue(
            Groups.MONGODB.name(),
            MONGO_CONFIG_PREFIX + "connectionString",
            Errors.MONGODB_07,
            host
        ));
      } else {
        try {
          InetAddress.getByName(hostport[0]);
          servers.add(new ServerAddress(hostport[0], Integer.parseInt(hostport[1])));
        } catch (UnknownHostException e) {
          issues.add(context.createConfigIssue(
              Groups.MONGODB.name(),
              MONGO_CONFIG_PREFIX + "connectionString",
              Errors.MONGODB_09,
              hostport[0]
          ));
        } catch (NumberFormatException e) {
          issues.add(context.createConfigIssue(
              Groups.MONGODB.name(),
              MONGO_CONFIG_PREFIX + "connectionString",
              Errors.MONGODB_08,
              hostport[1]
          ));
        }
      }
    }
  }
}
