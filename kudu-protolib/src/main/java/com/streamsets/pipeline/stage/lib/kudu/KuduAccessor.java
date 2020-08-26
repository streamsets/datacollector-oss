/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.lib.kudu;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.common.kudu.KuduConnection;
import com.streamsets.pipeline.stage.destination.kudu.Groups;
import com.streamsets.pipeline.stage.destination.kudu.KuduConfigBean;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.OperationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Wrapper for a {@linkplain KuduClient} created using the {@linkplain KuduConnection}. Every instance of this
 * class owns only one client, but can creates many sessions using the same client.
 */
public class KuduAccessor {

  private static final Logger LOG = LoggerFactory.getLogger(KuduAccessor.class);

  private final KuduConnection connection;

  private KuduClient client;
  private AsyncKuduClient asyncClient;

  private final ConcurrentLinkedQueue<KuduSession> sessions = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<AsyncKuduSession> asyncSessions = new ConcurrentLinkedQueue<>();

  public KuduAccessor(KuduConnection connection) {
    this.connection = connection;
  }

  public List<Stage.ConfigIssue> verify(Stage.Context context) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    try {
      verify();
    } catch (KuduException ex) {
      issues.add(
          context.createConfigIssue(
              Groups.KUDU.name(),
              KuduConfigBean.CONF_PREFIX + "connection.kuduMaster",
              Errors.KUDU_00,
              connection.kuduMaster
          )
      );
    }
    return issues;
  }

  // Transformer does not have Stage context yet, so inform failure via exception.
  public void verify() throws KuduException {
    // Check if SDC can reach the Kudu Master
    getKuduClient().getTablesList();
  }

  public synchronized KuduClient getKuduClient() {
    if (client != null) return client;
    KuduClient.KuduClientBuilder builder =
        new KuduClient.KuduClientBuilder(connection.kuduMaster)
            .defaultOperationTimeoutMs(connection.operationTimeout)
            .defaultAdminOperationTimeoutMs(connection.adminOperationTimeout);

    if (connection.numWorkers > 0) {
      builder.workerCount(connection.numWorkers);
    }
    client = builder.build();
    return client;
  }

  /**
   * Create a new KuduSession using the client owned by this class.
   * @return a new KuduSession
   */
  public synchronized KuduSession newSession() {
    if (client == null) getKuduClient();
    KuduSession session = client.newSession();
    sessions.add(session);
    return session;
  }

  public synchronized AsyncKuduClient getAsyncKuduClient() {
    if (asyncClient != null) return asyncClient;
    AsyncKuduClient.AsyncKuduClientBuilder builder =
        new AsyncKuduClient.AsyncKuduClientBuilder(connection.kuduMaster)
            .defaultOperationTimeoutMs(connection.operationTimeout)
            .defaultAdminOperationTimeoutMs(connection.adminOperationTimeout);

    if (connection.numWorkers > 0) {
      builder.workerCount(connection.numWorkers);
    }
    asyncClient = builder.build();
    return asyncClient;
  }

  public synchronized AsyncKuduSession newAsyncSession() {
    if (asyncClient == null) getAsyncKuduClient();
    AsyncKuduSession session = asyncClient.newSession();
    asyncSessions.add(session);
    return session;
  }

  public synchronized void close() {
    sessions.forEach(kuduSession -> {
      try {
        if (!kuduSession.isClosed()) {
          kuduSession.flush();
          kuduSession.close();
        }
      } catch (KuduException e) {
        LOG.error("Error while closing Kudu Session", e);
      }
    });
    asyncSessions.forEach(kuduSession -> {
      try {
        if (!kuduSession.isClosed()) {
          kuduSession.flush();
          List<OperationResponse> responses = kuduSession.close().join();
          responses.forEach(response -> {
            if (response.hasRowError()) {
              throw new RuntimeException(response.getRowError().toString());
            }
          });
        }
      } catch (InterruptedException e) {
        LOG.error("Interrupted while closing Kudu Session", e);
      } catch (Exception e) {
        LOG.error("Error while closing Kudu Session", e);
      }
    });
    try {
      if (client != null) client.close();
    } catch (KuduException e) {
      LOG.error("Error while closing Kudu Client", e);
    }

    try {
      if (asyncClient != null) asyncClient.close();
    } catch (Exception e) {
      LOG.error("Error while closing Kudu Client", e);
    }
  }
}
