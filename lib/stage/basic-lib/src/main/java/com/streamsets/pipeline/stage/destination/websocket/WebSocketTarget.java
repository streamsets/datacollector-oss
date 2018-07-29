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
package com.streamsets.pipeline.stage.destination.websocket;

import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.http.Errors;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import com.streamsets.pipeline.lib.websocket.Groups;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class WebSocketTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(WebSocketTarget.class);
  private static final String DATA_FORMAT_CONFIG_PREFIX = "conf.dataFormatConfig.";
  private static final String RESOURCE_URL_CONFIG = "conf.resourceUrl";

  private final WebSocketTargetConfig conf;
  private DataGeneratorFactory generatorFactory;
  private ErrorRecordHandler errorRecordHandler;
  private WebSocketClient webSocketClient = null;

  WebSocketTarget(WebSocketTargetConfig conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    String resourceUrl = conf.resourceUrl.toLowerCase();

    if (!resourceUrl.startsWith("ws://") && !resourceUrl.startsWith("wss://")) {
      LOG.error("Invalid URL: " + conf.resourceUrl);
      issues.add(getContext().createConfigIssue(
          Groups.WEB_SOCKET.name(),
          RESOURCE_URL_CONFIG,
          Errors.HTTP_51, conf.resourceUrl
      ));
    }

    try {
      new URI(conf.resourceUrl);
    } catch (URISyntaxException e) {
      LOG.error("Invalid URL: " + conf.resourceUrl, e);
      issues.add(getContext().createConfigIssue(
          Groups.WEB_SOCKET.name(),
          RESOURCE_URL_CONFIG,
          Errors.HTTP_52, e.toString()
      ));
    }
    createWebSocketClient();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    if (issues.isEmpty()) {
      conf.dataGeneratorFormatConfig.init(
          getContext(),
          conf.dataFormat,
          Groups.WEB_SOCKET.name(),
          DATA_FORMAT_CONFIG_PREFIX,
          issues
      );
      if(issues.isEmpty()) {
        generatorFactory = conf.dataGeneratorFormatConfig.getDataGeneratorFactory();
      }
    }
    if (issues.isEmpty() && conf.tlsConfig.isEnabled()) {
      // this configuration has no separate "tlsEnabled" field on the bean level, so need to do it this way
      conf.tlsConfig.init(
          getContext(),
          Groups.TLS.name(),
          "conf.tlsConfig.",
          issues
      );
    }
    return issues;
  }

  private void createWebSocketClient() {
    try {
      String resourceUrl = conf.resourceUrl.toLowerCase();
      if (resourceUrl.startsWith("wss")) {
        SslContextFactory sslContextFactory = new SslContextFactory();
        final TlsConfigBean tlsConf = conf.tlsConfig;

        if (tlsConf.keyStoreFilePath != null) {
          sslContextFactory.setKeyStorePath(tlsConf.keyStoreFilePath);
        }
        if (tlsConf.keyStoreType != null) {
          sslContextFactory.setKeyStoreType(tlsConf.keyStoreType.getJavaValue());
        }
        if (tlsConf.keyStorePassword != null) {
          sslContextFactory.setKeyStorePassword(tlsConf.keyStorePassword.get());
        }
        if (tlsConf.trustStoreFilePath != null) {
          sslContextFactory.setTrustStorePath(tlsConf.trustStoreFilePath);
        }
        if (tlsConf.trustStoreType != null) {
          sslContextFactory.setTrustStoreType(tlsConf.trustStoreType.getJavaValue());
        }
        if (tlsConf.trustStorePassword != null) {
          sslContextFactory.setTrustStorePassword(tlsConf.trustStorePassword.get());
        }
        if (tlsConf != null && tlsConf.isEnabled() && tlsConf.isInitialized()) {
          sslContextFactory.setSslContext(tlsConf.getSslContext());
          sslContextFactory.setIncludeCipherSuites(tlsConf.getFinalCipherSuites());
          sslContextFactory.setIncludeProtocols(tlsConf.getFinalProtocols());
        }
        webSocketClient = new WebSocketClient(sslContextFactory);
      } else {
        webSocketClient = new WebSocketClient();
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(conf.resourceUrl, e);
    }
  }

  @Override
  public void write(Batch batch) throws StageException {
    Session wsSession = null;
    try {
      WebSocketTargetSocket webSocketTargetSocket = new WebSocketTargetSocket(
          conf,
          generatorFactory,
          errorRecordHandler,
          batch
      );
      webSocketClient.start();
      URI webSocketUri = new URI(conf.resourceUrl);
      ClientUpgradeRequest request = new ClientUpgradeRequest();
      for(HeaderBean header : conf.headers) {
        request.setHeader(header.key, header.value.get());
      }
      Future<Session> connectFuture = webSocketClient.connect(webSocketTargetSocket, webSocketUri, request);
      wsSession = connectFuture.get();
      if (!webSocketTargetSocket.awaitClose(conf.maxRequestCompletionSecs, TimeUnit.SECONDS)) {
        throw new RuntimeException("Failed to send all records in maximum wait time.");
      }
    } catch (Exception ex) {
      LOG.error(Errors.HTTP_50.getMessage(), ex.toString(), ex);
      errorRecordHandler.onError(Lists.newArrayList(batch.getRecords()), throwStageException(ex));
    } finally {
      if (wsSession != null) {
        wsSession.close();
      }
      try {
        webSocketClient.stop();
      } catch (Exception e) {
        LOG.error(Errors.HTTP_50.getMessage(), e.toString(), e);
      }
    }
  }

  private static StageException throwStageException(Exception e) {
    if (e instanceof RuntimeException) {
      Throwable cause = e.getCause();
      if (cause != null) {
        return new StageException(Errors.HTTP_50, cause, cause);
      }
    } else if (e instanceof StageException) {
      return (StageException)e;
    }
    return new StageException(Errors.HTTP_50, e, e);
  }

  @Override
  public void destroy() {
    super.destroy();
  }

}
