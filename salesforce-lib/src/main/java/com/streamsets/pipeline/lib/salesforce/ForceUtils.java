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
package com.streamsets.pipeline.lib.salesforce;

import com.sforce.async.AsyncApiException;
import com.sforce.async.AsyncExceptionCode;
import com.sforce.async.BulkConnection;
import com.sforce.soap.partner.SessionHeader_element;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.SessionRenewer;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.lib.salesforce.connection.mutualauth.ClientSSLTransportFactory;
import com.streamsets.pipeline.lib.salesforce.connection.mutualauth.MutualAuthConfigBean;
import com.streamsets.pipeline.lib.salesforce.connection.mutualauth.MutualAuthConnectorConfig;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CommonTokenStream;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.util.BasicAuthentication;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import soql.SOQLLexer;
import soql.SOQLParser;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class ForceUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ForceUtils.class);
  private static final int MUTUAL_AUTHENTICATION_PORT = 8443;
  public static final String RECORD_ID_OFFSET_PREFIX = "recordId:";
  public static final String EVENT_ID_OFFSET_PREFIX = "eventId:";
  public static final long EVENT_ID_FROM_NOW = -1;
  private static final long EVENT_ID_FROM_START = -2;
  public static final String READ_EVENTS_FROM_START = EVENT_ID_OFFSET_PREFIX + EVENT_ID_FROM_START;
  public static final String READ_EVENTS_FROM_NOW = ForceUtils.EVENT_ID_OFFSET_PREFIX + ForceUtils.EVENT_ID_FROM_NOW;

  public static String getExceptionCode(Throwable th) {
    return (th instanceof ApiFault) ? ((ApiFault) th).getExceptionCode().name() : "";
  }

  public static String getExceptionMessage(Throwable th) {
    return (th instanceof ApiFault) ? ((ApiFault) th).getExceptionMessage() : th.getMessage();
  }

  private static void setProxyConfig(ForceConfigBean conf, ConnectorConfig config) throws StageException {
    if (conf.connection.useProxy) {
      config.setProxy(conf.connection.proxyHostname, conf.connection.proxyPort);
      if (conf.connection.useProxyCredentials) {
        config.setProxyUsername(conf.connection.proxyUsername.get());
        config.setProxyPassword(conf.connection.proxyPassword.get());
      }
    }
  }

  public static ConnectorConfig getPartnerConfig(ForceConfigBean conf, SessionRenewer sessionRenewer) throws StageException {
    ConnectorConfig config = new ConnectorConfig();

    config.setUsername(conf.connection.username.get());
    config.setPassword(conf.connection.password.get());
    config.setAuthEndpoint("https://"+conf.connection.authEndpoint+"/services/Soap/u/"+conf.connection.apiVersion);
    config.setCompression(conf.useCompression);
    config.setTraceMessage(conf.showTrace);
    config.setSessionRenewer(sessionRenewer);

    setProxyConfig(conf, config);

    return config;
  }

  public static BulkConnection getBulkConnection(
      ConnectorConfig partnerConfig,
      ForceConfigBean conf
  ) throws ConnectionException, AsyncApiException, StageException, URISyntaxException {
    // When PartnerConnection is instantiated, a login is implicitly
    // executed and, if successful,
    // a valid session is stored in the ConnectorConfig instance.
    // Use this key to initialize a BulkConnection:
    ConnectorConfig config = conf.connection.mutualAuth.useMutualAuth
        ? new MutualAuthConnectorConfig(conf.connection.mutualAuth.getUnderlyingConfig().getSslContext())
        : new ConnectorConfig();
    config.setSessionId(partnerConfig.getSessionId());

    // The endpoint for the Bulk API service is the same as for the normal
    // SOAP uri until the /Soap/ part. From here it's '/async/versionNumber'
    String soapEndpoint = partnerConfig.getServiceEndpoint();
    String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/"))
        + "async/" + conf.connection.apiVersion;
    config.setRestEndpoint(restEndpoint);
    config.setCompression(conf.useCompression);
    config.setTraceMessage(conf.showTrace);
    config.setSessionRenewer(partnerConfig.getSessionRenewer());

    setProxyConfig(conf, config);

    BulkConnection bulkConnection = new BulkConnection(config);

    if (conf.connection.mutualAuth.useMutualAuth) {
      setupMutualAuthBulk(config, conf.connection.mutualAuth);
    }

    return bulkConnection;
  }

  public static String getSobjectTypeFromQuery(String query) throws StageException {
    try {
      SOQLParser.StatementContext statementContext = getStatementContext(query);

      return statementContext.objectList().getText().toLowerCase();
    } catch (Exception e) {
      LOG.error(Errors.FORCE_27.getMessage(), query, e);
      throw new StageException(Errors.FORCE_27, query, e);
    }
  }

  public static SOQLParser.StatementContext getStatementContext(String query) {
    SOQLLexer lexer = new SOQLLexer(new ANTLRInputStream(query));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SOQLParser parser = new SOQLParser(tokens);
    parser.setErrorHandler(new BailErrorStrategy());
    return parser.statement();
  }

  public static int getOperationFromRecord(Record record,
      SalesforceOperationType defaultOp,
      UnsupportedOperationAction unsupportedAction,
      List<OnRecordErrorException> errorRecords) {
    String op = record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE);
    int opCode = -1; // unsupported
    // Check if the operation code from header attribute is valid
    if (op != null && !op.isEmpty()) {
      try {
        opCode = SalesforceOperationType.convertToIntCode(op);
      } catch (NumberFormatException | UnsupportedOperationException ex) {
        LOG.debug(
            "Operation obtained from record is not supported. Handle by UnsupportedOpertaionAction {}. {}",
            unsupportedAction.getLabel(),
            ex
        );
        switch (unsupportedAction) {
          case DISCARD:
            LOG.debug("Discarding record with unsupported operation {}", op);
            break;
          case SEND_TO_ERROR:
            LOG.debug("Sending record to error due to unsupported operation {}", op);
            errorRecords.add(new OnRecordErrorException(record, Errors.FORCE_23, op));
            break;
          case USE_DEFAULT:
            opCode = defaultOp.code;
            break;
          default: //unknown action
            LOG.debug("Sending record to error due to unknown operation {}", op);
        }
      }
    } else {
      opCode = defaultOp.code;
    }
    return opCode;
  }

  public static void setProxy(HttpClient httpClient, ForceConfigBean conf) throws URISyntaxException, StageException {
    httpClient.getProxyConfiguration().getProxies().add(
        new HttpProxy(conf.connection.proxyHostname, conf.connection.proxyPort));
    if (conf.connection.useProxyCredentials) {
      URI proxyURI = new URI("http",
          null,
          conf.connection.proxyHostname,
          conf.connection.proxyPort,
          null,
          null,
          null
      );
      httpClient.getAuthenticationStore().addAuthentication(new BasicAuthentication(
          proxyURI,
          conf.connection.proxyRealm.get(),
          conf.connection.proxyUsername.get(),
          conf.connection.proxyPassword.get()
      ));
    }
  }

  public static void setupMutualAuth(ConnectorConfig config, MutualAuthConfigBean mutualAuth) throws
      URISyntaxException {
    String serviceEndpoint = config.getServiceEndpoint();
    config.setTransportFactory(new ClientSSLTransportFactory(mutualAuth.getUnderlyingConfig().getSslContext()));
    config.setServiceEndpoint(changePort(serviceEndpoint, MUTUAL_AUTHENTICATION_PORT));

    LOG.debug("Set Service Endpoint to {} for Mutual Authentication", config.getServiceEndpoint());
  }

  public static void setupMutualAuthBulk(ConnectorConfig config, MutualAuthConfigBean mutualAuth) throws
      URISyntaxException {
    String serviceEndpoint = config.getRestEndpoint();
    // Bulk API client doesn't set ConnectorConfig when using custom transport
    config.setTransportFactory(new ClientSSLTransportFactory(mutualAuth.getUnderlyingConfig().getSslContext(), config));
    config.setRestEndpoint(changePort(serviceEndpoint, MUTUAL_AUTHENTICATION_PORT));

    LOG.debug("Set Service Endpoint to {} for Mutual Authentication", config.getServiceEndpoint());
  }

  private static String changePort(String url, int port) throws URISyntaxException {
    URI uri = new URI(url);
    return new URI(uri.getScheme(),
        uri.getUserInfo(),
        uri.getHost(),
        port,
        uri.getPath(),
        uri.getQuery(),
        uri.getFragment()
    ).toString();
  }

  public static SslContextFactory makeSslContextFactory(ForceConfigBean conf) throws StageException {
    SslContextFactory sslContextFactory = new SslContextFactory();
    if (conf.connection.mutualAuth.useMutualAuth) {
      sslContextFactory.setKeyStore(conf.connection.mutualAuth.getUnderlyingConfig().getKeyStore());
      // Need to set password in the SSLContextFactory even though it's set in the KeyStore
      sslContextFactory.setKeyStorePassword(conf.connection.mutualAuth.getUnderlyingConfig().keyStorePassword.get());
    }
    return sslContextFactory;
  }

  public static void renewSession(BulkConnection connection, AsyncApiException e) throws AsyncApiException {
    if (AsyncExceptionCode.InvalidSessionId.equals(e.getExceptionCode())) {
      ConnectorConfig config = connection.getConfig();
      try {
        SessionRenewer.SessionRenewalHeader sessionHeader = config.getSessionRenewer().renewSession(config);
        config.setSessionId(((SessionHeader_element)sessionHeader.headerElement).getSessionId());
      } catch (ConnectionException e1) {
        // Can't renew the session - log an error and throw the original AsyncApiException
        LOG.error("Exception renewing session", e1);
        throw e;
      }
    }
  }
}
