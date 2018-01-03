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
import com.sforce.async.BulkConnection;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.SessionRenewer;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CommonTokenStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import soql.SOQLLexer;
import soql.SOQLParser;

import java.util.List;

public class ForceUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ForceUtils.class);

  public static String getExceptionCode(Throwable th) {
    return (th instanceof ApiFault) ? ((ApiFault) th).getExceptionCode().name() : "";
  }

  public static String getExceptionMessage(Throwable th) {
    return (th instanceof ApiFault) ? ((ApiFault) th).getExceptionMessage() : th.getMessage();
  }

  private static void setProxyConfig(ForceConfigBean conf, ConnectorConfig config) throws StageException {
    if (conf.useProxy) {
      config.setProxy(conf.proxyHostname, conf.proxyPort);
      if (conf.useProxyCredentials) {
        config.setProxyUsername(conf.proxyUsername.get());
        config.setProxyPassword(conf.proxyPassword.get());
      }
    }
  }

  public static ConnectorConfig getPartnerConfig(ForceConfigBean conf, SessionRenewer sessionRenewer) throws StageException {
    ConnectorConfig config = new ConnectorConfig();

    config.setUsername(conf.username.get());
    config.setPassword(conf.password.get());
    config.setAuthEndpoint("https://"+conf.authEndpoint+"/services/Soap/u/"+conf.apiVersion);
    config.setCompression(conf.useCompression);
    config.setTraceMessage(conf.showTrace);
    config.setSessionRenewer(sessionRenewer);

    setProxyConfig(conf, config);

    return config;
  }

  public static BulkConnection getBulkConnection(
      ConnectorConfig partnerConfig,
      ForceConfigBean conf
  ) throws ConnectionException, AsyncApiException, StageException {
    // When PartnerConnection is instantiated, a login is implicitly
    // executed and, if successful,
    // a valid session is stored in the ConnectorConfig instance.
    // Use this key to initialize a BulkConnection:
    ConnectorConfig config = new ConnectorConfig();
    config.setSessionId(partnerConfig.getSessionId());

    // The endpoint for the Bulk API service is the same as for the normal
    // SOAP uri until the /Soap/ part. From here it's '/async/versionNumber'
    String soapEndpoint = partnerConfig.getServiceEndpoint();
    String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/"))
        + "async/" + conf.apiVersion;
    config.setRestEndpoint(restEndpoint);
    config.setCompression(conf.useCompression);
    config.setTraceMessage(conf.showTrace);
    config.setSessionRenewer(partnerConfig.getSessionRenewer());

    setProxyConfig(conf, config);

    return new BulkConnection(config);
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
}
