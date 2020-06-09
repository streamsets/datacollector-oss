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

package com.streamsets.pipeline.lib.websocket;

import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.microservice.ResponseConfigBean;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import com.streamsets.pipeline.stage.origin.restservice.RestServiceReceiver;
import com.streamsets.pipeline.stage.util.http.HttpStageUtil;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.glassfish.jersey.internal.util.Base64;

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class WebSocketCommon {

  public static WebSocketClient createWebSocketClient(String resourceUrl, TlsConfigBean tlsConf) {
    try {
      resourceUrl = resourceUrl.toLowerCase();
      if (resourceUrl.startsWith("wss")) {
        SslContextFactory sslContextFactory = new SslContextFactory();
        if (tlsConf != null && tlsConf.isEnabled() && tlsConf.isInitialized()) {
          if (tlsConf.getKeyStore() != null) {
            sslContextFactory.setKeyStore(tlsConf.getKeyStore());
          } else {
            if (tlsConf.keyStoreFilePath != null) {
              sslContextFactory.setKeyStorePath(tlsConf.keyStoreFilePath);
            }
            if (tlsConf.keyStoreType != null) {
              sslContextFactory.setKeyStoreType(tlsConf.keyStoreType.getJavaValue());
            }
          }
          if (tlsConf.keyStorePassword != null) {
            sslContextFactory.setKeyStorePassword(tlsConf.keyStorePassword.get());
          }
          if (tlsConf.getTrustStore() != null) {
            sslContextFactory.setTrustStore(tlsConf.getTrustStore());
          } else {
            if (tlsConf.trustStoreFilePath != null) {
              sslContextFactory.setTrustStorePath(tlsConf.trustStoreFilePath);
            }
            if (tlsConf.trustStoreType != null) {
              sslContextFactory.setTrustStoreType(tlsConf.trustStoreType.getJavaValue());
            }
          }
          if (tlsConf.trustStorePassword != null) {
            sslContextFactory.setTrustStorePassword(tlsConf.trustStorePassword.get());
          }

          sslContextFactory.setSslContext(tlsConf.getSslContext());
          sslContextFactory.setIncludeCipherSuites(tlsConf.getFinalCipherSuites());
          sslContextFactory.setIncludeProtocols(tlsConf.getFinalProtocols());
        }
        return new WebSocketClient(sslContextFactory);
      } else {
       return new WebSocketClient();
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(resourceUrl, e);
    }
  }

  public static String generateBasicAuthHeader(String username, String password) {
    if (username == null) {
      username = "";
    }

    if (password == null) {
      password = "";
    }

    final byte[] prefix = (username + ":").getBytes();
    final byte[] passwordByte = password.getBytes();
    final byte[] usernamePassword = new byte[prefix.length + passwordByte.length];

    System.arraycopy(prefix, 0, usernamePassword, 0, prefix.length);
    System.arraycopy(passwordByte, 0, usernamePassword, prefix.length, passwordByte.length);

    return "Basic " + Base64.encodeAsString(usernamePassword);
  }


  public static void sendOriginResponseToWebSocketClient(
      Session wsSession,
      PushSource.Context context,
      DataParserFactory dataParserFactory,
      DataGeneratorFactory dataGeneratorFactory,
      List<Record> sourceResponseRecords,
      ResponseConfigBean responseConfig
  ) throws IOException {
    int responseStatusCode = HttpServletResponse.SC_OK;
    Set<Integer> statusCodesFromResponse = new HashSet<>();
    String errorMessage = null;

    List<Record> successRecords = new ArrayList<>();
    List<Record> errorRecords = new ArrayList<>();
    for (Record responseRecord : sourceResponseRecords) {
      String statusCode = responseRecord.getHeader().getAttribute(
          RestServiceReceiver.STATUS_CODE_RECORD_HEADER_ATTR_NAME
      );
      if (statusCode != null) {
        statusCodesFromResponse.add(Integer.valueOf(statusCode));
      }
      if (responseRecord.getHeader().getErrorMessage() == null) {
        successRecords.add(responseRecord);
      } else {
        errorMessage = responseRecord.getHeader().getErrorMessage();
        errorRecords.add(responseRecord);
      }
    }

    if (statusCodesFromResponse.size() == 1) {
      responseStatusCode = statusCodesFromResponse.iterator().next();
    } else if (statusCodesFromResponse.size() > 1) {
      // If we received more than one status code, return 207 MULTI-STATUS Code
      // https://httpstatuses.com/207
      responseStatusCode = 207;
    }

    List<Record> responseRecords = new ArrayList<>();

    if (responseConfig.sendRawResponse) {
      responseRecords.addAll(successRecords);
      responseRecords.addAll(errorRecords);
    } else {
      Record responseEnvelopeRecord = HttpStageUtil.createEnvelopeRecord(
          context,
          dataParserFactory,
          successRecords,
          errorRecords,
          responseStatusCode,
          errorMessage,
          responseConfig.dataFormat
      );
      responseRecords.add(responseEnvelopeRecord);
    }

    ByteArrayOutputStream byteBufferOutputStream = new ByteArrayOutputStream();
    try (DataGenerator dataGenerator = dataGeneratorFactory.getGenerator(byteBufferOutputStream)) {
      for (Record record : responseRecords) {
        dataGenerator.write(record);
        dataGenerator.flush();
      }
      wsSession.getRemote().sendString(byteBufferOutputStream.toString());
    } catch (DataGeneratorException e) {
      throw new IOException(e);
    }
  }
}
