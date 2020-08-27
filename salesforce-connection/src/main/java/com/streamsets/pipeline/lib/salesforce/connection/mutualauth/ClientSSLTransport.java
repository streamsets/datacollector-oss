/*
 * This file was copied from Force.com Web Service Connector (WSC) project and altered to allow Mutual Authentication
 * to work.
 *
 * Copyright (c) 2017, salesforce.com, inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 *    Redistributions of source code must retain the above copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 *    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 *    the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 *    Neither the name of salesforce.com, inc. nor the names of its contributors may be used to endorse or
 *    promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package com.streamsets.pipeline.lib.salesforce.connection.mutualauth;

import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.MessageHandler;
import com.sforce.ws.MessageHandlerWithHeaders;
import com.sforce.ws.transport.JdkHttpTransport;
import com.sforce.ws.transport.LimitingInputStream;
import com.sforce.ws.transport.LimitingOutputStream;
import com.sforce.ws.transport.MessageHandlerOutputStream;
import com.sforce.ws.transport.Transport;
import com.sforce.ws.util.FileUtil;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

// Based on JdkHttpTransport from the Salesforce WSC library, modified as suggested by
// Steven Lawrance at https://success.salesforce.com/answers?id=9063A000000Dj7SQAS
public class ClientSSLTransport implements Transport {
  private HttpURLConnection connection;
  private boolean successful;
  private ConnectorConfig config;
  private URL url;
  private SSLContext sc;

  public ClientSSLTransport(SSLContext sc, ConnectorConfig config) {
    this.sc = sc;
    this.config = config;
  }

  @Override
  public void setConfig(ConnectorConfig config) {
    this.config = config;
  }

  @Override
  public OutputStream connect(String uri, HashMap<String, String> httpHeaders) throws IOException {
    return connectLocal(uri, httpHeaders, true);
  }

  @Override
  public OutputStream connect(String uri, HashMap<String, String> httpHeaders, boolean enableCompression)
      throws IOException {
    return connectLocal(uri, httpHeaders, enableCompression);
  }

  @Override
  public OutputStream connect(String uri, String soapAction) throws IOException {
    if (soapAction == null) {
      soapAction = "";
    }

    HashMap<String, String> header = new HashMap<String, String>();

    header.put("SOAPAction", "\"" + soapAction + "\"");
    header.put("Content-Type", "text/xml; charset=UTF-8");
    header.put("Accept", "text/xml");

    return connectLocal(uri, header);
  }

  private OutputStream connectLocal(String uri, HashMap<String, String> httpHeaders) throws IOException {
    return connectLocal(uri, httpHeaders, true);
  }

  private OutputStream connectLocal(String uri, HashMap<String, String> httpHeaders, boolean enableCompression)
      throws IOException {
    return wrapOutput(connectRaw(uri, httpHeaders, enableCompression), enableCompression);
  }

  private OutputStream wrapOutput(OutputStream output, boolean enableCompression) throws IOException {
    if (config.getMaxRequestSize() > 0) {
      output = new LimitingOutputStream(config.getMaxRequestSize(), output);
    }

    // when we are writing a zip file we don't bother with compression
    if (enableCompression && config.isCompression()) {
      output = new GZIPOutputStream(output);
    }

    if (config.isTraceMessage()) {
      output = config.teeOutputStream(output);
    }

    if (config.hasMessageHandlers()) {
      output = new MessageHandlerOutputStream(config, url, output);
    }

    return output;
  }

  private OutputStream connectRaw(String uri, HashMap<String, String> httpHeaders, boolean enableCompression)
      throws IOException {
    url = new URL(uri);

    connection = JdkHttpTransport.createConnection(config, url, httpHeaders, enableCompression);
    if (connection instanceof HttpsURLConnection) {
      ((HttpsURLConnection)connection).setSSLSocketFactory(sc.getSocketFactory());
    }
    connection.setRequestMethod("POST");
    connection.setDoInput(true);
    connection.setDoOutput(true);
    if (config.useChunkedPost()) {
      connection.setChunkedStreamingMode(4096);
    }

    return connection.getOutputStream();
  }

  @Override
  public InputStream getContent() throws IOException {
    InputStream in;

    try {
      in = connection.getInputStream();
    } catch (IOException e) {
      in = connection.getErrorStream();
      if (in == null) {
        throw e;
      }
    }

    successful = connection.getResponseCode() < 400;

    String encoding = connection.getHeaderField("Content-Encoding");

    if (config.getMaxResponseSize() > 0) {
      in = new LimitingInputStream(config.getMaxResponseSize(), in);
    }

    if ("gzip".equals(encoding)) {
      in = new GZIPInputStream(in);
    }

    if (config.hasMessageHandlers() || config.isTraceMessage()) {
      byte[] bytes = FileUtil.toBytes(in);
      in = new ByteArrayInputStream(bytes);

      if (config.hasMessageHandlers()) {
        Iterator<MessageHandler> it = config.getMessagerHandlers();
        while(it.hasNext()) {
          MessageHandler handler = it.next();
          if (handler instanceof MessageHandlerWithHeaders) {
            ((MessageHandlerWithHeaders) handler).handleResponse(url, bytes, connection.getHeaderFields());
          } else {
            handler.handleResponse(url, bytes);
          }
        }
      }

      if (config.isTraceMessage()) {
        Map<String, List<String>> headers = connection.getHeaderFields();
        for (Map.Entry header : headers.entrySet()) {
          config.getTraceStream().print(header.getKey());
          config.getTraceStream().print("=");
          config.getTraceStream().println(header.getValue());
        }

        config.teeInputStream(bytes);
      }
    }

    return in;
  }

  @Override
  public boolean isSuccessful() {
    return successful;
  }
}
