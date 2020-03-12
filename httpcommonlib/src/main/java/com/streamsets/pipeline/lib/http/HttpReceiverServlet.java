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
package com.streamsets.pipeline.lib.http;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.iq80.snappy.SnappyFramedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

@SuppressWarnings({"squid:S2226", "squid:S1989", "squid:S1948"})
public class HttpReceiverServlet extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(HttpReceiverServlet.class);

  private final HttpReceiver receiver;
  private final BlockingQueue<Exception> errorQueue;
  private final Meter invalidRequestMeter;
  protected final Meter errorRequestMeter;
  protected final Meter requestMeter;
  private final Timer requestTimer;
  private volatile boolean shuttingDown;

  // In case of FlowFile data format, we need to put certain header as the OK response
  // NiFi HTTP destination will check the header and fail if it doesn't see this header
  private static final String NIFI_TRANSACTION_HEADER = "x-nifi-transaction-id";
  // Even if the actual FlowFile is version 1 or 2, passing v3 seems valid
  private static final String NIFI_RESPONSE = "application/flowfile-v3";

  public HttpReceiverServlet(Stage.Context context, HttpReceiver receiver, BlockingQueue<Exception> errorQueue) {
    this.receiver = receiver;
    this.errorQueue = errorQueue;
    invalidRequestMeter = context.createMeter("invalidRequests");
    errorRequestMeter = context.createMeter("errorRequests");
    requestMeter = context.createMeter("requests");
    requestTimer = context.createTimer("requests");
  }

  protected HttpReceiver getReceiver() {
    return receiver;
  }

  // From https://stackoverflow.com/a/31928740/33905
  @VisibleForTesting
  protected static Map<String, String[]> getQueryParameters(HttpServletRequest request) {
    Map<String, String[]> queryParameters = new HashMap<>();
    String queryString = request.getQueryString();

    if (StringUtils.isEmpty(queryString)) {
      return queryParameters;
    }

    String[] parameters = queryString.split("&");

    for (String parameter : parameters) {
      String[] keyValuePair = parameter.split("=");
      String[] values = queryParameters.get(keyValuePair[0]);
      values = ArrayUtils.add(values, keyValuePair.length == 1 ? "" : keyValuePair[1]); //length is one if no value is available.
      queryParameters.put(keyValuePair[0], values);
    }
    return queryParameters;
  }

  @VisibleForTesting
  protected boolean validateAppId(HttpServletRequest req, HttpServletResponse res)
      throws ServletException, IOException {
    boolean valid = false;
    List<? extends CredentialValue> ourAppIds = null;
    try {
      ourAppIds = getReceiver().getAppIds();
    } catch (StageException e) {
      throw new IOException("Cant resolve credential value", e);
    }
    String requestor = req.getRemoteAddr() + ":" + req.getRemotePort();
    String reqAppId = req.getHeader(HttpConstants.X_SDC_APPLICATION_ID_HEADER);

    if (reqAppId == null && receiver.isAppIdViaQueryParamAllowed()) {
      String[] ids = getQueryParameters(req).get(HttpConstants.SDC_APPLICATION_ID_QUERY_PARAM);
      if(ids!=null && ids.length>0)
        reqAppId = ids[0];
    }

    if (reqAppId == null) {
      LOG.warn("Request from '{}' missing appId, rejected", requestor);
      res.sendError(HttpServletResponse.SC_FORBIDDEN, "Missing 'appId'");
    } else {
      int counter = 0;
      while(counter < ourAppIds.size() && !valid){
        valid = valid || ourAppIds.get(counter).get().equals(reqAppId);
        counter++;
      }
      if(!valid) {
        LOG.warn("Request from {} rejected due to invalid appid {}", requestor, reqAppId);
        res.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid 'appId'");
      }
    }
    return valid;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    if (!getReceiver().isApplicationIdEnabled() || validateAppId(req, res)) {
      LOG.debug("Validation from '{}', OK", req.getRemoteAddr());
      res.setHeader(HttpConstants.X_SDC_PING_HEADER, HttpConstants.X_SDC_PING_VALUE);
      res.setStatus(HttpServletResponse.SC_OK);
      if (res.getHeaderNames().contains(NIFI_TRANSACTION_HEADER)) {
        res.setHeader("Accept", NIFI_RESPONSE);
      }
    }
  }

  @VisibleForTesting
  boolean validatePostRequest(HttpServletRequest req, HttpServletResponse res)
      throws ServletException, IOException {
    boolean valid = false;
    if (!getReceiver().isApplicationIdEnabled() || validateAppId(req, res)) {
      String compression = req.getHeader(HttpConstants.X_SDC_COMPRESSION_HEADER);
      if (compression == null) {
        valid = true;
      } else {
        switch (compression) {
          case HttpConstants.SNAPPY_COMPRESSION:
            valid = true;
            break;
          default:
            String requestor = req.getRemoteAddr() + ":" + req.getRemotePort();
            LOG.warn("Invalid compression '{}' in request from '{}', returning error", compression, requestor);
            res.sendError(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE, "Unsupported compression: " + compression);
            break;
        }
      }
    }
    return valid && getReceiver().validate(req, res);
  }

  @Override
  protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    doPost(req, resp);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String requestor = req.getRemoteAddr() + ":" + req.getRemotePort();
    if (isShuttingDown()) {
      LOG.debug("Shutting down, discarding incoming request from '{}'", requestor);
      resp.setStatus(HttpServletResponse.SC_GONE);
    } else {
      if (validatePostRequest(req, resp)) {
        long start = System.currentTimeMillis();
        LOG.debug("Request accepted from '{}'", requestor);
        try (InputStream in = req.getInputStream()) {
          InputStream is = in;
          String compression = req.getHeader(HttpConstants.X_SDC_COMPRESSION_HEADER);
          if (compression == null) {
            compression = req.getHeader(HttpConstants.CONTENT_ENCODING_HEADER);
          }
          if (compression != null) {
            switch (compression) {
              case HttpConstants.SNAPPY_COMPRESSION:
                is = new SnappyFramedInputStream(is, true);
                break;
              case HttpConstants.GZIP_COMPRESSION:
                is = new GZIPInputStream(is);
                break;
              default:
                throw new IOException(Utils.format("It shouldn't happen, unexpected compression '{}'", compression));
            }
          }
          LOG.debug("Processing request from '{}'", requestor);
          processRequest(req, is, resp);
        } catch (Exception ex) {
          errorQueue.offer(ex);
          errorRequestMeter.mark();
          LOG.warn("Error while processing request payload from '{}': {}", requestor, ex.toString(), ex);
          resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ex.toString());
        } finally {
          requestTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        }

      } else {
        invalidRequestMeter.mark();
      }
    }
  }

  protected void processRequest(HttpServletRequest req, InputStream is, HttpServletResponse resp) throws IOException {
    if (getReceiver().process(req, is, resp)) {
      resp.setStatus(HttpServletResponse.SC_OK);
      requestMeter.mark();
    } else {
      resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Record(s) didn't reach all destinations");
      errorRequestMeter.mark();
    }
  }

  @VisibleForTesting
  boolean isShuttingDown() {
    return shuttingDown;
  }

  public void setShuttingDown() {
    shuttingDown = true;
  }

}
