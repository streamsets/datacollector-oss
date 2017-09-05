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
package com.streamsets.pipeline.stage.origin.sdcipc;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.destination.sdcipc.Constants;
import org.iq80.snappy.SnappyFramedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

@SuppressWarnings({"squid:S2226", "squid:S1989", "squid:S1948"})
public class IpcServlet extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(IpcServlet.class);

  private final Stage.Context context;
  private final Configs configs;
  private final int maxObjectLen;
  private final BlockingQueue<List<Record>> queue;
  private volatile boolean batchDone;
  private volatile boolean batchCancelled;
  private volatile boolean shuttingDown;
  private volatile boolean inPost;

  public IpcServlet(Stage.Context context, Configs configs, BlockingQueue<List<Record>> queue) {
    this.context = context;
    this.configs = configs;
    maxObjectLen = this.configs.maxRecordSize * 1000 * 1000;
    this.queue = queue;
  }

  private String resolveAppId() throws IOException {
    try {
      return configs.appId.get();
    } catch (StageException e) {
      throw new IOException("Cant resolve credential value", e);
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String appId = req.getHeader(Constants.X_SDC_APPLICATION_ID_HEADER);
    if (!resolveAppId().equals(appId)) {
      LOG.warn("Validation from '{}' invalid appId '{}', rejected", req.getRemoteAddr(), appId);
      resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid 'appId'");
    } else {
      LOG.debug("Validation from '{}', OK", req.getRemoteAddr());
      resp.setHeader(Constants.X_SDC_PING_HEADER, Constants.X_SDC_PING_VALUE);
      resp.setStatus(HttpServletResponse.SC_OK);
    }
  }

  @Override
  protected synchronized void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    inPost = true;
    LOG.debug("Got connection from '{}'", req.getRemoteAddr());

    try {
      if (shuttingDown) {
        LOG.debug("Shutting down, discarding incoming request");
        resp.setStatus(HttpServletResponse.SC_GONE);
      } else {
        String appId = req.getHeader(Constants.X_SDC_APPLICATION_ID_HEADER);
        String compression = req.getHeader(Constants.X_SDC_COMPRESSION_HEADER);
        String contentType = req.getContentType();
        if (!Constants.APPLICATION_BINARY.equals(contentType)) {
          resp.sendError(HttpServletResponse.SC_BAD_REQUEST,
                         Utils.format("Wrong content-type '{}', expected '{}'", contentType,
                                      Constants.APPLICATION_BINARY));
        } else if (!resolveAppId().equals(appId)) {
          LOG.warn("IPC from '{}' invalid appId '{}', rejected", req.getRemoteAddr(), appId);
          resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid 'appId'");
        } else {
          try (InputStream in = req.getInputStream()) {
            InputStream is = in;
            boolean processRequest = true;
            if (compression != null) {
              switch (compression) {
                case Constants.SNAPPY_COMPRESSION:
                  is = new SnappyFramedInputStream(is, true);
                  break;
                default:
                  LOG.warn("Invalid compression '{}' in request, returning error", compression);
                  resp.sendError(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE,
                                 "Unsupported compression: " + compression);
                  processRequest = false;
              }
            }
            if (processRequest) {
              RecordReader reader = ((ContextExtensions) context).createRecordReader(is, 0, maxObjectLen);

              List<Record> records = new ArrayList<>();
              Record record = reader.readRecord();
              while (record != null) {
                records.add(record);
                record = reader.readRecord();
              }
              LOG.debug("Got '{}' records from '{}'", records.size(), req.getRemoteAddr());
              batchDone = false;
              batchCancelled = false;
              queue.add(records);
              synchronized (queue) {
                LOG.debug("Waiting for signal of batch completion");
                while (!(batchDone || batchCancelled)) {
                  queue.wait();
                }
                if (batchDone) {
                  LOG.debug("Batch done");
                  resp.setStatus(HttpServletResponse.SC_OK);
                } else {
                  // Batch cancelled
                  LOG.debug("Batch cancelled: {}", batchCancelled);
                  resp.setStatus(HttpServletResponse.SC_GONE);
                }
              }
            }
          } catch (IOException ex) {
            LOG.warn("Error while reading records: {}", ex.toString(), ex);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ex.toString());
          } catch (InterruptedException ex) {
            LOG.warn("Pipeline stopped while waiting for completion for batch from '{}'", req.getRemoteAddr());
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                "Pipeline stopped while waiting for batch completion");
            Thread.currentThread().interrupt();
          }
        }
      }
    } finally {
      inPost = false;
    }
  }

  public void setShuttingDown() {
    shuttingDown = true;
  }

  public void batchDone() {
    batchDone = true;
  }

  public void batchCancelled() {
    batchCancelled = true;
  }

  public boolean isInPost() {
    return inPost;
  }

}
