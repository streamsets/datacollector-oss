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
package com.streamsets.pipeline.stage.destination.sdcipc;

import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.iq80.snappy.SnappyFramedOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class SdcIpcTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(SdcIpcTarget.class);

  private final Configs config;
  private ErrorRecordHandler errorRecordHandler;
  final List<String> standByHostPorts;
  final List<String> activeHostPorts;
  int lastActive;

  public SdcIpcTarget(Configs config) {
    this.config = config;
    standByHostPorts = new ArrayList<>();
    activeHostPorts = new ArrayList<>();
    lastActive = -1;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    issues.addAll(config.init(getContext()));
    if (issues.isEmpty()) {
      initializeHostPortsLists();
    }
    return issues;
  }

  int getActiveConnectionsNumber() {
    int count = (int) Math.log(config.hostPorts.size()) + 1;
    return (count < 2) ? 2 : count;
  }

  void initializeHostPortsLists() {
    if (config.hostPorts.size() == 1) {
      lastActive = 0;
      activeHostPorts.addAll(config.hostPorts);
      LOG.debug("There is only one hostPort '{}'", activeHostPorts.get(0));
    } else {
      List<String> hostsPorts = config.hostPorts;
      // randomize hostPorts
      Collections.shuffle(hostsPorts);

      // separate active from standby
      int active = getActiveConnectionsNumber();
      for (int i = 0; i < hostsPorts.size(); i++) {
        if (i < active) {
          activeHostPorts.add(hostsPorts.get(i));
        } else {
          standByHostPorts.add(hostsPorts.get(i));
        }
      }
      LOG.debug("Active hostPorts: {}", activeHostPorts);
      LOG.debug("Standby hostPorts: {}", standByHostPorts);
    }
  }

  String getHostPort(boolean previousOneHadError) {
    if (activeHostPorts.size() == 1) {
      return activeHostPorts.get(0);
    } else {
      if (previousOneHadError && !standByHostPorts.isEmpty()) {
        String goingIn = standByHostPorts.remove(0);
        String goingOut = activeHostPorts.set(lastActive, goingIn);
        standByHostPorts.add(goingOut);
        LOG.debug("Sending '{}' hostPort to standby and activating '{}' hostPost", goingOut, goingIn);
        LOG.debug("Active hostPorts: {}", activeHostPorts);
        LOG.debug("Standby hostPorts: {}", standByHostPorts);
      } else {
        lastActive = (lastActive + 1) % activeHostPorts.size();
      }
      String hostPort = activeHostPorts.get(lastActive);
      LOG.debug("Selecting hostPort '{}'", hostPort);
      return hostPort;
    }
  }

  HttpURLConnection createWriteConnection(boolean isRetry) throws IOException, StageException {
    HttpURLConnection  conn = config.createConnection(getHostPort(isRetry));
    conn.setRequestMethod("POST");
    conn.setRequestProperty(Constants.CONTENT_TYPE_HEADER, Constants.APPLICATION_BINARY);
    conn.setRequestProperty(Constants.X_SDC_JSON1_FRAGMENTABLE_HEADER, "true");
    conn.setDefaultUseCaches(false);
    conn.setDoOutput(true);
    conn.setDoInput(true);
    return conn;
  }

  @Override
  public void write(Batch batch) throws StageException {
    ContextExtensions ext = (ContextExtensions) getContext();
    boolean ok = false;
    int retryCount = 0;
    String errorReason = null;
    HttpURLConnection conn = null;

    while (!ok && retryCount <= config.retriesPerBatch) {
      LOG.debug("Writing out batch for entity '{}' and offset '{}' retry '{}'", batch.getSourceEntity(), batch.getSourceOffset(), retryCount);
      config.backOffWait(retryCount);

      try {
        conn = createWriteConnection(retryCount > 0);
        if (config.compression) {
          conn.setRequestProperty(Constants.X_SDC_COMPRESSION_HEADER, Constants.SNAPPY_COMPRESSION);
        }
        OutputStream os = conn.getOutputStream();
        if (config.compression) {
          os = new SnappyFramedOutputStream(os);
        }
        RecordWriter writer = ext.createRecordWriter(os);
        Iterator<Record> it = batch.getRecords();
        while (it.hasNext()) {
          Record record = it.next();
          writer.write(record);
        }
        writer.close();
        os.close();
        ok = conn.getResponseCode() == HttpURLConnection.HTTP_OK;
        if (!ok) {
          errorReason = conn.getResponseMessage();
          LOG.warn("Batch for entity '{}' and offset '{}' could not be written out: {}", batch.getSourceEntity(), batch.getSourceOffset(), errorReason);
        } else {
          LOG.debug("Batch for entity '{}' and offset '{}' written out on retry '{}'", batch.getSourceEntity(), batch.getSourceOffset(), retryCount);
        }
      } catch (IOException ex) {
        errorReason = ex.toString();
        LOG.warn("Batch for entity '{}' and offset '{}' could not be written out: {}", batch.getSourceEntity(), batch.getSourceOffset(), errorReason, ex);

        if (conn != null) {
          conn.disconnect();
        }
      }
      retryCount++;
    }
    if (!ok) {
      OnRecordError onErrorRecord = getContext().getOnErrorRecord();
      // this branch only happens when the pipeline error handling strategy is "send to RPC". if we can't forward to
      // that pipeline, then it's a pipeline-stopping problem.
      if (onErrorRecord == null) {
        throw new StageException(Errors.IPC_DEST_20, errorReason);
      }

      errorRecordHandler.onError(
          Lists.newArrayList(batch.getRecords()),
          new StageException(
              Errors.IPC_DEST_20,
              errorReason
          )
      );
    }
  }

}
