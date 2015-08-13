/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.sdcipc;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SdcIpcSource extends BaseSource implements OffsetCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(SdcIpcSource.class);

  private final Configs configs;
  private IpcServer ipcServer;
  private long counter;

  public SdcIpcSource(Configs configs) {
    this.configs = configs;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    issues.addAll(configs.init(getContext()));
    if (issues.isEmpty()) {
      ipcServer = new IpcServer(getContext(), configs);
      try {
        ipcServer.start();
      } catch (Exception ex) {
        Stage.ConfigIssue issue = getContext().createConfigIssue(null, null, Errors.IPC_ORIG_20, ex.toString());
        LOG.warn(issue.toString(), ex);
        issues.add(issue);
      }
    }
    return issues;
  }

  @Override
  public void destroy() {
    if (ipcServer != null) {
      ipcServer.stop();
    }
    super.destroy();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    try {
      List<Record> records = ipcServer.poll(configs.maxWaitTimeSecs);
      if (records != null) {
        LOG.debug("Got '{}' records", records.size());
        for (Record record : records) {
          batchMaker.addRecord(record);
        }
      } else {
        LOG.debug("No records after '{}'secs, dispatching empty batch", configs.maxWaitTimeSecs);
      }
    } catch (InterruptedException ex) {
      LOG.debug("Interrupted while waiting for records");
      ipcServer.cancelBatch();
    }
    return "::ipc::" + (counter++) + System.currentTimeMillis();
  }

  @Override
  public void commit(String offset) throws StageException {
    LOG.debug("Notifying IpcServer that batch is done");
    ipcServer.doneWithBatch();
  }

}
