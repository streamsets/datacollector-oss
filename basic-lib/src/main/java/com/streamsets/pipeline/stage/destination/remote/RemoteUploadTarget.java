/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.remote;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.event.WholeFileProcessedEvent;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.stage.connection.remote.Protocol;
import com.streamsets.pipeline.lib.remote.RemoteConnector;
import com.streamsets.pipeline.lib.remote.RemoteFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class RemoteUploadTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteUploadTarget.class);
  private static final String CONF_PREFIX = "conf.";
  public static final String EVENT_PATH_FIELD_NAME = "path";

  private URI remoteURI;
  private final RemoteUploadConfigBean conf;
  private RemoteUploadTargetDelegate delegate;
  private DataGeneratorFactory dataGeneratorFactory;
  private ELEval fileNameEval;

  public RemoteUploadTarget(RemoteUploadConfigBean conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    if (conf.dataFormat == null || !conf.dataFormat.equals(DataFormat.WHOLE_FILE)) {
      issues.add(getContext().createConfigIssue(
          Groups.DATA_FORMAT.getLabel(),
          CONF_PREFIX + "dataFormat",
          Errors.REMOTE_UPLOAD_04,
          conf.dataFormat
      ));
    }

    conf.dataFormatConfig.init(
        getContext(),
        conf.dataFormat,
        Groups.DATA_FORMAT.getLabel(),
        CONF_PREFIX + "dataFormatConfig.",
        issues
    );

    this.remoteURI = RemoteConnector.getURI(conf.remoteConfig, issues, getContext(), Groups.REMOTE);

    if (issues.isEmpty()) {
      if (conf.remoteConfig.connection.protocol == Protocol.FTP || conf.remoteConfig.connection.protocol == Protocol.FTPS) {
        delegate = new FTPRemoteUploadTargetDelegate(conf.remoteConfig);
        delegate.initAndConnect(issues, getContext(), remoteURI);
      } else if (conf.remoteConfig.connection.protocol == Protocol.SFTP) {
        delegate = new SFTPRemoteUploadTargetDelegate(conf.remoteConfig);
        delegate.initAndConnect(issues, getContext(), remoteURI);
      }
    }

    if (issues.isEmpty()) {
      dataGeneratorFactory = conf.dataFormatConfig.getDataGeneratorFactory();
      fileNameEval = getContext().createELEval("fileNameEL");
    }

    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> recordIterator = batch.getRecords();
    if (recordIterator.hasNext()) {
      delegate.verifyAndReconnect();
    }

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();

      ELVars vars = getContext().createELVars();
      RecordEL.setRecordInContext(vars, record);
      TimeNowEL.setTimeNowInContext(vars, new Date());
      String filename = fileNameEval.eval(vars, conf.dataFormatConfig.fileNameEL, String.class);

      try {
        RemoteFile file = delegate.getFile(filename);
        LOG.debug("About to write '{}'", file.getFilePath());

        if (conf.dataFormatConfig.wholeFileExistsAction == WholeFileExistsAction.TO_ERROR) {
          if (file.exists()) {
            LOG.debug("Sending '{}' to Error", file.getFilePath());
            getContext().toError(record, Errors.REMOTE_UPLOAD_02, filename);
            continue;
          }
        }

        try(OutputStream os = file.createOutputStream()) {
          DataGenerator generator = dataGeneratorFactory.getGenerator(os);
          generator.write(record);
        }
        file.commitOutputStream();
        LOG.debug("Finished writing '{}'", file.getFilePath());
        sendCompleteEvent(record, file);
      } catch (IOException ioe) {
        LOG.error(Errors.REMOTE_UPLOAD_03.getMessage(), filename, ioe.getMessage(), ioe);
        getContext().toError(record, Errors.REMOTE_UPLOAD_03, ioe);
      }
    }
  }

  private void sendCompleteEvent(Record record, RemoteFile file) {
    LOG.debug("Sending File Transfer Complete Event for '{}'", file.getFilePath());
    WholeFileProcessedEvent.FILE_TRANSFER_COMPLETE_EVENT.create(
        getContext())
        .with(
            WholeFileProcessedEvent.SOURCE_FILE_INFO,
            record.get(FileRefUtil.FILE_INFO_FIELD_PATH).getValueAsMap()
        )
        .withStringMap(WholeFileProcessedEvent.TARGET_FILE_INFO,
            ImmutableMap.of(EVENT_PATH_FIELD_NAME, file.getFilePath())
        )
        .createAndSend();
  }

  @Override
  public void destroy() {
    LOG.info(Utils.format("Destroying {}", getInfo().getInstanceName()));
    try {
      if (delegate != null) {
        delegate.close();
      }
    } catch (IOException ex) {
      LOG.warn("Error during destroy", ex);
    } finally {
      delegate = null;
      dataGeneratorFactory = null;
    }
  }
}
