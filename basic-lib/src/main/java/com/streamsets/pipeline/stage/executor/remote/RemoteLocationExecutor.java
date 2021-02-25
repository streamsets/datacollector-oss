/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.executor.remote;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseExecutor;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.connection.remote.Protocol;
import com.streamsets.pipeline.lib.remote.RemoteConfigBean;
import com.streamsets.pipeline.lib.remote.RemoteConnector;
import com.streamsets.pipeline.lib.remote.RemoteFile;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.commons.lang3.StringUtils;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class RemoteLocationExecutor extends BaseExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteLocationExecutor.class);
  private final RemoteConfigBean remoteConfig;
  private final PostProcessingFileActionsConfig action;

  private static final Pattern URL_PATTERN_FTP = Pattern.compile("(ftp://).*:?.*");
  private static final Pattern URL_PATTERN_FTPS = Pattern.compile("(ftps://).*:?.*");
  private static final Pattern URL_PATTERN_SFTP = Pattern.compile("(sftp://).*:?.*");

  private ErrorRecordHandler errorRecordHandler;
  private Map<String, ELEval> evals;

  private RemoteLocationExecutorDelegate delegate;

  public RemoteLocationExecutor(RemoteExecutorConfigBean conf) {
    this.remoteConfig = conf.remoteConfig;
    this.action = conf.action;
  }

  private void validateEL(String configName, String el, List<ConfigIssue> issues) {
    try {
      evals.put(configName, getContext().createELEval(configName));
      getContext().parseEL(el);
    } catch (ELEvalException e) {
      issues.add(getContext().createConfigIssue(Groups.ACTION.name(), configName, Errors.REMOTE_LOCATION_EXECUTOR_01, e.getMessage()));
    }
  }

  private String evaluate(ELVars variables, String configOptionName, String expression) throws ELEvalException {
    return evals.get(configOptionName).eval(variables, expression, String.class);
  }

  @Override
  protected List<ConfigIssue> init() {
    List <ConfigIssue> issues = super.init();

    URI remoteURI = RemoteConnector.getURI(remoteConfig, issues, getContext(), Groups.REMOTE);

    if (issues.isEmpty()) {
      if (remoteConfig.connection.protocol == Protocol.FTP || remoteConfig.connection.protocol == Protocol.FTPS) {
        delegate = new FTPRemoteLocationExecutorDelegate(remoteConfig);
        delegate.initAndConnect(issues, getContext(), remoteURI);
      } else if (remoteConfig.connection.protocol == Protocol.SFTP) {
        delegate = new SFTPRemoteLocationExecutorDelegate(remoteConfig);
        delegate.initAndConnect(issues, getContext(), remoteURI);
      }
    }
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    evals = new HashMap<>();
    validateEL("filePath", action.filePath, issues);

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

      ELVars variables = getContext().createELVars();
      RecordEL.setRecordInContext(variables, record);
      String remoteFileName = evaluate(variables, "filePath", action.filePath);

      try {
        RemoteFile file = delegate.getFile(remoteFileName);
        String fileName = file.getFilePath();

        if (!file.exists()) {
          Log.debug("File: {} does not exist in remote destination", fileName);
          throw new OnRecordErrorException(record, Errors.REMOTE_LOCATION_EXECUTOR_02, fileName);
        }

        switch (action.actionType) {
          case DELETE_FILE:
            delegate.delete(fileName);
            break;
          case MOVE_FILE:
            boolean isMoveSuccessful = delegate.move(fileName,
                action.targetDir,
                action.fileExistsAction == WholeFileExistsAction.OVERWRITE
            );
            if (!isMoveSuccessful) {
              throw new OnRecordErrorException(record, Errors.REMOTE_LOCATION_EXECUTOR_04, fileName);
            }
            break;
          default:
            break;
        }
      } catch (OnRecordErrorException e) {
        errorRecordHandler.onError(e);
      } catch (IOException e) {
        LOG.error("Failure to perform action: {} on file: {}",
            action.actionType,
            StringUtils.substringAfterLast(remoteFileName, "/"),
            e
        );
        errorRecordHandler.onError(new OnRecordErrorException(
            record,
            Errors.REMOTE_LOCATION_EXECUTOR_03,
            action.actionType,
            e.getMessage(),
            e
        ));
      }
    }
  }

  @Override
  public void destroy() {
    LOG.info("Destroying {}", getInfo().getInstanceName());
    try {
      if (delegate != null) {
        delegate.close();
      }
    } catch (IOException ex) {
      LOG.warn("Error during destroy", ex);
    } finally {
      delegate = null;
    }
  }
}