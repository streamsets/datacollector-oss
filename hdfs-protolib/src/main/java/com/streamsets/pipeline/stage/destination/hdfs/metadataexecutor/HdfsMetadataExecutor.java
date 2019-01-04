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
package com.streamsets.pipeline.stage.destination.hdfs.metadataexecutor;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseExecutor;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.hdfs.util.HdfsUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HdfsMetadataExecutor extends BaseExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsMetadataExecutor.class);

  private final HdfsConnectionConfig hdfsConnection;
  private final HdfsActionsConfig actions;
  private ErrorRecordHandler errorRecordHandler;
  private Map<String, ELEval> evals;

  public HdfsMetadataExecutor(HdfsConnectionConfig hdfsConnection, HdfsActionsConfig actions) {
    this.hdfsConnection = hdfsConnection;
    this.actions = actions;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    hdfsConnection.init(getContext(), "connection", issues);
    actions.init(getContext(), "actions", issues);

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    evals = new HashMap<>();
    validateEL("filePath", actions.filePath, issues);
    if(actions.shouldMoveFile) {
      validateEL("newLocation", actions.newLocation, issues);
    }
    if(actions.shouldRename) {
      validateEL("newName", actions.newName, issues);
    }
    if(actions.shouldChangeOwnership) {
      validateEL("newOwner", actions.newOwner, issues);
      validateEL("newGroup", actions.newGroup, issues);
    }
    if(actions.shouldSetPermissions) {
      validateEL("newPermissions", actions.newPermissions, issues);
    }
    if(actions.shouldSetAcls) {
      validateEL("newAcls", actions.newAcls, issues);
    }

    return issues;
  }

  private void validateEL(String configName, String el, List<ConfigIssue> issues) {
     try {
      evals.put(configName, getContext().createELEval(configName));
      getContext().parseEL(el);
    } catch (ELEvalException e) {
      issues.add(getContext().createConfigIssue(Groups.TASKS.name(), configName, HdfsMetadataErrors.HDFS_METADATA_007, e.getMessage()));
    }
  }

  private String evaluate(ELVars variables, String name, String expression) throws ELEvalException {
    return evals.get(name).eval(variables, expression, String.class);
  }

  @Override
  public void write(Batch batch) throws StageException {
    final ELVars variables = getContext().createELVars();
    final FileSystem fs = hdfsConnection.getFs();

    Iterator<Record> it = batch.getRecords();
    while(it.hasNext()) {
      Record record = it.next();
      RecordEL.setRecordInContext(variables, record);

      // Execute all configured HDFS metadata operations as target user
      try {
        hdfsConnection.getUGI().doAs((PrivilegedExceptionAction<Void>) () -> {
          Path workingFile = new Path(evaluate(variables, "filePath", actions.filePath));
          LOG.info("Working on file: " + workingFile);

          // Create empty file if configured
          if(actions.taskType == TaskType.CREATE_EMPTY_FILE) {
            ensureDirectoryExists(fs, workingFile.getParent());
            if(!fs.createNewFile(workingFile)) {
              throw new IOException("Can't create file (probably already exists): " + workingFile);
            }
          }

          if(actions.taskType == TaskType.CHANGE_EXISTING_FILE && (actions.shouldMoveFile || actions.shouldRename)) {
            Path newPath = workingFile.getParent();
            String newName = workingFile.getName();
            if(actions.shouldMoveFile) {
              newPath = new Path(evaluate(variables, "newLocation", actions.newLocation));
            }
            if(actions.shouldRename) {
              newName = evaluate(variables, "newName", actions.newName);
            }

            Path destinationFile = new Path(newPath, newName);
            ensureDirectoryExists(fs, newPath);

            LOG.debug("Renaming to: {}", destinationFile);
            if(!fs.rename(workingFile, destinationFile)) {
              throw new IOException(Utils.format("Can't rename '{}' to '{}''", workingFile, destinationFile));
            }
            workingFile = destinationFile;
          }

          if(actions.taskType.isOneOf(TaskType.CHANGE_EXISTING_FILE, TaskType.CREATE_EMPTY_FILE)) {
            if (actions.shouldChangeOwnership) {
              String newOwner = evaluate(variables, "newOwner", actions.newOwner);
              String newGroup = evaluate(variables, "newGroup", actions.newGroup);
              LOG.debug("Applying ownership: user={} and group={}", newOwner, newGroup);
              fs.setOwner(workingFile, newOwner, newGroup);
            }

            if (actions.shouldSetPermissions) {
              String stringPerms = evaluate(variables, "newPermissions", actions.newPermissions);
              FsPermission fsPerms = HdfsUtils.parseFsPermission(stringPerms);
              LOG.debug("Applying permissions: {} loaded from value '{}'", fsPerms, stringPerms);
              fs.setPermission(workingFile, fsPerms);
            }

            if (actions.shouldSetAcls) {
              String stringAcls = evaluate(variables, "newAcls", actions.newAcls);
              List<AclEntry> acls = AclEntry.parseAclSpec(stringAcls, true);
              LOG.debug("Applying ACLs: {}", stringAcls);
              fs.setAcl(workingFile, acls);
            }
          }

          if(actions.taskType == TaskType.REMOVE_FILE) {
            fs.delete(workingFile, true);
          }

          // Issue event with the final file name (e.g. the renamed one if applicable)
          actions.taskType.getEventCreator().create(getContext())
            .with("filepath", workingFile.toString())
            .with("filename", workingFile.getName())
            .createAndSend();

          LOG.debug("Done changing metadata on file: {}", workingFile);
          return null;
        });
      } catch (Throwable e) {
        // Hadoop libraries will wrap any non InterruptedException, RuntimeException, Error or IOException to UndeclaredThrowableException,
        // so we manually unwrap it here and properly propagate it to user.
        if(e instanceof UndeclaredThrowableException) {
          e = e.getCause();
        }
        LOG.error("Failure when applying metadata changes to HDFS", e);
        errorRecordHandler.onError(new OnRecordErrorException(record, HdfsMetadataErrors.HDFS_METADATA_000, e.getMessage()));
      }
    }
  }

  /**
   * Ensure that given directory exists.
   *
   * Creates the directory if it doesn't exists. No-op if it does.
   */
  private void ensureDirectoryExists(FileSystem fs, Path path) throws IOException {
    if(!fs.exists(path)) {
      LOG.debug("Creating directory: {}", path);
      if(!fs.mkdirs(path)) {
        throw new IOException("Can't create directory: " + path);
      }
    }
  }

  @Override
  public void destroy() {
    hdfsConnection.destroy();
    actions.destroy();
  }

}
