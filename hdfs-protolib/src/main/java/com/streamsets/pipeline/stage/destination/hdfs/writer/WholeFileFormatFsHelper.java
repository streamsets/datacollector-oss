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
package com.streamsets.pipeline.stage.destination.hdfs.writer;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.event.WholeFileProcessedEvent;
import com.streamsets.pipeline.lib.generator.StreamCloseEventHandler;
import com.streamsets.pipeline.lib.io.fileref.FileRefStreamCloseEventHandler;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.lib.hdfs.common.Errors;
import com.streamsets.pipeline.stage.destination.hdfs.util.HdfsUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;

final class WholeFileFormatFsHelper implements FsHelper {
  private static final Logger LOG = LoggerFactory.getLogger(WholeFileFormatFsHelper.class);

  private final Target.Context context;
  private final String fileNameEL;
  private final WholeFileExistsAction wholeFileAlreadyExistsAction;
  private final String permissionEL;
  private final String uniquePrefix;
  private final RecordWriterManager mgr;

  private FsPermission fsPermissions;
  private EventRecord wholeFileEventRecord;


  WholeFileFormatFsHelper(
      Target.Context context,
      String fileNameEL,
      WholeFileExistsAction wholeFileAlreadyExistsAction,
      String permissionEL,
      String uniquePrefix,
      RecordWriterManager mgr
  ) {
    this.context = context;
    this.fileNameEL = fileNameEL;
    this.wholeFileAlreadyExistsAction = wholeFileAlreadyExistsAction;
    this.permissionEL = permissionEL;
    this.uniquePrefix = uniquePrefix;
    this.mgr = mgr;
    fsPermissions = null;
  }

  private String getTempFile(Date recordDate, Record record) throws StageException {
    ELEval elEval = context.createELEval("fileNameEL");
    ELVars vars = context.createELVars();
    RecordEL.setRecordInContext(vars, record);
    TimeNowEL.setTimeNowInContext(vars, recordDate);
    String fileName = elEval.eval(vars, fileNameEL, String.class);
    return RecordWriterManager.TMP_FILE_PREFIX + uniquePrefix + fileName;
  }

  private void checkAndHandleWholeFileExistence(FileSystem fs, Path renamableFinalPath) throws IOException, OnRecordErrorException {
    if (fs.exists(renamableFinalPath)) {
      //whole file exists.
      if (wholeFileAlreadyExistsAction == WholeFileExistsAction.OVERWRITE) {
        fs.delete(renamableFinalPath, false);
        LOG.debug(Utils.format(Errors.HADOOPFS_54.getMessage(), renamableFinalPath) + "so deleting it");
      } else {
        throw new OnRecordErrorException(Errors.HADOOPFS_54, renamableFinalPath);
      }
    }
  }

  private void updateFsPermissionsIfNeeded(Record record) throws StageException {
    if (permissionEL !=null && !permissionEL.isEmpty()) {
      ELEval elEval = context.createELEval("permissionEL");
      ELVars vars = context.createELVars();
      RecordEL.setRecordInContext(vars, record);
      String permissions = null;
      try {
        permissions = elEval.eval(vars, permissionEL, String.class);
      } catch (ELEvalException e) {
        LOG.error("El evaluation error:", e);
        throw new OnRecordErrorException(Errors.HADOOPFS_55, permissionEL);
      }
      try {
        fsPermissions = HdfsUtils.parseFsPermission(permissions);
      } catch (IllegalArgumentException e) {
        LOG.error("Can't parse the permission value string:", e);
        throw new OnRecordErrorException(Errors.HADOOPFS_56, permissions);
      }
    }
  }

  //This is going to be done only once per record, so skipping cache save
  //because the path is going to be used only once (because only one record is used).
  @Override
  public Path getPath(FileSystem fs, Date recordDate, Record record) throws StageException, IOException {
    //Check whether the real file already exists
    Path path = new Path(mgr.getDirPath(recordDate, record), getTempFile(recordDate, record));
    //this will check the file exists.
    Path renamableFinalPath = getRenamablePath(fs, path);
    updateFsPermissionsIfNeeded(record);

    wholeFileEventRecord = createWholeFileEventRecord(record, renamableFinalPath);

    return path;
  }

  private EventRecord createWholeFileEventRecord(Record record, Path renamableFinalPath) throws StageException {
    try {
      FileRefUtil.validateWholeFileRecord(record);
    } catch (IllegalArgumentException e) {
      throw new OnRecordErrorException(record, Errors.HADOOPFS_14, e);
    }
    //Update the event record with source file info information
    return WholeFileProcessedEvent.FILE_TRANSFER_COMPLETE_EVENT
        .create(context)
        .with(WholeFileProcessedEvent.SOURCE_FILE_INFO, record.get(FileRefUtil.FILE_INFO_FIELD_PATH).getValueAsMap())
        .withStringMap(WholeFileProcessedEvent.TARGET_FILE_INFO, ImmutableMap.of("path", (Object) renamableFinalPath))
        .create();
  }

  @Override
  public void commitOldFiles(FileSystem fs) throws ELEvalException, IOException {
    //Nothing to do, we will simply overwrite the tmp files with new file.
    //As a performance feature, we could have marker files which tells
    //whether the file is completely copied and rename failed on previous
    //run (but there is a problem our op to delete the marker file may still fail)
  }

  @Override
  public void handleAlreadyExistingFile(FileSystem fs, Path tempPath) {
    //NOOP createWriter will overwrite existing tmp file
  }

  @Override
  public Path renameAndGetPath(FileSystem fs, Path tempPath) throws IOException, StageException {
    Path finalPath = getRenamablePath(fs, tempPath);
    if (!fs.rename(tempPath, finalPath)) {
      throw new IOException(Utils.format("Could not rename '{}' to '{}'", tempPath, finalPath));
    }

    //updatePermissions
    if (fsPermissions != null) {
      fs.setPermission(finalPath, fsPermissions);
    }

    fsPermissions = null;

    //Throw file copied event here.
    context.toEvent(wholeFileEventRecord);

    return finalPath;
  }

  private Path getRenamablePath(FileSystem fs, Path tempPath) throws IOException, OnRecordErrorException {
    String finalFileName = tempPath.getName().replaceFirst(RecordWriterManager.TMP_FILE_PREFIX, "");
    Path finalPath = new Path(tempPath.getParent(), finalFileName);
    //Checks during rename.
    checkAndHandleWholeFileExistence(fs, finalPath);
    return finalPath;
  }

  @Override
  public OutputStream create(FileSystem fs, Path path) throws IOException {
    //Make sure if the tmp file already exists, overwrite it
    return fs.create(path, true);
  }

  @Override
  public StreamCloseEventHandler<?> getStreamCloseEventHandler() {
    return new FileRefStreamCloseEventHandler(wholeFileEventRecord);
  }
}
