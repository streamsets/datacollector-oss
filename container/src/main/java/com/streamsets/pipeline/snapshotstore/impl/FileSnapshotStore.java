/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.snapshotstore.impl;

import com.streamsets.pipeline.io.DataStore;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.restapi.bean.SnapshotJson;
import com.streamsets.pipeline.restapi.bean.StageOutputJson;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.snapshotstore.Snapshot;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.snapshotstore.SnapshotStore;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

public class FileSnapshotStore implements SnapshotStore {

  private static final Logger LOG = LoggerFactory.getLogger(FileSnapshotStore.class);

  private static final String TEMP_SNAPSHOT_FILE = "snapshot.json.tmp";
  private static final String SNAPSHOT_FILE = "snapshot.json";

  private final RuntimeInfo runtimeInfo;

  public FileSnapshotStore(RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
  }

  public void storeSnapshot(String pipelineName, String rev, List<StageOutput> snapshot) {
    try {
      ObjectMapperFactory.get().writeValue(new DataStore(getPipelineSnapshotFile(pipelineName, rev)).getOutputStream(),
        new SnapshotJson(new Snapshot(snapshot)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  public List<StageOutput> retrieveSnapshot(String pipelineName, String rev) {
    if(!PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev).exists() ||
      !getPipelineSnapshotFile(pipelineName, rev).exists()) {
      return Collections.emptyList();
    }
    try {
      List<StageOutputJson> snapshotJson = ObjectMapperFactory.get().readValue(
        new DataStore(getPipelineSnapshotFile(pipelineName, rev)).getInputStream(), SnapshotJson.class).getSnapshot();
      return BeanHelper.unwrapStageOutput(snapshotJson);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public SnapshotStatus getSnapshotStatus(String pipelineName, String rev) {
    boolean snapshotFileExists = getPipelineSnapshotFile(pipelineName, rev).exists();
    boolean snapshotStageFileExists = getPipelineSnapshotTempFile(pipelineName, rev).exists();
    return new SnapshotStatus(snapshotFileExists, snapshotStageFileExists);
  }

  @Override
  public void deleteSnapshot(String pipelineName, String rev) {
    if(getPipelineSnapshotFile(pipelineName, rev).exists()) {
      getPipelineSnapshotFile(pipelineName, rev).delete();
    }
  }

  @Override
  public InputStream getSnapshot(String pipelineName, String rev) {
    if(getPipelineSnapshotFile(pipelineName, rev).exists()) {
      try {
        return new FileInputStream(getPipelineSnapshotFile(pipelineName, rev));
      } catch (FileNotFoundException e) {
        LOG.warn(e.getMessage());
        return null;
      }
    }
    return null;
  }

  private File getPipelineSnapshotFile(String pipelineName, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev), SNAPSHOT_FILE);
  }

  private File getPipelineSnapshotTempFile(String pipelineName, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev), TEMP_SNAPSHOT_FILE);
  }

}
