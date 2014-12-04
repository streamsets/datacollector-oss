/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.snapshotstore.impl;

import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.snapshotstore.Snapshot;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.snapshotstore.SnapshotStore;
import com.streamsets.pipeline.util.JsonFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.List;

public class FileSnapshotStore implements SnapshotStore {

  private static final Logger LOG = LoggerFactory.getLogger(FileSnapshotStore.class);

  private static final String TEMP_SNAPSHOT_FILE = "snapshot.json.tmp";
  private static final String SNAPSHOT_FILE = "snapshot.json";
  private static final String SNAPSHOT_DIR = "runInfo";

  private File snapshotBaseDir;
  private JsonFileUtil<Snapshot> snapShotPersister = new JsonFileUtil<>();

  public FileSnapshotStore(RuntimeInfo runtimeInfo) {
    this.snapshotBaseDir = new File(runtimeInfo.getDataDir(), SNAPSHOT_DIR);
    snapShotPersister = new JsonFileUtil<>();
  }

  public void storeSnapshot(String pipelineName, List<StageOutput> snapshot) {
    try {
      snapShotPersister.writeObjectToFile(getPipelineSnapshotTempFile(pipelineName),
          getPipelineSnapshotFile(pipelineName), new Snapshot(snapshot));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  public List<StageOutput> retrieveSnapshot(String pipelineName) {
    if(!getPipelineDir(pipelineName).exists() || !getPipelineSnapshotFile(pipelineName).exists()) {
      return Collections.EMPTY_LIST;
    }
    try {
      return snapShotPersister.readObjectFromFile(getPipelineSnapshotFile(pipelineName), Snapshot.class).getSnapshot();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public SnapshotStatus getSnapshotStatus(String pipelineName) {
    boolean snapshotFileExists = getPipelineSnapshotFile(pipelineName).exists();
    boolean snapshotStageFileExists = getPipelineSnapshotTempFile(pipelineName).exists();
    return new SnapshotStatus(snapshotFileExists, snapshotStageFileExists);
  }

  @Override
  public void deleteSnapshot(String pipelineName) {
    if(getPipelineSnapshotFile(pipelineName).exists()) {
      getPipelineSnapshotFile(pipelineName).delete();
    }
  }

  @Override
  public InputStream getSnapshot(String pipelineName) {
    if(getPipelineSnapshotFile(pipelineName).exists()) {
      try {
        return new FileInputStream(getPipelineSnapshotFile(pipelineName));
      } catch (FileNotFoundException e) {
        LOG.warn(e.getMessage());
        return null;
      }
    }
    return null;
  }

  private File getPipelineSnapshotFile(String name) {
    return new File(getPipelineDir(name), SNAPSHOT_FILE);
  }

  private File getPipelineSnapshotTempFile(String name) {
    return new File(getPipelineDir(name), TEMP_SNAPSHOT_FILE);
  }

  private File getPipelineDir(String name) {
    File pipelineDir = new File(snapshotBaseDir, name);
    if(!pipelineDir.exists()) {
      if(!pipelineDir.mkdirs()) {
        throw new RuntimeException(Utils.format("Could not create directory '{}'", pipelineDir.getAbsolutePath()));
      }
    }
    return pipelineDir;
  }

}
