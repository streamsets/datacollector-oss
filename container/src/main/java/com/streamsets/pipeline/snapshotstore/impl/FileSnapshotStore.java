/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.snapshotstore.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.io.DataStore;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.restapi.bean.SnapshotInfoJson;
import com.streamsets.pipeline.restapi.bean.SnapshotJson;
import com.streamsets.pipeline.restapi.bean.StageOutputJson;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.snapshotstore.Snapshot;
import com.streamsets.pipeline.snapshotstore.SnapshotInfo;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.snapshotstore.SnapshotStore;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class FileSnapshotStore implements SnapshotStore {

  private static final Logger LOG = LoggerFactory.getLogger(FileSnapshotStore.class);

  private static final String SNAPSHOT_FILE = "snapshot.json";
  private static final String INFO_FILE = "info.json";

  private final RuntimeInfo runtimeInfo;
  private boolean inProgress = false;
  private String inProgressPipelineName = "";
  private String inProgressSnapshotName = "";
  private final ObjectMapper json;

  public void setInProgress(String pipelineName, String rev, String snapshotName, boolean inProgress) {
    PipelineDirectoryUtil.createPipelineSnapshotDir(runtimeInfo, pipelineName, rev, snapshotName);
    this.inProgress = inProgress;
    this.inProgressPipelineName = pipelineName;
    this.inProgressSnapshotName = snapshotName;
  }

  public FileSnapshotStore(RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
    json = ObjectMapperFactory.get();
  }

  @Override
  public void storeSnapshot(String pipelineName, String rev, String snapshotName, List<StageOutput> snapshot) {
    SnapshotInfo info = new SnapshotInfo(pipelineName, snapshotName, new Date());
    try (OutputStream os = new DataStore(getPipelineSnapshotFile(pipelineName, rev, snapshotName)).getOutputStream()) {
      json.writeValue(os, new SnapshotJson(new Snapshot(snapshot)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try (OutputStream os =
      new DataStore(getPipelineSnapshotInfoFile(pipelineName, rev, snapshotName)).getOutputStream();) {
      json.writeValue(os, new SnapshotInfoJson(info));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    inProgress = false;
  }

  @Override
  public List<StageOutput> retrieveSnapshot(String pipelineName, String rev, String snapshotName) {
    if(!PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev).exists() ||
      !getPipelineSnapshotFile(pipelineName, rev, snapshotName).exists()) {
      return Collections.emptyList();
    }
    try (InputStream is = new DataStore(getPipelineSnapshotFile(pipelineName, rev, snapshotName)).getInputStream()) {
      List<StageOutputJson> snapshotJson = ObjectMapperFactory.get().readValue(is, SnapshotJson.class).getSnapshot();
      return BeanHelper.unwrapStageOutput(snapshotJson);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public SnapshotStatus getSnapshotStatus(String pipelineName, String rev, String snapshotName) {
    boolean snapshotFileExists = getPipelineSnapshotFile(pipelineName, rev, snapshotName).exists();
    return new SnapshotStatus(snapshotFileExists, inProgress);
  }

  @Override
  public void deleteSnapshot(String pipelineName, String rev, String snapshotName) {
    File snapshotBaseDir = PipelineDirectoryUtil.getPipelineSnapshotDir(runtimeInfo, pipelineName, rev, snapshotName);
    if(snapshotBaseDir.exists()) {
      if (!PipelineDirectoryUtil.deleteAll(snapshotBaseDir)) {
        throw new RuntimeException("Cannot delete snapshot");
      }
    }
  }

  @Override
  public InputStream getSnapshot(String pipelineName, String rev, String snapshotName) {
    if(getPipelineSnapshotFile(pipelineName, rev, snapshotName).exists()) {
      try {
        return new FileInputStream(getPipelineSnapshotFile(pipelineName, rev, snapshotName));
      } catch (FileNotFoundException e) {
        LOG.warn(e.getMessage());
        return null;
      }
    }
    return null;
  }

  @Override
  public List<SnapshotInfo> getSnapshotsInfo(String pipelineName, String rev) {
    List<SnapshotInfo> list = new ArrayList<>();
    File snapshotDir = PipelineDirectoryUtil.getPipelineSnapshotBaseDir(runtimeInfo, pipelineName, rev);
    if(snapshotDir.exists()) {
      for (String snapshotName : snapshotDir.list(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          //If one browses to the pipelines directory, mac creates a ".DS_store directory and this causes us problems
          //So filter it out
          return !name.startsWith(".");
        }
      })) {
        if(inProgress && inProgressPipelineName.equals(pipelineName) && inProgressSnapshotName.equals(snapshotName)) {
          list.add(new SnapshotInfo(pipelineName, snapshotName, null));
        } else {
          SnapshotInfo snapshotInfo = getInfo(pipelineName, rev, snapshotName);
          if(snapshotInfo != null) {
            list.add(snapshotInfo);
          }
        }
      }
    }
    return Collections.unmodifiableList(list);
  }

  private SnapshotInfo getInfo(String pipelineName, String rev, String name) {
    try {
      File infoFile = getPipelineSnapshotInfoFile(pipelineName, rev, name);
      if(infoFile.exists()) {
        SnapshotInfoJson snapshotInfoJsonBean =
          json.readValue(infoFile, SnapshotInfoJson.class);
        return snapshotInfoJsonBean.getSnapshotInfo();
      } else {
        return null;
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private File getPipelineSnapshotFile(String pipelineName, String rev, String snapshotName) {
    return new File(PipelineDirectoryUtil.getPipelineSnapshotDir(runtimeInfo, pipelineName, rev, snapshotName),
      SNAPSHOT_FILE);
  }

  private File getPipelineSnapshotInfoFile(String pipelineName, String rev, String snapshotName) {
    return new File(PipelineDirectoryUtil.getPipelineSnapshotDir(runtimeInfo, pipelineName, rev, snapshotName),
      INFO_FILE);
  }
}
