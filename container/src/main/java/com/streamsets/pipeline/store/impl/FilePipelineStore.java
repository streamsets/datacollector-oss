/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.store.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.agent.RuntimeInfo;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.container.Configuration;
import com.streamsets.pipeline.store.*;
import com.streamsets.pipeline.validation.Issues;

import javax.inject.Inject;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class FilePipelineStore implements PipelineStore {
  public static final String CREATE_DEFAULT_PIPELINE_KEY = "create.default.pipeline";
  public static final boolean CREATE_DEFAULT_PIPELINE_DEFAULT = true;

  @VisibleForTesting
  static final String DEFAULT_PIPELINE_NAME = "xyz";

  @VisibleForTesting
  static final String DEFAULT_PIPELINE_DESCRIPTION = "Default Pipeline";

  @VisibleForTesting
  static final String SYSTEM_USER = "system";

  @VisibleForTesting
  static final String REV = "0";

  private static final String INFO_FILE = "info.json";
  private static final String PIPELINE_FILE = "pipeline.json";

  private final RuntimeInfo runtimeInfo;
  private final Configuration conf;
  private File storeDir;
  private ObjectMapper json;

  @Inject
  public FilePipelineStore(RuntimeInfo runtimeInfo, Configuration conf) {
    this.runtimeInfo = runtimeInfo;
    this.conf = conf;
    json = new ObjectMapper();
    json.enable(SerializationFeature.INDENT_OUTPUT);
  }

  @VisibleForTesting
  File getStoreDir() {
    return storeDir;
  }

  @Override
  public void init()  {
    storeDir = new File(runtimeInfo.getDataDir(), "pipelines");
    if (!storeDir.exists()) {
      if (!storeDir.mkdirs()) {
        throw new RuntimeException(String.format("Could not create directory '%s'", storeDir.getAbsolutePath()));
      }
    }
    if (conf.get(CREATE_DEFAULT_PIPELINE_KEY, CREATE_DEFAULT_PIPELINE_DEFAULT)) {
      if (!doesPipelineExist(DEFAULT_PIPELINE_NAME)) {
        try {
          create(DEFAULT_PIPELINE_NAME, DEFAULT_PIPELINE_DESCRIPTION, SYSTEM_USER);
        } catch (PipelineStoreException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
  }

  @Override
  public void destroy() {
  }

  private File getPipelineDir(String name) {
    return new File(storeDir, name);
  }

  @VisibleForTesting
  File getInfoFile(String name) {
    return new File(getPipelineDir(name), INFO_FILE);
  }

  private File getPipelineFile(String name) {
    return new File(getPipelineDir(name), PIPELINE_FILE);
  }

  private boolean doesPipelineExist(String name) {
    return getPipelineDir(name).exists();
  }

  @Override
  public PipelineConfiguration create(String name, String description, String user) throws PipelineStoreException {
    if (doesPipelineExist(name)) {
      throw new PipelineStoreException(PipelineStoreErrors.PIPELINE_ALREADY_EXISTS, name);
    }
    if (!getPipelineDir(name).mkdir()) {
      throw new PipelineStoreException(PipelineStoreErrors.COULD_NOT_CREATE_PIPELINE, name,
                                       String.format("'%s' mkdir failed", getPipelineDir(name)));
    }
    Date date = new Date();
    UUID uuid = UUID.randomUUID();
    PipelineInfo info = new PipelineInfo(name, description, date, date, user, user, REV, uuid, false);

    List<ConfigConfiguration> configuration = new ArrayList<ConfigConfiguration>(2);
    configuration.add(new ConfigConfiguration("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    configuration.add(new ConfigConfiguration("stopPipelineOnError", false));
    PipelineConfiguration pipeline = new PipelineConfiguration(uuid, configuration, null,
      null);
    try {
      json.writeValue(getInfoFile(name), info);
      json.writeValue(getPipelineFile(name), pipeline);
    } catch (Exception ex) {
      throw new PipelineStoreException(PipelineStoreErrors.COULD_NOT_CREATE_PIPELINE, name, ex.getMessage(),
                                       ex);
    }
    return pipeline;
  }

  private boolean deleteAll(File path) {
    boolean ok = true;
    File[] children = path.listFiles();
    if (children != null) {
      for (File child : children) {
        ok = deleteAll(child);
        if (!ok) {
          break;
        }
      }
    }
    return ok && path.delete();
  }

  private boolean cleanUp(String name) throws PipelineStoreException {
    return deleteAll(getPipelineDir(name));
  }

  @Override
  public void delete(String name) throws PipelineStoreException {
    if (!doesPipelineExist(name)) {
      throw new PipelineStoreException(PipelineStoreErrors.PIPELINE_DOES_NOT_EXIST, name);
    }
    if (!cleanUp(name)) {
      throw new PipelineStoreException(PipelineStoreErrors.COULD_NOT_DELETE_PIPELINE, name);
    }
    cleanUp(name);
  }

  private PipelineInfo getInfo(String name, boolean checkExistence) throws PipelineStoreException {
    if (checkExistence && !doesPipelineExist(name)) {
      throw new PipelineStoreException(PipelineStoreErrors.PIPELINE_DOES_NOT_EXIST, name);
    }
    try {
      return json.readValue(getInfoFile(name), PipelineInfo.class);
    } catch (Exception ex) {
      throw new PipelineStoreException(PipelineStoreErrors.COULD_NOT_LOAD_PIPELINE_INFO, name);
    }
  }

  @Override
  public List<PipelineInfo> getPipelines() throws PipelineStoreException {
    List<PipelineInfo> list = new ArrayList<PipelineInfo>();
    for (String name : storeDir.list()) {
      list.add(getInfo(name, false));
    }
    return Collections.unmodifiableList(list);
  }

  @Override
  public PipelineInfo getInfo(String name) throws PipelineStoreException {
    return getInfo(name, true);
  }

  @Override
  public PipelineConfiguration save(String name, String user, String tag, String tagDescription,
      PipelineConfiguration pipeline) throws PipelineStoreException {
    if (!doesPipelineExist(name)) {
      throw new PipelineStoreException(PipelineStoreErrors.PIPELINE_DOES_NOT_EXIST, name);
    }
    PipelineInfo savedInfo = getInfo(name, false);
    if (!savedInfo.getUuid().equals(pipeline.getUuid())) {
      throw new PipelineStoreException(PipelineStoreErrors.INVALID_UUID_FOR_PIPELINE, name);
    }
    UUID uuid = UUID.randomUUID();
    PipelineInfo info = new PipelineInfo(getInfo(name, false), new Date(), user, REV, uuid,
                                         pipeline.isValid());
    try {
      pipeline.setUuid(uuid);
      json.writeValue(getInfoFile(name), info);
      json.writeValue(getPipelineFile(name), pipeline);
    } catch (Exception ex) {
      throw new PipelineStoreException(PipelineStoreErrors.COULD_NOT_SAVE_PIPELINE, name, ex.getMessage(), ex);
    }
    return pipeline;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<PipelineRevInfo> getHistory(String name) throws PipelineStoreException {
    return ImmutableList.of(new PipelineRevInfo(getInfo(name, true)));
  }

  @Override
  public PipelineConfiguration load(String name, String tagOrRev) throws PipelineStoreException {
    if (!doesPipelineExist(name)) {
      throw new PipelineStoreException(PipelineStoreErrors.PIPELINE_DOES_NOT_EXIST, name);
    }
    try {
      return json.readValue(getPipelineFile(name), PipelineConfiguration.class);
    } catch (Exception ex) {
      throw new PipelineStoreException(PipelineStoreErrors.COULD_NOT_LOAD_PIPELINE_INFO, name, ex.getMessage(), ex);
    }
  }

}
