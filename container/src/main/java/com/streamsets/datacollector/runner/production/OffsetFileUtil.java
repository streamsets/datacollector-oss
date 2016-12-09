/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.runner.production;

import com.google.api.client.util.Charsets;
import com.streamsets.datacollector.execution.store.CuratorFrameworkConnector;
import com.streamsets.datacollector.execution.store.FatalZKStoreException;
import com.streamsets.datacollector.execution.store.NotPrimaryException;
import com.streamsets.datacollector.execution.store.ZKStoreException;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.SourceOffsetJson;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;
import com.streamsets.pipeline.api.ext.DataCollectorServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class OffsetFileUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ProductionSourceOffsetTracker.class);

  private static final String OFFSET_FILE = "offset.json";
  private static final String DEFAULT_OFFSET = null;

  private OffsetFileUtil() {}

  public static File getPipelineOffsetFile(RuntimeInfo runtimeInfo, String pipelineName, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev), OFFSET_FILE);
  }

  public static String saveIfEmpty(RuntimeInfo runtimeInfo, String pipelineName, String rev) {
//    File pipelineOffsetFile =  getPipelineOffsetFile(runtimeInfo, pipelineName, rev);
//    SourceOffset sourceOffset;
//    DataStore ds = new DataStore(pipelineOffsetFile);
//    try {
//      if (ds.exists()) {
//        // offset file exists, read from it
//        try (InputStream is = ds.getInputStream()) {
//          SourceOffsetJson sourceOffsetJson = ObjectMapperFactory.get().readValue(is, SourceOffsetJson.class);
//          sourceOffset = BeanHelper.unwrapSourceOffset(sourceOffsetJson);
//        }
//      } else {
//        sourceOffset = new SourceOffset(DEFAULT_OFFSET);
//        try (OutputStream os = ds.getOutputStream()) {
//          ObjectMapperFactory.get().writeValue((os), BeanHelper.wrapSourceOffset(sourceOffset));
//          ds.commit(os);
//        } finally {
//          ds.release();
//        }
//      }
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }
    CuratorFrameworkConnector curator = DataCollectorServices.instance().get(CuratorFrameworkConnector.SERVICE_NAME);
    try {
      String offset = curator.getOffset(pipelineName);
      if (offset != null) {
        SourceOffsetJson json = ObjectMapperFactory.get().readValue(new ByteArrayInputStream(offset.getBytes(Charsets.UTF_8)), SourceOffsetJson.class);
        return BeanHelper.unwrapSourceOffset(json).getOffset();
      } else {
        SourceOffset sourceOffset = new SourceOffset(DEFAULT_OFFSET);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ObjectMapperFactory.get().writeValue(os, BeanHelper.wrapSourceOffset(sourceOffset));
        curator.commitOffset(pipelineName, new String(os.toByteArray(), Charsets.UTF_8));
        return sourceOffset.getOffset();
      }
    } catch (Exception ex){
      throw new RuntimeException(ex);

    }
  }

  public static void saveOffset(RuntimeInfo runtimeInfo, String pipelineName, String rev, String offset) {
    LOG.debug("Saving offset {} for pipeline {}", offset, pipelineName);
    SourceOffset sourceOffset = new SourceOffset(offset);
    CuratorFrameworkConnector curator = DataCollectorServices.instance().get(CuratorFrameworkConnector.SERVICE_NAME);
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      ObjectMapperFactory.get().writeValue((os), BeanHelper.wrapSourceOffset(sourceOffset));
      curator.commitOffset(pipelineName, new String(os.toByteArray(), Charsets.UTF_8));
    } catch (IOException e) {
      LOG.error("Failed to save offset value {}. Reason {}", sourceOffset.getOffset(), e.toString(), e);
      throw new RuntimeException(e);
    } catch (NotPrimaryException ex) {
      LOG.error("Not primary, cannot save FO", ex);
      throw new RuntimeException(ex);
    }
  }

  public static void resetOffset(RuntimeInfo runtimeInfo, String pipelineName, String rev) {
    saveOffset(runtimeInfo, pipelineName, rev, DEFAULT_OFFSET);
  }

  public static String getOffset(RuntimeInfo runtimeInfo, String pipelineName, String rev) {
//    File pipelineOffsetFile =  getPipelineOffsetFile(runtimeInfo, pipelineName, rev);
//    String offset = null;
//    if (pipelineOffsetFile.exists()) {
//      DataStore ds = new DataStore(pipelineOffsetFile);
//      try {
//        if (ds.exists()) {
//          // offset file exists, read from it
//          try (InputStream is = ds.getInputStream()) {
//            SourceOffsetJson sourceOffsetJson = ObjectMapperFactory.get().readValue(is, SourceOffsetJson.class);
//            offset = BeanHelper.unwrapSourceOffset(sourceOffsetJson).getOffset();
//          } catch (IOException e) {
//            throw new RuntimeException(e);
//          }
//        }
//      } catch (IOException e) {
//        throw new RuntimeException(e);
//      }
//    }
    CuratorFrameworkConnector curator = DataCollectorServices.instance().get(CuratorFrameworkConnector.SERVICE_NAME);
    try {
      ByteArrayInputStream is = new ByteArrayInputStream(curator.getOffset(pipelineName).getBytes(Charsets.UTF_8));
      SourceOffsetJson json = ObjectMapperFactory.get().readValue(is, SourceOffsetJson.class);
      return BeanHelper.unwrapSourceOffset(json).getOffset();
    } catch (ZKStoreException | IOException ex) {
      LOG.error("Error while reading offset", ex);
      throw new FatalZKStoreException(ex);
    }
  }

  public static long getLastModified(String pipelineName) {
    CuratorFrameworkConnector curator = DataCollectorServices.instance().get(CuratorFrameworkConnector.SERVICE_NAME);
    return curator.getLastBatchTime(pipelineName);
  }
}

