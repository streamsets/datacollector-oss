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
package com.streamsets.datacollector.updatechecker;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.DataCollectorBuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class UpdateChecker implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(UpdateChecker.class);

  public static final String URL_KEY = "streamsets.updatecheck.url";
  public static final String URL_DEFAULT = "https://streamsets.com/rest/v1/updatecheck";

  static final String APPLICATION_JSON_MIME = "application/json";

  private final RuntimeInfo runtimeInfo;
  private final Runner runner;
  private URL url = null;
  private volatile Map updateInfo;
  private final PipelineConfiguration pipelineConf;

  public UpdateChecker(RuntimeInfo runtimeInfo, Configuration configuration,
    PipelineConfiguration pipelineConf, Runner runner) {
    this.pipelineConf = pipelineConf;
    this.runtimeInfo = runtimeInfo;
    this.runner = runner;
    String url = configuration.get(URL_KEY, URL_DEFAULT);
    try {
      this.url = new URL(url);
    } catch (Exception ex) {
      LOG.trace("Invalid update check URL '{}': {}", url, ex.toString(), ex);
    }
  }

  URL getUrl() {
    return url;
  }

  static String getSha256(String id) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(id.getBytes("UTF-8"));
      return Base64.encodeBase64String(md.digest());
    } catch (Exception ex) {
      return "<UNKNOWN>";
    }
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  Map getUploadInfo() {
    Map uploadInfo = null;
    if (pipelineConf != null) {
      List stages = new ArrayList();

      // Stats aggregator target stage
      Map stage = new LinkedHashMap();
      if(pipelineConf.getStatsAggregatorStage() != null) {
        stage.put("name", pipelineConf.getStatsAggregatorStage().getStageName());
        stage.put("version", pipelineConf.getStatsAggregatorStage().getStageVersion());
        stage.put("library", pipelineConf.getStatsAggregatorStage().getLibrary());
        stages.add(stage);
      }

      // error stage
      stage = new LinkedHashMap();
      stage.put("name", pipelineConf.getErrorStage().getStageName());
      stage.put("version", pipelineConf.getErrorStage().getStageVersion());
      stage.put("library", pipelineConf.getErrorStage().getLibrary());
      stages.add(stage);

      // pipeline stages
      for (StageConfiguration stageConf : pipelineConf.getStages()) {
        stage = new LinkedHashMap();
        stage.put("name", stageConf.getStageName());
        stage.put("version", stageConf.getStageVersion());
        stage.put("library", stageConf.getLibrary());
        stages.add(stage);
      }

      uploadInfo = new LinkedHashMap();
      uploadInfo.put("sdc.sha256", getSha256(runner.getToken()));
      uploadInfo.put("sdc.buildInfo", new DataCollectorBuildInfo());
      uploadInfo.put("sdc.stages", stages);
    }

    return uploadInfo;
  }

  @Override
  public void run() {
    updateInfo = null;
    PipelineState ps;
    try {
      ps = runner.getState();
    } catch (PipelineStoreException e) {
      LOG.warn(Utils.format("Cannot get pipeline state: '{}'", e.toString()), e);
      return;
    }
      if (ps.getStatus() == PipelineStatus.RUNNING) {
        if (url != null) {
          Map uploadInfo = getUploadInfo();
          if (uploadInfo != null) {
            HttpURLConnection conn = null;
            try {
              conn = (HttpURLConnection) url.openConnection();
              conn.setConnectTimeout(2000);
              conn.setReadTimeout(2000);
              conn.setDoOutput(true);
              conn.setDoInput(true);
              conn.setRequestProperty("content-type", APPLICATION_JSON_MIME);
              try(OutputStream outputStream = conn.getOutputStream()) {
                ObjectMapperFactory.getOneLine().writeValue(outputStream, uploadInfo);
              }
              if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                String responseContentType = conn.getHeaderField("content-type");
                if (APPLICATION_JSON_MIME.equals(responseContentType)) {
                  try(InputStream inputStream = conn.getInputStream()) {
                    updateInfo = ObjectMapperFactory.get().readValue(inputStream, Map.class);
                  }
                } else {
                  LOG.trace("Got invalid content-type '{}' from from update-check server", responseContentType);
                }
              } else {
                LOG.trace("Got '{} : {}' from update-check server", conn.getResponseCode(), conn.getResponseMessage());
              }
            } catch (Exception ex) {
              LOG.trace("Could not do an update check: {}", ex.toString(), ex);
            } finally {
              if (conn != null) {
                conn.disconnect();
              }
            }
          }
        }
    }
  }

  public Map getUpdateInfo() {
    return updateInfo;
  }

}
