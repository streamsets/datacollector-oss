/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.updatechecker;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.dc.execution.PipelineState;
import com.streamsets.dc.execution.PipelineStatus;
import com.streamsets.dc.execution.Runner;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.BuildInfo;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.Configuration;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
      LOG.trace("Invalid update check URL '{}': {}", url, ex.getMessage(), ex);
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

      // error stage
      Map stage = new LinkedHashMap();
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
      uploadInfo.put("sdc.sha256", getSha256(runtimeInfo.getSDCToken()));
      uploadInfo.put("sdc.buildInfo", new BuildInfo());
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
      LOG.warn(Utils.format("Cannot get pipeline state: '{}'", e.getMessage()), e);
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
              ObjectMapperFactory.getOneLine().writeValue(conn.getOutputStream(), uploadInfo);
              if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                String responseContentType = conn.getHeaderField("content-type");
                if (APPLICATION_JSON_MIME.equals(responseContentType)) {
                  updateInfo = ObjectMapperFactory.get().readValue(conn.getInputStream(), Map.class);
                } else {
                  LOG.trace("Got invalid content-type '{}' from from update-check server", responseContentType);
                }
              } else {
                LOG.trace("Got '{} : {}' from update-check server", conn.getResponseCode(), conn.getResponseMessage());
              }
            } catch (Exception ex) {
              LOG.trace("Could not do an update check: {}", ex.getMessage(), ex);
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
