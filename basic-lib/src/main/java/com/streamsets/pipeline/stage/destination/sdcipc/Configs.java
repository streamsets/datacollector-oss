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
package com.streamsets.pipeline.stage.destination.sdcipc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.math.IntMath;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Configs {
  private static final Logger LOG = LoggerFactory.getLogger(Configs.class);
  private static final String CONFIG_PREFIX = "config.";
  private static final String HOST_PORTS = CONFIG_PREFIX + "hostPorts";
  private static final int MAX_BACKOFF_WAIT = 5 * 60 * 1000; // 5 minutes in milliseconds

  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfigBean = new TlsConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "SDC RPC Connection",
      description = "Connection information for the destination pipeline. Use the format <host>:<port>.",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "RPC"
  )
  public List<String> hostPorts;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "SDC RPC ID",
      description = "User-defined ID. Must match the SDC RPC ID used in the SDC RPC origin of the destination pipeline.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "RPC"
  )
  public CredentialValue appId;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Verify Host In Server Certificate",
      description = "Disables server certificate hostname verification",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "RPC",
      dependsOn = "tlsConfigBean.tlsEnabled",
      triggeredByValue = "true"
  )
  public boolean hostVerification;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "3",
      label = "Retries per Batch",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED",
      min = 0,
      max = Integer.MAX_VALUE
  )
  public int retriesPerBatch;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "30",
      label = "Back off period",
      description = "If set to non-zero, each retry will be spaced exponentially. For value 10, first retry will be" +
        " done after 10 milliseconds, second retry after additional 100 milliseconds, third retry after additional second, ..." +
        " The maximum wait time is 5 minutes.",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED",
      min=0
  )
  public int backOff;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5000",
      label = "Connection Timeout (ms)",
      displayPosition = 90,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED",
      min = 100
  )
  public int connectionTimeOutMs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2000",
      label = "Read Timeout (ms)",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED",
      min = 100
  )
  public int readTimeOutMs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Use Compression",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED"
  )
  public boolean compression;

  // This flag indicates that connection validation must apply the retry and backoff.
  boolean retryDuringValidation = false;

  private SSLSocketFactory sslSocketFactory;

  public List<Stage.ConfigIssue> init(Stage.Context context) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();

    boolean ok = validateHostPorts(context, issues);
    ok |= validateSecurity(context, issues);
    if (ok) {
      if (tlsConfigBean.isEnabled()) {
        sslSocketFactory = createSSLSocketFactory(context);
      }
      if (ok && !context.isPreview()) {
        List<Stage.ConfigIssue> moreIssues = new ArrayList<>();
        validateConnectivity(context, moreIssues);

        int retryCount = 0;
        while (!moreIssues.isEmpty() && retryDuringValidation && (retryCount < retriesPerBatch)) {
          backOffWait(retryCount);

          moreIssues.clear();
          validateConnectivity(context, moreIssues);
          retryCount++;
        }

        issues.addAll(moreIssues);
      }
    }
    return issues;
  }

  public void backOffWait(int retryCount) {
    // No wait if wait is disabled or this is first re-try
    if(retryCount <= 0 || backOff <= 0) {
      return;
    }

    // Wait time period
    int waitTime;

    // Current exponential back off
    try {
      waitTime = IntMath.checkedPow(backOff, retryCount);
    } catch (ArithmeticException e) {
      waitTime = MAX_BACKOFF_WAIT;
    }

    // Apply upper limit for the wait and finally wait
    waitTime = Math.min(waitTime, MAX_BACKOFF_WAIT);
    if (!ThreadUtil.sleep(waitTime)) {
      LOG.info("Backoff waiting was interrupted");
    }
  }

  boolean validateHostPorts(Stage.Context context, List<Stage.ConfigIssue> issues) {
    if (hostPorts.isEmpty()) {
      issues.add(context.createConfigIssue(Groups.RPC.name(), HOST_PORTS, Errors.IPC_DEST_00));
      return false;
    }
    Set<String> uniqueHostPorts = new HashSet<>();
    for (String rawHostPort : hostPorts) {
      if (rawHostPort == null) {
        issues.add(context.createConfigIssue(Groups.RPC.name(), HOST_PORTS, Errors.IPC_DEST_01));
        return false;
      }
      final String hostPort = rawHostPort.toLowerCase().trim();
      uniqueHostPorts.add(hostPort);
      try {
        InetAddress.getByName(getHost(hostPort));
      } catch (UnknownHostException e) {
        LOG.error(Errors.IPC_DEST_02.getMessage(), hostPort, e.toString(), e);
        issues.add(context.createConfigIssue(Groups.RPC.name(), HOST_PORTS, Errors.IPC_DEST_02, hostPort));
        return false;
      }
      try {
        int port = getPort(hostPort);
        if (port < 1 || port > 65535) {
          issues.add(
              context.createConfigIssue(Groups.RPC.name(), HOST_PORTS, Errors.IPC_DEST_04, hostPort)
          );
          return false;
        }
      } catch (IllegalArgumentException e) {
        LOG.error(Errors.IPC_DEST_05.getMessage(), hostPort, e.toString(), e);
        issues.add(
            context.createConfigIssue(Groups.RPC.name(), HOST_PORTS, Errors.IPC_DEST_05, hostPort, e.toString())
        );
        return false;
      }
    }
    if (uniqueHostPorts.size() != hostPorts.size()) {
      issues.add(context.createConfigIssue(Groups.RPC.name(), HOST_PORTS, Errors.IPC_DEST_06));
      return false;
    }
    return true;
  }

  private String getHost(String hostPort) {
    if (hostPort.contains("[") && hostPort.contains("]")) {
      return hostPort.substring(hostPort.indexOf('[') + 1, hostPort.indexOf(']'));
    }

    // If there is more than one ':' and we didn't find [] then this isn't a valid IPv6 hostPort
    if (hostPort.contains(":") && (hostPort.indexOf(':') == hostPort.lastIndexOf(':'))) {
      return hostPort.substring(0, hostPort.lastIndexOf(':'));
    } else {
      return hostPort;
    }
  }

  private int getPort(String hostPort) {
    if (!hostPort.contains(":")) {
      throw new IllegalArgumentException("Destination does not include ':' port delimiter");
    }

    int port;
    try {
      port = Integer.parseInt(hostPort.substring(hostPort.lastIndexOf(':') + 1));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(e);
    }

    return port;
  }

  boolean validateSecurity(Stage.Context context, List<Stage.ConfigIssue> issues) {
    boolean ok = true;
    if (tlsConfigBean.isEnabled()) {
      ok &= tlsConfigBean.init(context, "TLS", "tlsConfigBean.", issues);
    }
    return ok;
  }

  SSLSocketFactory createSSLSocketFactory(Stage.Context context) {
    return tlsConfigBean.getSslContext().getSocketFactory();
  }

  HttpURLConnection createConnection(URL url) throws IOException {
    return (HttpURLConnection) url.openConnection();
  }

  static final HostnameVerifier ACCEPT_ALL_HOSTNAME_VERIFIER = (s, sslSession) -> true;

  @VisibleForTesting
  public HttpURLConnection createConnection(String hostPort) throws IOException, StageException {
    return createConnection(hostPort, Constants.IPC_PATH);
  }

  @VisibleForTesting
  public HttpURLConnection createConnection(String hostPort, String path) throws IOException, StageException {
    String scheme = (tlsConfigBean.isEnabled()) ? "https://" : "http://";
    URL url = new URL(scheme + hostPort.trim()  + path);
    HttpURLConnection conn = createConnection(url);
    conn.setConnectTimeout(connectionTimeOutMs);
    conn.setReadTimeout(readTimeOutMs);
    if (tlsConfigBean.isEnabled()) {
      HttpsURLConnection sslConn = (HttpsURLConnection) conn;
      sslConn.setSSLSocketFactory(sslSocketFactory);
      if (!hostVerification) {
        sslConn.setHostnameVerifier(ACCEPT_ALL_HOSTNAME_VERIFIER);
      }
    }
    conn.setRequestProperty(Constants.X_SDC_APPLICATION_ID_HEADER, appId.get());
    return conn;
  }

  void validateConnectivity(Stage.Context context, List<Stage.ConfigIssue> issues) {
    boolean ok = false;
    List<String> errors = new ArrayList<>();
    for (String hostPort : hostPorts) {
      try {
        HttpURLConnection conn = createConnection(hostPort);
        conn.setRequestMethod("GET");
        conn.setDefaultUseCaches(false);
        if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
          if (Constants.X_SDC_PING_VALUE.equals(conn.getHeaderField(Constants.X_SDC_PING_HEADER))) {
            ok = true;
          } else {
            issues.add(context.createConfigIssue(Groups.RPC.name(), HOST_PORTS,
                                                 Errors.IPC_DEST_12, hostPort ));
          }
        } else {
          errors.add(Utils.format("'{}': {}", hostPort, conn.getResponseMessage()));
        }
      } catch (Exception ex) {
        errors.add(Utils.format("'{}': {}", hostPort, ex.toString()));
      }
    }
    if (!ok) {
      issues.add(context.createConfigIssue(null, null, Errors.IPC_DEST_15, errors));
    }
  }

}
