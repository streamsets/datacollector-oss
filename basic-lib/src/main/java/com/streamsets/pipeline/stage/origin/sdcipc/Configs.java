/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.sdcipc;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;

import javax.net.ssl.SSLSocketFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;

public class Configs {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "20000",
      label = "RPC Listening Port",
      displayPosition = 10,
      group = "",
      min = 1,
      max = 65535
  )
  public int port;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "RPC ID",
      description = "User-defined ID. Must match the RPC ID used in the RPC destination of the origin pipeline.",
      displayPosition = 20,
      group = ""
  )
  public String appId;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5",
      label = "Batch Wait Time (secs)",
      description = " Maximum amount of time to wait for a batch before sending and empty one",
      displayPosition = 30,
      group = "",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxWaitTimeSecs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "SSL Enabled",
      displayPosition = 40,
      group = ""
  )
  public boolean sslEnabled;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Certificate Key Store File",
      description = "The KeyStore file is looked under the data collector resources directory",
      displayPosition = 50,
      group = "",
      dependsOn = "sslEnabled",
      triggeredByValue = "true"
  )
  public String keyStoreFile;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Key Store Password",
      displayPosition = 60,
      group = "",
      dependsOn = "sslEnabled",
      triggeredByValue = "true"
  )
  public String keyStorePassword;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "10",
      label = "Max Record Size (MB)",
      description = "",
      displayPosition = 10,
      group = "ADVANCED",
      min = 1,
      max = 100
  )
  public int maxRecordSize;

  public List<Stage.ConfigIssue> init(Stage.Context context) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();

    validatePort(context, issues);
    validateSecurity(context, issues);
    return issues;
  }

  void validatePort(Stage.Context context, List<Stage.ConfigIssue> issues) {
    if (port < 1 || port > 65535) {
      issues.add(context.createConfigIssue("", "port", Errors.IPC_ORIG_00));

    } else {
      try (ServerSocket ss = new ServerSocket(port)){
      } catch (Exception ex) {
        issues.add(context.createConfigIssue("", "port", Errors.IPC_ORIG_01, ex.toString()));

      }
    }
  }

  void validateSecurity(Stage.Context context, List<Stage.ConfigIssue> issues) {
    if (sslEnabled) {
      if (!keyStoreFile.isEmpty()) {
        File file = getKeyStoreFile(context);
        if (!file.exists()) {
          issues.add(context.createConfigIssue("", "keyStoreFile", Errors.IPC_ORIG_07));
        } else {
          if (!file.isFile()) {
            issues.add(context.createConfigIssue("", "keyStoreFile", Errors.IPC_ORIG_08));
          } else {
            if (!file.canRead()) {
              issues.add(context.createConfigIssue("", "keyStoreFile", Errors.IPC_ORIG_09));
            } else {
              try {
                KeyStore keystore = KeyStore.getInstance("jks");
                try (InputStream is = new FileInputStream(getKeyStoreFile(context))) {
                  keystore.load(is, keyStorePassword.toCharArray());
                }
              } catch (Exception ex) {
                issues.add(context.createConfigIssue("", "keyStoreFile", Errors.IPC_ORIG_10, ex.toString()));
              }
            }
          }
        }
      } else {
        issues.add(context.createConfigIssue("", "keyStoreFile", Errors.IPC_ORIG_11));
      }
    }
  }

  File getKeyStoreFile(Stage.Context context) {
    return new File(context.getResourcesDirectory(), keyStoreFile);
  }

}
