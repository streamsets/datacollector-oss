/*
 * Copyright 2017 StreamSets Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.credential.javakeystore.cli;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.streamsets.datacollector.credential.javakeystore.JavaKeyStoreCredentialStore;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.impl.Utils;
import io.airlift.airline.Option;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

public abstract class AbstractCommand implements Runnable {

  @Option(
      name = {"-i", "--id"},
      description = "Credential store ID",
      required = true
  )
  public String storeId;

  @Option(
      name = {"--stack"},
      description = "In case of error print the stack trace",
      required = false
  )
  public boolean stackTrace;

  protected CredentialStore.Context createContext(Configuration configuration) {
    return new CredentialStore.Context() {
      @Override
      public String getId() {
        return storeId;
      }

      @Override
      public CredentialStore.ConfigIssue createConfigIssue(ErrorCode errorCode, Object... args) {
        return new CredentialStore.ConfigIssue() {
          @Override
          public String toString() {
            return errorCode.toString() + " - " + Utils.format(errorCode.getMessage(), args);
          }
        };
      }

      @Override
      public String getConfig(String configName) {
        return configuration.get("credentialStore." + storeId + ".config." + configName, null);
      }
    };
  }

  protected Configuration loadConfiguration() {
     String dirName = System.getProperty("sdc.conf.dir");
    Preconditions.checkNotNull(dirName, "sdc.conf.dir system property not defined");
    File dir = new File(dirName).getAbsoluteFile();
    Preconditions.checkState(dir.exists(), Utils.format("Directory '{}' does not exist", dir));
    Configuration.setFileRefsBaseDir(new File(dirName));
    File file = new File(dir, "sdc.properties");
    Preconditions.checkState(file.exists(), Utils.format("File '{}' does not exist", file));
    Configuration conf = new Configuration();
    try (Reader reader = new FileReader(file)) {
      conf.load(reader);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    conf = conf.getSubSetConfiguration("credentialStore." + storeId + ".");
    return conf;
  }

  @VisibleForTesting
  protected JavaKeyStoreCredentialStore createStore() {
    return new JavaKeyStoreCredentialStore();
  }

  @Override
  public void run() {
    JavaKeyStoreCredentialStore store = createStore();
    try {
      Configuration configuration = loadConfiguration();
      List<CredentialStore.ConfigIssue> issues = store.init(createContext(configuration));
      if (issues.isEmpty()) {
        execute(store);
      } else {
        System.err.println();
        System.err.printf("Could not initialize Credential Store '%s\n", storeId);
        for (CredentialStore.ConfigIssue issue : issues) {
          System.err.println("  " + issue);
        }
        throw new RuntimeException();
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      store.destroy();
    }
  }

  protected abstract void execute(JavaKeyStoreCredentialStore store);

}
