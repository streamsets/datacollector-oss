/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.datacollector.security;

import com.streamsets.datacollector.io.TempFile;
import com.streamsets.pipeline.api.StageException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.KeyStore;
import java.util.UUID;

public class KeyStoreIO {

  public static class KeyStoreFile {
    private String path;
    private String password;

    public String getPath() {
      return path;
    }

    public KeyStoreFile setPath(String path) {
      this.path = path;
      return this;
    }

    public String getPassword() {
      return password;
    }

    public KeyStoreFile setPassword(String password) {
      this.password = password;
      return this;
    }
  }

  /**
   * Saves a Java KeyStore in a temporary file with a random password.
   * <p/>
   * It returns a bean with the absolute file path and the password.
   */
  public static KeyStoreFile save(KeyStore keyStore) throws StageException {
    try {
      String password = UUID.randomUUID().toString();
      File file = TempFile.createFile(".jks");
      try (OutputStream os = new FileOutputStream(file)) {
        keyStore.store(os, password.toCharArray());
      } catch (Exception ex) {
        throw new StageException(Errors.SECURITY_004, ex.toString());
      }
      return new KeyStoreFile().setPath(file.getAbsolutePath()).setPassword(password);
    } catch (IOException ex) {
      throw new StageException(Errors.SECURITY_005, ex.toString());
    }
  }

}
