/*
 * Copyright 2020 StreamSets Inc.
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

import com.streamsets.pipeline.api.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.UUID;

/**
 * Utility class for managing temporary keytab files. It maintains a temporary directory (whose path is determined
 * by configuration), and generates temporary keytab files within that directory (given the binary keytab data), and
 * deletes these temp files when no longer needed.
 *
 */
public class TempKeytabManager {

  private static final Logger LOG = LoggerFactory.getLogger(TempKeytabManager.class);
  private final Path tempKeytabDir;

  /**
   * Constructs a temporary keytab manager, using the given container {@link Configuration}, setting the temporary
   * keytab location from the given property (with given default value), and with the given subdirectory name.
   *
   * The constructor attempts to create this temp directory, and throws an {@link IllegalStateException} if that fails
   *
   * @param configuration
   * @param baseDirKey
   * @param baseDirDefaultValue
   * @param subdirName
   * @throws IllegalStateException
   */
  public TempKeytabManager(
      Configuration configuration,
      String baseDirKey,
      String baseDirDefaultValue,
      String subdirName
  ) throws IllegalStateException {

    final String keytabDir = configuration.get(baseDirKey, baseDirDefaultValue);
    tempKeytabDir = Paths.get(keytabDir).resolve(subdirName);
    if (!Files.exists(tempKeytabDir) && !createAndRestrictKeytabSubdirIfNeeded(tempKeytabDir.toFile())) {
      throw new IllegalStateException(String.format(
          "Failed to create temporary keytab directory at: %s",
          tempKeytabDir.toString()
      ));
    }
  }

  private void ensureKeytabTempDir() {
    String errorMsg = null;
    if (tempKeytabDir == null) {
      errorMsg = "tempKeytabDir was null; initialization did not complete successfully";
    } else if (!Files.exists(tempKeytabDir)) {
      errorMsg = String.format("Temp keytab dir at %s doesn't exist, or isn't readable", tempKeytabDir.toString());
    } else if (!Files.isDirectory(tempKeytabDir)) {
      errorMsg = String.format("Temp keytab dir %s exists, but isn't a directory", tempKeytabDir.toString());
    }

    if (errorMsg != null) {
      throw new IllegalStateException(errorMsg);
    }
  }

  /**
   * Creates a keytab in the temporary keytab directory, with a generated name, and returns that keytab name.  The
   * temporary keytab file on disk will be deleted either when {@link #deleteTempKeytabFileIfExists(String)} is called,
   * or the JVM exits (whichever happens first).
   *
   * @param keytabContentsBase64 the contents of the keytab (binary data encoded to a Base64 String), to be written to
   *                             the temporary keytab file
   * @return the generated keytab name
   * @throws IOException if an error occurs when creating or writing the temp keytab file
   * @throws IllegalStateException if the keytab temp directory was not in the correct state (writeable directory)
   */
  public String createTempKeytabFile(String keytabContentsBase64) throws IOException, IllegalStateException {
    ensureKeytabTempDir();
    final String keytabFileName = UUID.randomUUID().toString();
    final Path keytabPath = tempKeytabDir.resolve(keytabFileName);
    if (Files.exists(keytabPath)) {
      // should not happen unless there is a UUID collision...
      throw new IllegalStateException(String.format(
          "Failed to create new file for temporary keytab at %s, since this file already existed on disk",
          keytabPath.toString()
      ));
    } else {
      Files.createFile(keytabPath);
      try (final FileOutputStream writer = new FileOutputStream(keytabPath.toFile())) {
        writer.write(Base64.getDecoder().decode(keytabContentsBase64));
      }
      // the temp keytab file should be deleted on JVM exit
      keytabPath.toFile().deleteOnExit();
      return keytabFileName;
    }
  }

  /**
   * Returns the {@link Path} for a given temporary keytab file name.
   *
   * @param keytabFileName
   * @return the {@link Path} pointing to the underlying temp keytab file
   */
  public Path getTempKeytabPath(String keytabFileName) {
    return tempKeytabDir.resolve(keytabFileName);
  }

  private static synchronized boolean createAndRestrictKeytabSubdirIfNeeded(File keytabSubdir) {
    boolean created = keytabSubdir.exists();
    if (!created) {
      created = keytabSubdir.mkdirs();
      if (created) {
        // make the temp keytab dir not readable except by owner, for security purposes
        created = keytabSubdir.setReadable(false, false);
      }
    }
    return created;
  }

  /**
   * Deletes a temporary keytab file.
   *
   * @param keytabFileName the keytab name to be deleted (which was generated by {@link #createTempKeytabFile(String)})
   * @throws IOException if the call to delete the file fails
   * @throws IllegalStateException if the keytab temp directory was not in the correct state (writeable directory)
   */
  public void deleteTempKeytabFileIfExists(String keytabFileName) throws IOException {
    if (keytabFileName == null) {
      LOG.debug("deleteTempKeytabFileIfExists was called with a null keytabFileName");
      return;
    }
    ensureKeytabTempDir();
    final Path keytabPath = tempKeytabDir.resolve(keytabFileName);
    if (!Files.exists(keytabPath)) {
      LOG.warn("Could not delete keytab at {}; file did not exist", keytabPath.toString());
    } else {
      Files.delete(keytabPath);
    }
  }

}
