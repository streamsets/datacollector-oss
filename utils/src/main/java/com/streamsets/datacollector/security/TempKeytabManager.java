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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Base64;
import java.util.Comparator;
import java.util.Set;
import java.util.UUID;

/**
 * Utility class for managing temporary keytab files. It maintains a temporary directory (whose path is determined
 * by configuration), and generates temporary keytab files within that directory (given the binary keytab data), and
 * deletes these temp files when no longer needed.
 *
 */
public class TempKeytabManager {

  private static final Logger LOG = LoggerFactory.getLogger(TempKeytabManager.class);
  private Path tempKeytabDir;

  private final Configuration configuration;
  private final String baseDirKey;
  private final String baseDirDefaultValue;
  private final String subdirName;

  @VisibleForTesting
  public static final Set<PosixFilePermission> GLOBAL_ALL_PERM = PosixFilePermissions.fromString(
      "rwxrwxrwx"
  );

  @VisibleForTesting
  public static final Set<PosixFilePermission> USER_ONLY_PERM = PosixFilePermissions.fromString(
      "rwx------"
  );

  /**
   * Constructs a temporary keytab manager, using the given container {@link Configuration}, setting the temporary
   * keytab location from the given property (with given default value), and with the given subdirectory name.
   *
   * The overall directory where the temp keytabs will be stored is:
   *
   * <pre>/<baseDir>/<subdirName>/<userName></pre>
   *
   * Where baseDir is resolved as per the given configuration properties (and params; see below), and subdirName
   * is taken from the parameter, and userName is taken from the current user running the process. The last portion is
   * to ensure that if different uids run this code in different contexts (ex: container versus cluster mode), then
   * they don't interfere with each others' temp keytab stores.
   *
   * @param configuration the product {@link Configuration}
   * @param baseDirKey the key in the properties file pointing to the top level temp keytab dir
   * @param baseDirDefaultValue the default value for the aforementioned property
   * @param subdirName the subdirectory name within the top level directory for the implementation-specific
   *                   temporary keytab store
   * @throws IllegalStateException
   */
  public TempKeytabManager(
      Configuration configuration,
      String baseDirKey,
      String baseDirDefaultValue,
      String subdirName
  ) throws IllegalStateException {

    this.configuration = configuration;
    this.baseDirKey = baseDirKey;
    this.baseDirDefaultValue = baseDirDefaultValue;
    this.subdirName = subdirName;
  }

  /**
   * Ensures that the temp keytab directory (including top level and user-specific subdir) exists and has the
   * proper permissions.
   *
   * Synchronized to ensure that if multiple threads both attempt to store a temp keytab at the same time,
   * only one will attempt create a missing directory.
   */
  @VisibleForTesting
  public synchronized void ensureKeytabTempDir() {
    final String keytabDir = configuration.get(baseDirKey, baseDirDefaultValue);
    final Path keytabDirPath = keytabDirHelper(
        Paths.get(keytabDir, subdirName),
        GLOBAL_ALL_PERM,
        "top level keytab directory",
        true
    );
    final Path userSpecificDirPath = keytabDirHelper(
        keytabDirPath.resolve(System.getProperty("user.name")),
        USER_ONLY_PERM,
        "user specific keytab directory",
        true
    );
    // the temp keytab directory is the subdir, then a user-specific (user named) directory underneath that
    tempKeytabDir = userSpecificDirPath;

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
   * This method is overall thread-safe. If multiple threads attempt to create a temp keytab file at the same time,
   * they should generate distinct UUIDs so there should not be a clash. If the overall temp keytab directory does not
   * yet exist (see constructor Javadoc for details), then it will be created in a synchronized block.
   *
   * @param keytabContentsBase64 the contents of the keytab (binary data encoded to a Base64 String), to be written to
   *                             the temporary keytab file
   * @return the generated keytab name
   * @throws IOException if an error occurs when creating or writing the temp keytab file
   * @throws IllegalStateException if the keytab temp directory was not in the correct state (writeable directory)
   */
  public String createTempKeytabFile(String keytabContentsBase64) throws IOException, IllegalStateException {
    ensureKeytabTempDir();
    Utils.checkNotNull(tempKeytabDir, "tempKeytabDir");
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
    final Path keytabPath = tempKeytabDir.resolve(keytabFileName);
    if (!Files.exists(keytabPath)) {
      LOG.warn("Could not delete keytab at {}; file did not exist", keytabPath.toString());
    } else {
      Files.delete(keytabPath);
    }
  }

  private Path keytabDirHelper(Path path, Set<PosixFilePermission> permissions, String label, boolean create) {
    final File dir = path.toFile();

    final boolean exists = dir.exists();
    if (!exists) {
      if (create) {
        LOG.debug("Attempting to create {} directory at: {}", label, path.toString());
        try {
          final Path directories = Files.createDirectories(path);
          Files.setPosixFilePermissions(path, permissions);
          return directories;
        } catch (IOException e) {
          throw new IllegalStateException(String.format(
              "Failed to create %s directory at %s: %s",
              label,
              path.toString(),
              e.getMessage()
          ), e);
        }
      } else {
        // doesn't exist, but shouldn't be created either; return the path as it was passed in
        return path;
      }
    } else /* exists = true */ {
      final boolean isDirectory = dir.isDirectory();
      if (!isDirectory) {
        throw new IllegalStateException(String.format(
            "%s directory is not a directory, but a file: %s",
            label,
            dir.toString()
        ));
      } else /* isDirectory = true */ {
        LOG.trace("{} directory already exists at: {}", label, path.toString());
        try {
          final Set<PosixFilePermission> existingPermissions = Files.getPosixFilePermissions(path);
          if (!permissions.equals(existingPermissions)) {
            if (LOG.isWarnEnabled()) {
              LOG.warn(
                  "Existing permissions for temp keytab dir at {} were {}; changing them to {}",
                  path.toString(),
                  PosixFilePermissions.toString(existingPermissions),
                  PosixFilePermissions.toString(permissions)
              );
            }
            Files.setPosixFilePermissions(path, permissions);
          }
        } catch (IOException e) {
          throw new RuntimeException("Failed to get existing permissions for top level keytab dir", e);
        }
        return path;
      }
    }
  }

  /**
   * Cleans up the temp keytab directory managed by this instance. All temp keytab files within it are removed.
   */
  public void cleanUpKeytabDirectory() throws IOException {
    if (tempKeytabDir != null) {
      if (Files.exists(tempKeytabDir) && Files.isDirectory(tempKeytabDir)) {
        Files.walk(tempKeytabDir)
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);
      } else {
        LOG.warn(
            "Directory {} did not exist, or was not a directory, when attempting to clean up temp keytab directory",
            tempKeytabDir.toString()
        );
      }
    } else {
      LOG.warn("tempKeytabDir was null; may not have been initialized");
    }
  }
}
