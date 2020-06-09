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
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.api.ext.DataCollectorServices;
import com.streamsets.pipeline.api.ext.json.JsonMapper;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A <code>LiveFile</code> is a File reference that keeps track of its iNode and it can resync its name,
 * using the iNode as the anchor, in case of a rename. IMPORTANT: The rename must be within the same directory.
 * <p/>
 * <b>NOTE:</b> EXT4 filesystems reuse iNodes immediately, so if you delete a file and create a new file the iNode
 * of the old file will most likely be used for the new file. To be able to handle this case and detect a file has
 * been renamed (as opposed to deleted followed by a complete different file being created reusing the iNode) we
 * hash the head (1024 bytes) of the file (Brocks idea).
 * <p/>
 * The primary use case for this class is for handling log files which may be rotated (renamed) while the file is
 * being accessed. By keeping track of the iNode, it is possible to get intermittent access to the same file (i.e.
 * from an application that has been restarted).
 * <p/>
 * A <code>LiveFile</code> is immutable.
 */
public class LiveFile {
  private static final int HEAD_LEN = 1024;

  private final Path path;
  private final String headHash;
  private final int headLen;
  private final String iNode;

  /**
   * Creates a <code>LiveFile</code> given a {@link Path}.
   *
   * @param path the Path of the LiveFile. The file referred by the Path must exist.
   * @throws IOException thrown if the LiveFile does not exist.
   */
  public LiveFile(Path path) throws IOException {
    Utils.checkNotNull(path, "path");
    this.path = path.toAbsolutePath();
    if (!this.path.toFile().isFile()) {
      throw new NoSuchFileException(Utils.format("Path '{}' is not a file", this.path));
    }
    BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
    headLen = (int) Math.min(HEAD_LEN, attrs.size());
    headHash = computeHash(path, headLen);
    iNode = attrs.fileKey().toString();
  }

  private LiveFile(Path path,  String inode, String headHash, int headLen) {
    this.path = path.toAbsolutePath();
    iNode = inode;
    this.headHash = headHash;
    this.headLen = headLen;
  }


  String computeHash(Path path, int len) throws IOException {
    byte[] buffer = new byte[len];
    try (InputStream is = new FileInputStream(path.toFile())) {
      IOUtils.readFully(is, buffer);
    }
    try {
      MessageDigest digest = MessageDigest.getInstance("MD5");
      buffer = digest.digest(buffer);
      return Base64.encodeBase64String(buffer);
    } catch (NoSuchAlgorithmException ex) {
      throw new IOException(ex);
    }
  }

  /**
   * Returns the {@link Path} of the <code>LiveFile</code>.
   *
   * @return the {@link Path} of the <code>LiveFile</code>.
   */
  public Path getPath() {
    return path;
  }

  /**
   * Returns the iNode of the <code>LiveFile</code>.
   *
   * @return the iNode of the <code>LiveFile</code>.
   */
  public String getINode() {
    return iNode;
  }

  @Override
  public int hashCode() {
    return path.hashCode() + iNode.hashCode() + headHash.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (obj instanceof LiveFile) {
      LiveFile other = (LiveFile) obj;
      return path.equals(other.path) && iNode.equals(other.iNode) && headHash.equals(other.headHash);
    }
    return false;
  }

  public String toString() {
    return String.format("LiveFile[path=%s, iNode=%s, headHash=%s]", path, iNode, headHash);
  }

  /**
   * Serializes the <code>LiveFile</code> as a string.
   *
   * @return the serialized string representation of the <code>LiveFile</code>.
   */
  @SuppressWarnings("unchecked")
  public String serialize() {
    Map map = new LinkedHashMap();
    map.put("path", path.toString());
    map.put("headHash", headHash);
    map.put("headLen", headLen);
    map.put("inode", iNode);
    try {
      JsonMapper objectMapper = DataCollectorServices.instance().get(JsonMapper.SERVICE_KEY);
      return objectMapper.writeValueAsString(map);
    } catch (Exception ex) {
      throw new RuntimeException(Utils.format("Unexpected exception: {}", ex.toString()), ex);
    }
  }

  /**
   * Deserializes a string representation of a <code>LiveFile</code>.
   * <p/>
   *
   * @param str the string representation of a <code>LiveFile</code>.
   * @return the deserialized <code>LiveFile</code>
   * @throws IOException thrown if the string con not be deserialized into a <code>LiveFile</code>.
   */
  public static LiveFile deserialize(String str) throws IOException {
    Utils.checkNotNull(str, "str");
    try {
      JsonMapper objectMapper = DataCollectorServices.instance().get(JsonMapper.SERVICE_KEY);
      Map map = objectMapper.readValue(str, Map.class);
      Path path = Paths.get((String) map.get("path"));
      String headHash = (map.containsKey("headHash")) ? (String) map.get("headHash") : "";
      int headLen = (map.containsKey("headLen")) ? (int) map.get("headLen") : 0;
      String inode = (String) map.get("inode");
      return new LiveFile(path, inode, headHash, headLen);
    } catch (RuntimeException|IOException ex) {
      throw new IllegalArgumentException(Utils.format("Invalid LiveFile serialized string '{}': {}", str,
                                                      ex.toString()), ex);
    }
  }

  /**
   * Refreshes the <code>LiveFile</code>, if the file was renamed, the path will have the new name.
   *
   * @return the refreshed file if the file has been renamed, or itself if the file has not been rename or the file
   * does not exist in the directory anymore.
   * @throws IOException thrown if the LiveFile could not be refreshed
   */
  public LiveFile refresh() throws IOException {
    LiveFile refresh = this;
    boolean changed;
    try {
      BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
      String iNodeCurrent = attrs.fileKey().toString();
      int headLenCurrent = (int) Math.min(headLen, attrs.size());
      String headHashCurrent = computeHash(path, headLenCurrent);
      changed = !this.iNode.equals(iNodeCurrent) || !this.headHash.equals(headHashCurrent);
    } catch (NoSuchFileException ex) {
      changed = true;
    }
    if (changed) {
      try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path.getParent())) {
        for (Path path : directoryStream) {
          if (!Files.isDirectory(path)) {
            BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
            String iNode = attrs.fileKey().toString();
            int headLen = (int) Math.min(this.headLen, attrs.size());
            String headHash = computeHash(path, headLen);
            if (iNode.equals(this.iNode) && headHash.equals(this.headHash)) {
              if (headLen == 0) {
                headLen = (int) Math.min(HEAD_LEN, attrs.size());
                headHash = computeHash(path, headLen);
              }
              refresh = new LiveFile(path, iNode, headHash, headLen);
              break;
            }
          }
        }
      }
    }
    return refresh;
  }

}
