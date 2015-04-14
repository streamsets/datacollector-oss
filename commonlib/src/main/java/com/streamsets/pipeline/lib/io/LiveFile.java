/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A <code>LiveFile</code> is a File reference that keeps track of its iNode and it can resync its name,
 * using the iNode as the anchor, in case of a rename. IMPORTANT: The rename must be within the same directory.
 * <p/>
 * The primary use case for this class is for handling log files which may be rotated (renamed) while the file is
 * being accessed. By keeping track of the iNode, it is possible to get intermittent access to the same file (i.e.
 * from an application that has been restarted).
 * <p/>
 * A <code>LiveFile</code> is considered 'live' if its name has not change since it has been created (the
 * <code>LiveFile</code> instance).
 */
public class LiveFile {
  private Path path;
  private boolean live;
  private final String iNode;

  /**
   * Creates a <code>LiveFile</code> given a {@link Path}. The LiveFile is 'live'.
   *
   * @param path the Path of the LiveFile. The file refered by the Path must exist.
   * @throws IOException thrown if the LiveFile could not be instantiated.
   */
  public LiveFile(Path path) throws IOException {
    this(path, true);
  }

  /**
   * Creates a <code>LiveFile</code> given a {@link Path}.
   *
   * @param path the Path of the LiveFile. The file refered by the Path must exist.
   * @param live indicates if the LiveFile is 'live' or not, <code>true</code> is live, <code>false</code> is not.
   * @throws IOException thrown if the LiveFile could not be instantiated.
   */
  public LiveFile(Path path, boolean live) throws IOException {
    Utils.checkNotNull(path, "path");
    this.path = path.toAbsolutePath();
    this.live = live;
    iNode = Files.readAttributes(path, BasicFileAttributes.class).fileKey().toString();
  }

  private LiveFile(Path path, boolean live, String inode) {
    this.path = path.toAbsolutePath();
    this.live = live;
    iNode = inode;
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
   * Returns if the <code>LiveFile</code> is 'live' or not.
   *
   * @return <code>true</code> if the LiveFile is 'live', <code>false</code> if not.
   */
  public boolean isLive() {
    return live;
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
    return path.hashCode() + iNode.hashCode();
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
      return path.equals(other.path) && iNode.equals(other.iNode) && live == other.live;
    }
    return false;
  }

  public String toString() {
    return String.format("LiveFile[path=%s, live=%b, iNode=%s]", path, live, iNode);
  }

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Serializes the <code>LiveFile</code> as a string.
   *
   * @return the serialized string representation of the <code>LiveFile</code>.
   */
  @SuppressWarnings("unchecked")
  public String serialize() {
    Map map = new LinkedHashMap();
    map.put("path", path.toString());
    map.put("inode", iNode);
    map.put("live", live);
    try {
      return OBJECT_MAPPER.writeValueAsString(map);
    } catch (Exception ex) {
      throw new RuntimeException("It should not happen: " + ex.getMessage(), ex);
    }
  }

  /**
   * Deserializes a string representation of a <code>LiveFile</code>.
   * <p/>
   * The underlying file must exist at deserialization time.
   *
   * @param str the string representation of a <code>LiveFile</code>.
   * @return the deserialized <code>LiveFile</code>
   * @throws IOException thrown if the string con not be deserialized into a <code>LiveFile</code>.
   */
  public static LiveFile deserialize(String str) throws IOException {
    Utils.checkNotNull(str, "str");
    try {
      Map map = OBJECT_MAPPER.readValue(str, Map.class);
      Path path = Paths.get((String) map.get("path"));
      boolean live = (Boolean) map.get("live");
      String inode = (String) map.get("inode");
      LiveFile liveFile = new LiveFile(path, live, inode);
      liveFile.refresh();
      return liveFile;
    } catch (RuntimeException|JsonParseException ex) {
      throw new IllegalArgumentException(Utils.format("Invalid LiveFile serialized string '{}': {}", str,
                                                      ex.getMessage()), ex);
    }
  }

  /**
   * Refreshes the <code>LiveFile</code>, if the LiveFile was 'live' and its name changed, the LiveFile is not 'live'
   * anymore.
   *
   * @return <code>true</code> if the <code>LiveFile</code> name has changed, <code>false</code> if not.
   * @throws IOException thrown if the LiveFile could not be found (i.e. it has been deleted or moved to a different
   * directory).
   */
  public boolean refresh() throws IOException {
    boolean changed;
    try {
      String iNodeStr = Files.readAttributes(path, BasicFileAttributes.class).fileKey().toString();
      changed = !this.iNode.equals(iNodeStr);
    } catch (NoSuchFileException ex) {
      changed = true;
    }
    if (changed) {
      boolean found = false;
      try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path.getParent())) {
        for (Path path : directoryStream) {
          String fileiNodeStr = Files.readAttributes(path, BasicFileAttributes.class).fileKey().toString();
          if (fileiNodeStr.equals(iNode)) {
            this.path = path;
            this.live = false;
            found = true;
            break;
          }
        }
        if (!found) {
          throw new NoSuchFileException("Could not find file for iNode '" + iNode + "', old file name '" + path + "'");
        }
      }
    }
    return changed;
  }

}
