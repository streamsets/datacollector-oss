/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.lib.dirspooler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Interface for components that are exposing filesystem of both local and hdfs to the dirspooler
 */
public interface WrappedFile {
  /**
   * Tells whether or not this path is absolute.
   *
   * <p> An absolute path is complete in that it doesn't need to be combined
   * with other path information in order to locate a file.
   *
   * @return  {@code true} if, and only if, this path is absolute
   */
  boolean isAbsolute();

  /**
   * Returns the {@code String} absolute path of this file.
   *
   * @return  a {@code String} object representing the absolute path
   */
  String getAbsolutePath();

  /**
   * Returns the {@code String} the file name.
   *
   * @return  the {@code String} file name
   */
  String getFileName();

  /**
   * Returns the {@code String} absolute parent path
   *
   * @return  the {@code String} absolute parent path
   */
  String getParent();

  /**
   * Returns the {@code long} size of the file
   *
   * @return  the {@code long} size of the file
   */
  long getSize() throws IOException;

  /**
   * Returns the {@code InputStream} input stream of the file
   *
   * @return  the {@code InputStream} input stream of the file
   */
  InputStream getInputStream() throws IOException;

  /**
   * Returns the {@code Map<String, Object>} file metatdata
   *
   * @return  the {@code Map<String, Object>} file metatdata
   */
  Map<String, Object> getFileMetadata() throws IOException;

  /**
   * Returns the custom metadata map for the wrapped file (which is persisted across invocations)
   *
   * @return the {@code Map<String, Object>} file custom metatdata
   */
  Map<String, Object> getCustomMetadata();

  /**
   * Returns whether the file has read access permission or not
   *
   * @return true if the SDC has read access permission to the file
   */
  boolean canRead() throws IOException;
}
