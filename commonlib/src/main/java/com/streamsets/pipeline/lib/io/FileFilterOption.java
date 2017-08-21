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


import com.streamsets.pipeline.api.impl.Utils;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

public enum FileFilterOption {
  FILTER_FILES_ONLY,
  FILTER_DIRECTORIES_ONLY,
  FILTER_REGULAR_FILES_ONLY,
  FILTER_DIRECTORY_REGULAR_FILES,
  NO_FILTER_OPTION;

  public static DirectoryStream.Filter<Path> getFilter(
      final Set<Path> foundPaths,
      FileFilterOption option
  ) {
    switch (option) {
      case FILTER_FILES_ONLY:
        return new DirectoryStream.Filter<Path>() {
          @Override
          public boolean accept(Path entry) throws IOException {
            return !foundPaths.contains(entry) && !Files.isDirectory(entry);
          }
        };
      case FILTER_DIRECTORIES_ONLY:
        return new DirectoryStream.Filter<Path>() {
          @Override
          public boolean accept(Path entry) throws IOException {
            return !foundPaths.contains(entry) && Files.isDirectory(entry);
          }
        };
      case FILTER_REGULAR_FILES_ONLY:
        return new DirectoryStream.Filter<Path>() {
          @Override
          public boolean accept(Path entry) throws IOException {
            return !foundPaths.contains(entry) && Files.isRegularFile(entry);
          }
        };
      case FILTER_DIRECTORY_REGULAR_FILES:
        return new DirectoryStream.Filter<Path>() {
          @Override
          public boolean accept(Path entry) throws IOException {
            return !foundPaths.contains(entry) && (Files.isDirectory(entry) || Files.isRegularFile(entry));
          }
        };
      case NO_FILTER_OPTION:
        return new DirectoryStream.Filter<Path>() {
          @Override
          public boolean accept(Path entry) throws IOException {
            return !foundPaths.contains(entry);
          }
        };
      default:
        throw new IllegalArgumentException(Utils.format("Invalid File Filter option: {}", option.name()));
    }
  }
}
