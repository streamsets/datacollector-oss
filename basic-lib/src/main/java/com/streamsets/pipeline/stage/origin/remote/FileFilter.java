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
package com.streamsets.pipeline.stage.origin.remote;

import net.schmizz.sshj.sftp.RemoteResourceFilter;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class FileFilter implements RemoteResourceFilter, FileSelector {

  private static final Logger LOG = LoggerFactory.getLogger(FileFilter.class);

  protected final Pattern regex;

  FileFilter(FilePatternMode filePatternMode, String filePattern) {
    if (filePatternMode == FilePatternMode.GLOB) {
        filePattern = globToRegex(filePattern);
    }

    LOG.debug("Using regex: {}", filePattern);
    this.regex = Pattern.compile(filePattern);
  }

  @Override
  public boolean accept(RemoteResourceInfo resource) {
    if (resource.isDirectory()) {
      return true;
    }
    if (resource.isRegularFile()) {
      Matcher matcher = regex.matcher(resource.getName());
      if (matcher.matches()) {
        return true;
      }
    }
    LOG.trace("{} was not accepted", resource.getPath());
    return false;
  }

  @Override
  public boolean includeFile(FileSelectInfo fileInfo) throws Exception {
    if (fileInfo.getFile().getType() == FileType.FILE) {
      Matcher matcher = regex.matcher(fileInfo.getFile().getName().getBaseName());
      if (matcher.matches()) {
        return true;
      }
    }
    LOG.trace("{} was not included", fileInfo.getFile().getName().getPath());
    return false;
  }

  @Override
  public boolean traverseDescendents(FileSelectInfo fileInfo) {
    return true;
  }

  /**
   * Convert a limited file glob into a simple regex.
   *
   * @param glob file specification glob
   * @return regex.
   */
  private static String globToRegex(String glob) {
    if (glob.charAt(0) == '.' || glob.contains("/") || glob.contains("~")) {
      throw new IllegalArgumentException("Invalid character in file glob");
    }

    // treat dot as a literal.
    glob = glob.replace(".", "\\.");
    glob = glob.replace("*", ".+");
    glob = glob.replace("?", ".{1}+");

    return glob;
  }

  public Pattern getRegex() {
    return regex;
  }
}
