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
package com.streamsets.pipeline.stage.connection.remote;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import org.apache.commons.vfs2.provider.ftps.FtpsDataChannelProtectionLevel;

@GenerateResourceBundle
public enum FTPSDataChannelProtectionLevel implements Label {
  // There's actually 4 levels, but nobody seems to actually use the other two, and even the RFC says not to use them
  // https://tools.ietf.org/html/rfc4217#page-10
  CLEAR("Clear", FtpsDataChannelProtectionLevel.C),
  PRIVATE("Private", FtpsDataChannelProtectionLevel.P),
  ;

  private final String label;
  private final FtpsDataChannelProtectionLevel level;

  FTPSDataChannelProtectionLevel(String label, FtpsDataChannelProtectionLevel level) {
    this.label = label;
    this.level = level;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public FtpsDataChannelProtectionLevel getLevel() {
    return level;
  }
}
