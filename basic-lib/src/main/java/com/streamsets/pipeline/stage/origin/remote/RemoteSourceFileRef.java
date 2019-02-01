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
package com.streamsets.pipeline.stage.origin.remote;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.io.fileref.AbstractFileRef;
import com.streamsets.pipeline.lib.remote.RemoteFile;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

final class RemoteSourceFileRef extends AbstractFileRef{
  private final RemoteFile remoteFile;
  private final URI remoteUri;

  private RemoteSourceFileRef(
      RemoteFile remoteFile,
      URI remoteUri,
      int bufferSize,
      boolean createMetrics,
      long totalSizeInBytes,
      double rateLimit
  ) {
    super(
        ImmutableSet.<Class<? extends AutoCloseable>>of(InputStream.class),
        bufferSize,
        createMetrics,
        totalSizeInBytes,
        rateLimit,
        false,
        null,
        null
    );
    this.remoteFile = remoteFile;
    this.remoteUri = remoteUri;
  }

  @Override
  public String toString() {
    return "Remote: URI='" + remoteUri + "', File ='" + remoteFile.getFilePath() + "'";
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <T extends AutoCloseable> T createInputStream(Class<T> streamClassType) throws IOException {
    return (T) remoteFile.createInputStream();
  }


  static class Builder extends AbstractFileRef.Builder<RemoteSourceFileRef, Builder> {
    RemoteFile remoteFile;
    URI remoteUri;

    Builder remoteFile(RemoteFile remoteFile) {
      this.remoteFile = remoteFile;
      return this;
    }

    Builder remoteUri(URI remoteUri) {
      this.remoteUri = remoteUri;
      return this;
    }

    @Override
    public RemoteSourceFileRef build() {
      Utils.checkNotNull(remoteFile, "Remote file should not be null");
      Utils.checkNotNull(remoteUri, "Remote Uri should not be null");
      return new RemoteSourceFileRef(remoteFile, remoteUri, bufferSize, createMetrics, totalSizeInBytes, rateLimit);
    }
  }
}
