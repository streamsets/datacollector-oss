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
package com.streamsets.datacollector.bundles.content;

import com.streamsets.datacollector.blobstore.BlobStoreTask;
import com.streamsets.datacollector.bundles.BundleContentGenerator;
import com.streamsets.datacollector.bundles.BundleContentGeneratorDef;
import com.streamsets.datacollector.bundles.BundleContext;
import com.streamsets.datacollector.bundles.BundleWriter;
import com.streamsets.pipeline.api.StageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@BundleContentGeneratorDef(
  name = "Blob Store",
  description = "Internal blob store with various data provided by Control Hub.",
  version = 1,
  enabledByDefault = true
)
public class BlobStoreContentGenerator implements BundleContentGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(BlobStoreContentGenerator.class);

  @Override
  public void generateContent(BundleContext context, BundleWriter writer) throws IOException {
    BlobStoreTask blobStore = context.getBlobStore();

    for(String namespace : blobStore.listNamespaces()) {
      for(String objectId : blobStore.listObjects(namespace)) {
        for(long version : blobStore.allVersions(namespace, objectId)) {
          writer.markStartOfFile(namespace + "/" + objectId + "/" + version + ".txt");
          try {
            writer.write(blobStore.retrieve(namespace, objectId, version));
          } catch (StageException e) {
            LOG.error("Can't retrieve {}:{}:{}: {}", namespace, objectId, version, e.toString(), e);
          }
          writer.markEndOfFile();
        }
      }
    }
  }
}
