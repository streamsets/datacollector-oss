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

package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.PushSource;

public class AmazonS3RunnableBuilder {
  private PushSource.Context context;
  private int batchSize;
  private S3ConfigBean s3ConfigBean;
  private S3Spooler spooler;
  private AmazonS3Source amazonS3Source;
  private int threadNumber;

  public AmazonS3RunnableBuilder() {
  }

  public AmazonS3RunnableBuilder context(PushSource.Context context) {
    this.context = context;
    return this;
  }

  public AmazonS3RunnableBuilder batchSize(int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public AmazonS3RunnableBuilder s3ConfigBean(S3ConfigBean s3ConfigBean) {
    this.s3ConfigBean = s3ConfigBean;
    return this;
  }

  public AmazonS3RunnableBuilder spooler(S3Spooler spooler) {
    this.spooler = spooler;
    return this;
  }

  public AmazonS3RunnableBuilder amazonS3Source(AmazonS3Source amazonS3Source) {
    this.amazonS3Source = amazonS3Source;
    return this;
  }

  public AmazonS3RunnableBuilder threadNumber(int threadNumber) {
    this.threadNumber = threadNumber;
    return this;
  }

  public AmazonS3Runnable build() {
    return new AmazonS3Runnable(context, batchSize, threadNumber, s3ConfigBean, spooler, amazonS3Source);
  }
}
