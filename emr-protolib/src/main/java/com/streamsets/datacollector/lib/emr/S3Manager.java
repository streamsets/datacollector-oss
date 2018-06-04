
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
package com.streamsets.datacollector.lib.emr;

import com.amazonaws.SdkBaseException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.google.common.base.Preconditions;
import com.streamsets.datacollector.util.EmrClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

public class S3Manager implements Closeable {

  private final static Logger LOG = LoggerFactory.getLogger(S3Manager.class);


  public static class Builder {
    private EmrClusterConfig pipelineEmrConfigs;
    private String pipelineId;
    private List<File> filesToUpload;
    private String uniquePrefix;

    public Builder setPipelineEmrConfigs(EmrClusterConfig pipelineEmrConfigs) {
      this.pipelineEmrConfigs = pipelineEmrConfigs;
      return this;
    }

    public Builder setPipelineId(String pipelineId) {
      this.pipelineId = pipelineId;
      return this;
    }

    public Builder setFilesToUpload(List<File> filesToUpload) {
      this.filesToUpload = filesToUpload;
      return this;
    }

    public Builder setUniquePrefix(String uniquePrefix) {
      this.uniquePrefix = uniquePrefix;
      return this;
    }

    /**
     * Builds an EmrArchive.
     *
     * @return the EmrArchive.
     */
    public S3Manager build() {
      Preconditions.checkState(pipelineEmrConfigs != null, "pipelineEmrConfigs must be set");
      Preconditions.checkState(pipelineId != null, "pipelineId must be set");
      Preconditions.checkState(uniquePrefix != null, "uniquePrefix must be set");
      return new S3Manager(pipelineEmrConfigs, pipelineId, uniquePrefix, filesToUpload);
    }
  }

  private EmrClusterConfig pipelineEmrConfigs;
  private String pipelineId;
  private String uniquePrefix;
  private List<File> filesToUpload;
  private final AmazonS3 s3Client;
  private final TransferManager s3TransferManager;

  S3Manager(EmrClusterConfig pipelineEmrConfigs, String pipelineId, String uniquePrefix, List<File> filesToUpload) {
    this.pipelineEmrConfigs = pipelineEmrConfigs;
    this.pipelineId = pipelineId;
    this.uniquePrefix = uniquePrefix;
    this.filesToUpload = filesToUpload;
    s3Client = getS3Client();
    int uploadThreads = Integer.parseInt(System.getProperty("sdc.emr.s3.uploadThreads", "5"));
    s3TransferManager = TransferManagerBuilder.standard()
        .withS3Client(s3Client)
        .withMultipartUploadThreshold(10L*1024*1024)
        .withMinimumUploadPartSize(10L*1024*1024)
        .withExecutorFactory(() -> Executors.newFixedThreadPool(uploadThreads))
        .withShutDownThreadPools(true)
        .build();
  }

  public void close() {
    s3TransferManager.shutdownNow();
  }

  public TransferManager getS3TransferManager() {
     return s3TransferManager;
  }

  public List<String> upload() throws IOException {
    Preconditions.checkState(filesToUpload != null, "filesToUpload must be set");
    Preconditions.checkState(!filesToUpload.isEmpty(), "filesToUpload cannot be empty");
    List<String> s3Uris = new ArrayList<>();
    for (File file : filesToUpload) {
      s3Uris.add(uploadToS3(file.getName(), file));
    }
    return s3Uris;
  }



  public void delete() {
    String bucket = getBucket(pipelineEmrConfigs.getS3StagingUri());
    String path = getPath(pipelineEmrConfigs.getS3StagingUri()) + "/" + pipelineId + "/" + uniquePrefix;
    s3Client.listObjects(bucket, path).getObjectSummaries().forEach(os -> s3Client.deleteObject(bucket, os.getKey()));
  }

  String getBucket(String s3Uri) {
    Preconditions.checkArgument(s3Uri.startsWith("s3://"), "s3Uri must begin with 's3://'");
    String s3Location = s3Uri.substring("s3://".length());
    int firstPath = s3Location.indexOf("/");
    if (firstPath == -1) {
      return s3Location;
    } else {
      return s3Location.substring(0, firstPath);
    }
  }

  String getPath(String s3Uri) {
    Preconditions.checkArgument(s3Uri.startsWith("s3://"), "s3Uri must begin with 's3://'");
    String s3Location = s3Uri.substring("s3://".length());
    int firstPath = s3Location.indexOf("/");
    if (firstPath == -1) {
      return "";
    } else {
      return s3Location.substring(firstPath + 1);
    }
  }

  private AmazonS3 getS3Client( )  {
    AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials
        (pipelineEmrConfigs.getAccessKey(),
            pipelineEmrConfigs.getSecretKey()
        ));

    AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
        .withRegion(pipelineEmrConfigs.getUserRegion())
        .withCredentials(credentialsProvider)
        .build();
    return s3Client;
  }


  String uploadToS3(String name, File file) throws IOException {
    long start = System.currentTimeMillis();
    long fileLength = file.length() / (1000 * 1000);
    String bucket = getBucket(pipelineEmrConfigs.getS3StagingUri());
    String path = getPath(pipelineEmrConfigs.getS3StagingUri()) + "/" + pipelineId + "/" + uniquePrefix;
    String s3Uri =  "s3://" + bucket + "/" +  path + "/" + name;
    try {

      // Upload
      PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, path + "/" + name, file);
      putObjectRequest.setGeneralProgressListener(new ProgressListener() {
        long counter;
        long tick = -1;
        @Override
        public void progressChanged(ProgressEvent progressEvent) {
          counter += progressEvent.getBytesTransferred();
          if (counter / (100 * 1000000) > tick) {
            tick++;
            LOG.debug(
                "Uploading '{}' {}/{} MB, {} secs",
                s3Uri,
                counter / (1000 * 1000),
                fileLength,
                (System.currentTimeMillis() - start) / 1000
            );
          }
        }
      });

      getS3TransferManager().upload(putObjectRequest).waitForCompletion();

      LOG.info("Uploaded file at: {}", s3Uri);
      return s3Uri;
    } catch (SdkBaseException|InterruptedException ex) {
      throw new IOException(ex);
    }
  }

}
