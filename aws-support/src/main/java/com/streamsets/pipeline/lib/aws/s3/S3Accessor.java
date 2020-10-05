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
package com.streamsets.pipeline.lib.aws.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.aws.Errors;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public class S3Accessor implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(S3Accessor.class);

  public static class Builder {
    private CredentialsConfigs credentialConfigs;
    private ConnectionConfigs connectionConfigs;
    private TransferManagerConfigs transferManagerConfigs;
    private SseConfigs sseConfigs;

    private Builder() {
    }

    public Builder setCredentialConfigs(CredentialsConfigs credentialConfigs) {
      this.credentialConfigs = credentialConfigs;
      return this;
    }

    // if the region in the given configs is NULL or empty the S3Accessor will have global bucket access enabled
    public Builder setConnectionConfigs(ConnectionConfigs connectionConfigs) {
      this.connectionConfigs = connectionConfigs;
      return this;
    }

    public Builder setTransferManagerConfigs(TransferManagerConfigs transferManagerConfigs) {
      this.transferManagerConfigs = transferManagerConfigs;
      return this;
    }

    public Builder setSseConfigs(SseConfigs sseConfigs) {
      this.sseConfigs = sseConfigs;
      return this;
    }

    public S3Accessor build() {
      Utils.checkNotNull(credentialConfigs, "credentialConfigs");
      Utils.checkNotNull(connectionConfigs, "connectionConfigs");
      return new S3Accessor(credentialConfigs, connectionConfigs, transferManagerConfigs, sseConfigs);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private final CredentialsConfigs credentialConfigs;
  private final ConnectionConfigs connectionConfigs;
  private final TransferManagerConfigs transferManagerConfigs;
  private final SseConfigs sseConfigs;

  //visible for testing
  S3Accessor(
      CredentialsConfigs credentialConfigs,
      ConnectionConfigs connectionConfigs,
      TransferManagerConfigs transferManagerConfigs,
      SseConfigs sseConfigs
  ) {
    this.credentialConfigs = credentialConfigs;
    this.connectionConfigs = connectionConfigs;
    this.transferManagerConfigs = transferManagerConfigs;
    this.sseConfigs = sseConfigs;
  }

  private AWSCredentialsProvider credentialsProvider;
  private AmazonS3Client s3Client;
  private TransferManager transferManager;
  private EncryptionMetadataBuilder encryptionMetadataBuilder;

  public void init() {
    credentialsProvider = createCredentialsProvider();
    s3Client = createS3Client();
    if (transferManagerConfigs != null) {
      transferManager = createTransferManager(getS3Client());
    }
    encryptionMetadataBuilder = createEncryptionMetadataBuilder();
  }

  public AWSCredentialsProvider getCredentialsProvider() {
    return credentialsProvider;
  }

  public AmazonS3Client getS3Client() {
    return s3Client;
  }

  boolean hasTransferManager() {
    return transferManager != null;
  }

  public TransferManager getTransferManager() {
    Utils.checkState(hasTransferManager(), "transferManager not available");
    return transferManager;
  }

  public EncryptionMetadataBuilder getEncryptionMetadataBuilder() {
    return encryptionMetadataBuilder;
  }

  public interface Uploader {
    Upload upload(String bucket, String key, InputStream is);
  }

  public Uploader getUploader() {
    Utils.checkState(hasTransferManager(), "transferManager not available");
    return (bucket, key, is) -> {
      Utils.checkNotNull(bucket, "bucket");
      Utils.checkNotNull(key, "key");
      Utils.checkNotNull(is, "is");
      PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, is, getEncryptionMetadataBuilder().build());
      Upload upload = getTransferManager().upload(putObjectRequest);
      upload.addProgressListener(new UploaderProgressListener(bucket + key));
      return upload;
    };
  }

  @Override
  public void close() {
    if (hasTransferManager()) {
      getTransferManager().shutdownNow();
      transferManager = null;
    }
    if (getS3Client() != null) {
      getS3Client().shutdown();
      s3Client = null;
    }
    credentialsProvider = null;
    encryptionMetadataBuilder = null;
  }

  //visible for testing
  AWSCredentialsProvider createCredentialsProvider() {
    AWSCredentialsProvider awsCredentialsProvider = null;
    CredentialValue accessKey = credentialConfigs.getAccessKey();
    CredentialValue secretKey = credentialConfigs.getSecretKey();

    if (accessKey != null && secretKey != null) {
      String accessKeyString = accessKey.get();
      String secretKeyString = secretKey.get();

      if (accessKeyString != null &&
          !accessKeyString.isEmpty() &&
          secretKeyString != null &&
          !secretKeyString.isEmpty()) {
        awsCredentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyString,
            secretKeyString
        ));
      }
    }

    return awsCredentialsProvider;
  }

  //visible for testing
  ClientConfiguration createClientConfiguration() {
    ClientConfiguration clientConfig = new ClientConfiguration();

    clientConfig.setConnectionTimeout(connectionConfigs.getConnectionTimeoutMillis());
    clientConfig.setSocketTimeout(connectionConfigs.getSocketTimeoutMillis());
    clientConfig.withMaxErrorRetry(connectionConfigs.getMaxErrorRetry());

    if (connectionConfigs.isProxyEnabled()) {
      clientConfig.setProxyHost(connectionConfigs.getProxyHost());
      clientConfig.setProxyPort(connectionConfigs.getProxyPort());
      if (connectionConfigs.isProxyAuthenticationEnabled()) {
        clientConfig.setProxyUsername(connectionConfigs.getProxyUser().get());
        clientConfig.setProxyPassword(connectionConfigs.getProxyPassword().get());
      }
    }
    return clientConfig;
  }

  //visible for testing
  AmazonS3ClientBuilder createAmazonS3ClientBuilder() {
    return AmazonS3ClientBuilder.standard();
  }

  //visible for testing
  AmazonS3Client createS3Client() {

    AmazonS3ClientBuilder builder = createAmazonS3ClientBuilder().withClientConfiguration(createClientConfiguration())
                                                                 .withChunkedEncodingDisabled(connectionConfigs
                                                                     .isChunkedEncodingEnabled())
                                                                 .withPathStyleAccessEnabled(true);
    AWSCredentialsProvider awsCredentialsProvider = getCredentialsProvider();
    // If we don't call build.withCredentials(...) then we will not overwrite the default credentials provider
    // already set in the builder when doing AmazonS3ClientBuilder.standard() so only calling build.withCredentials(...)
    // if our own provider exists
    if (awsCredentialsProvider != null) {
      builder.withCredentials(awsCredentialsProvider);
    }

    String region = (connectionConfigs.getRegion() == null || connectionConfigs.getRegion().isEmpty())
                    ? null
                    : connectionConfigs.getRegion();

    if (connectionConfigs.isUseEndpoint()) {
      builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(connectionConfigs.getEndpoint(),
          region
      ));
    } else if (region != null) {
      builder.withRegion(connectionConfigs.getRegion());
    } else {
      builder.withRegion(AwsRegion.US_WEST_1.getId());
      builder.withForceGlobalBucketAccessEnabled(true);
    }
    return (AmazonS3Client) builder.build();
  }

  //visible for testing
  ExecutorFactory createExecutorFactory(int threads) {
    return () -> Executors.newFixedThreadPool(threads);
  }

  //visible for testing
  TransferManagerBuilder createTransferManagerBuilder() {
    return TransferManagerBuilder.standard();
  }

  //visible for testing
  TransferManager createTransferManager(AmazonS3 s3Client) {
    return createTransferManagerBuilder().withS3Client(s3Client)
                                         .withExecutorFactory(
                                             createExecutorFactory(transferManagerConfigs.getThreads())
                                         )
                                         .withShutDownThreadPools(true)
                                         .withMinimumUploadPartSize(transferManagerConfigs.getMinimumUploadPartSize())
                                         .withMultipartUploadThreshold(transferManagerConfigs
                                             .getMultipartUploadThreshold())
                                         .build();
  }

  public interface EncryptionMetadataBuilder {

    ObjectMetadata build();
  }

  public EncryptionMetadataBuilder createEncryptionMetadataBuilder() {
    return () -> {
      ObjectMetadata metadata = null;
      if (sseConfigs != null) {
        switch (sseConfigs.getEncryption()) {
          case NONE:
            metadata = null;
            break;
          case S3:
            metadata = new ObjectMetadata();
            metadata.setSSEAlgorithm(SSEAlgorithm.AES256.getAlgorithm());
            break;
          case KMS:
            metadata = new ObjectMetadata();
            metadata.setSSEAlgorithm(SSEAlgorithm.KMS.getAlgorithm());
            metadata.setHeader(Headers.SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID, sseConfigs.getKmsKeyId().get());
            Map<String, CredentialValue> encryptionContext = sseConfigs.getEncryptionContext();
            if (encryptionContext != null && !encryptionContext.isEmpty()) {
              Map<String, String> plainEncryptionContext = new HashMap<>();
              for (Map.Entry<String, CredentialValue> entry : encryptionContext.entrySet()) {
                // Don't include items with empty keys
                if (entry.getKey() != null && !entry.getKey().isEmpty()) {
                  plainEncryptionContext.put(entry.getKey(), entry.getValue().get());
                }
              }
              // Don't bother if all of the items had empty keys
              if (!plainEncryptionContext.isEmpty()) {
                try {
                  // This needs to be Bas64-encoded JSON
                  String json = new ObjectMapper().writeValueAsString(plainEncryptionContext);
                  String bas64Json = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
                  metadata.setHeader("x-amz-server-side-encryption-context", bas64Json);
                } catch (JsonProcessingException e) {
                  throw new StageException(Errors.AWS_01, e);
                }
              }
            }
            break;
          case CUSTOMER:
            metadata = new ObjectMetadata();
            metadata.setSSECustomerAlgorithm(SSEAlgorithm.AES256.getAlgorithm());
            metadata.setHeader(Headers.SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY, sseConfigs.getCustomerKey().get());
            metadata.setHeader(Headers.COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
                sseConfigs.getCustomerKeyMd5().get()
            );
            break;
          default:
            throw new IllegalArgumentException(String.format("Invalid encryption option '%s'",
                sseConfigs.getEncryption()
            ));
        }
      }
      return metadata;
    };
  }

  //visible for testing
  static class UploaderProgressListener implements ProgressListener {
    private final String object;

    public UploaderProgressListener(String object) {
      this.object = object;
    }

    @Override
    public void progressChanged(ProgressEvent progressEvent) {
      switch (progressEvent.getEventType()) {
        case TRANSFER_STARTED_EVENT:
          LOG.debug("Started uploading object {} into Amazon S3", object);
          break;
        case TRANSFER_COMPLETED_EVENT:
          LOG.debug("Completed uploading object {} into Amazon S3", object);
          break;
        case TRANSFER_FAILED_EVENT:
          LOG.warn("Failed uploading object {} into Amazon S3", object);
          break;
        default:
          break;
      }

    }
  }

}
