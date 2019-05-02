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
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.aws.SseOption;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class TestS3Accessor {

  @Test
  public void testCreateCredentials() throws Exception {
    CredentialsConfigs credentialsConfigs = new CredentialsConfigs() {
      @Override
      public CredentialValue getAccessKey() {
        return () -> "access";
      }

      @Override
      public CredentialValue getSecretKey() {
        return () -> "secret";
      }
    };
    S3Accessor accessor = new S3Accessor(credentialsConfigs, null, null, null);

    AWSCredentialsProvider provider = accessor.createCredentialsProvider();
    AWSCredentials credentials = provider.getCredentials();
    Assert.assertEquals("access", credentials.getAWSAccessKeyId());
    Assert.assertEquals("secret", credentials.getAWSSecretKey());
  }

  @Test
  public void testCreateClientConfiguration() throws Exception {

    // no proxy
    ConnectionConfigs connectionConfigs = new ConnectionConfigs() {
      @Override
      public int getConnectionTimeoutMillis() {
        return 1;
      }

      @Override
      public int getSocketTimeoutMillis() {
        return 2;
      }

      @Override
      public int getMaxErrorRetry() {
        return 3;
      }

      @Override
      public boolean isChunkedEncodingEnabled() {
        return true;
      }

      @Override
      public boolean isProxyEnabled() {
        return false;
      }

      @Override
      public String getProxyHost() {
        return "host";
      }

      @Override
      public int getProxyPort() {
        return 4;
      }

      @Override
      public boolean isProxyAuthenticationEnabled() {
        return false;
      }

      @Override
      public CredentialValue getProxyUser() {
        return () -> "user";
      }

      @Override
      public CredentialValue getProxyPassword() {
        return () -> "password";
      }

      @Override
      public boolean isUseEndpoint() {
        return false;
      }

      @Override
      public String getEndpoint() {
        return null;
      }

      @Override
      public String getRegion() {
        return "region";
      }
    };

    S3Accessor accessor = new S3Accessor(null, connectionConfigs, null, null);

    ClientConfiguration configuration = accessor.createClientConfiguration();
    Assert.assertEquals(1, configuration.getConnectionTimeout());
    Assert.assertEquals(2, configuration.getSocketTimeout());
    Assert.assertEquals(3, configuration.getMaxErrorRetry());
    Assert.assertEquals(null, configuration.getProxyHost());
    Assert.assertEquals(-1, configuration.getProxyPort());
    Assert.assertEquals(null, configuration.getProxyUsername());
    Assert.assertEquals(null, configuration.getProxyPassword());

    // proxy
    connectionConfigs = new ConnectionConfigs() {
      @Override
      public int getConnectionTimeoutMillis() {
        return 1;
      }

      @Override
      public int getSocketTimeoutMillis() {
        return 2;
      }

      @Override
      public int getMaxErrorRetry() {
        return 3;
      }

      @Override
      public boolean isChunkedEncodingEnabled() {
        return true;
      }

      @Override
      public boolean isProxyEnabled() {
        return true;
      }

      @Override
      public String getProxyHost() {
        return "host";
      }

      @Override
      public int getProxyPort() {
        return 4;
      }

      @Override
      public boolean isProxyAuthenticationEnabled() {
        return false;
      }

      @Override
      public CredentialValue getProxyUser() {
        return () -> "user";
      }

      @Override
      public CredentialValue getProxyPassword() {
        return () -> "password";
      }

      @Override
      public boolean isUseEndpoint() {
        return false;
      }

      @Override
      public String getEndpoint() {
        return null;
      }

      @Override
      public String getRegion() {
        return "region";
      }
    };

    accessor = new S3Accessor(null, connectionConfigs, null, null);

    configuration = accessor.createClientConfiguration();
    Assert.assertEquals(1, configuration.getConnectionTimeout());
    Assert.assertEquals(2, configuration.getSocketTimeout());
    Assert.assertEquals(3, configuration.getMaxErrorRetry());
    Assert.assertEquals("host", configuration.getProxyHost());
    Assert.assertEquals(4, configuration.getProxyPort());
    Assert.assertEquals(null, configuration.getProxyUsername());
    Assert.assertEquals(null, configuration.getProxyPassword());

    // proxy auth
    connectionConfigs = new ConnectionConfigs() {
      @Override
      public int getConnectionTimeoutMillis() {
        return 1;
      }

      @Override
      public int getSocketTimeoutMillis() {
        return 2;
      }

      @Override
      public int getMaxErrorRetry() {
        return 3;
      }

      @Override
      public boolean isChunkedEncodingEnabled() {
        return true;
      }

      @Override
      public boolean isProxyEnabled() {
        return true;
      }

      @Override
      public String getProxyHost() {
        return "host";
      }

      @Override
      public int getProxyPort() {
        return 4;
      }

      @Override
      public boolean isProxyAuthenticationEnabled() {
        return true;
      }

      @Override
      public CredentialValue getProxyUser() {
        return () -> "user";
      }

      @Override
      public CredentialValue getProxyPassword() {
        return () -> "password";
      }

      @Override
      public boolean isUseEndpoint() {
        return false;
      }

      @Override
      public String getEndpoint() {
        return null;
      }

      @Override
      public String getRegion() {
        return "region";
      }
    };

    accessor = new S3Accessor(null, connectionConfigs, null, null);

    configuration = accessor.createClientConfiguration();
    Assert.assertEquals(1, configuration.getConnectionTimeout());
    Assert.assertEquals(2, configuration.getSocketTimeout());
    Assert.assertEquals(3, configuration.getMaxErrorRetry());
    Assert.assertEquals("host", configuration.getProxyHost());
    Assert.assertEquals(4, configuration.getProxyPort());
    Assert.assertEquals("user", configuration.getProxyUsername());
    Assert.assertEquals("password", configuration.getProxyPassword());
  }

  @Test
  public void testCreateS3Client() throws Exception {
    // null credentials
    CredentialsConfigs credentialsConfigs = new CredentialsConfigs() {
      @Override
      public CredentialValue getAccessKey() {
        return () -> null;
      }

      @Override
      public CredentialValue getSecretKey() {
        return () -> "";
      }
    };

    //no endpoint
    ConnectionConfigs connectionConfigs = new ConnectionConfigs() {
      @Override
      public int getConnectionTimeoutMillis() {
        return 1;
      }

      @Override
      public int getSocketTimeoutMillis() {
        return 2;
      }

      @Override
      public int getMaxErrorRetry() {
        return 3;
      }

      @Override
      public boolean isChunkedEncodingEnabled() {
        return true;
      }

      @Override
      public boolean isProxyEnabled() {
        return false;
      }

      @Override
      public String getProxyHost() {
        return "host";
      }

      @Override
      public int getProxyPort() {
        return 4;
      }

      @Override
      public boolean isProxyAuthenticationEnabled() {
        return false;
      }

      @Override
      public CredentialValue getProxyUser() {
        return () -> "user";
      }

      @Override
      public CredentialValue getProxyPassword() {
        return () -> "password";
      }

      @Override
      public boolean isUseEndpoint() {
        return false;
      }

      @Override
      public String getEndpoint() {
        return null;
      }

      @Override
      public String getRegion() {
        return Region.US_West.getFirstRegionId();
      }
    };

    S3Accessor accessor = new S3Accessor(credentialsConfigs, connectionConfigs, null, null);
    AmazonS3Client s3Client = null;
    try {
      accessor = Mockito.spy(accessor);
      AWSCredentialsProvider provider = accessor.createCredentialsProvider();
      Mockito.doReturn(provider).when(accessor).getCredentialsProvider();

      s3Client = accessor.createS3Client();

      Mockito.verify(accessor, Mockito.times(1)).getCredentialsProvider();
      Mockito.verify(accessor, Mockito.times(1)).createAmazonS3ClientBuilder();
      Mockito.verify(accessor, Mockito.times(1)).createClientConfiguration();

      Assert.assertEquals(Region.US_West, s3Client.getRegion());
      Assert.assertNull(accessor.getCredentialsProvider());
    } finally {
      if (s3Client != null) {
        s3Client.shutdown();
      }
    }

    // not null credentials
    credentialsConfigs = new CredentialsConfigs() {
      @Override
      public CredentialValue getAccessKey() {
        return () -> "access";
      }

      @Override
      public CredentialValue getSecretKey() {
        return () -> "secret";
      }
    };

    accessor = new S3Accessor(credentialsConfigs, connectionConfigs, null, null);
    s3Client = null;
    try {
      accessor = Mockito.spy(accessor);
      AWSCredentialsProvider provider = accessor.createCredentialsProvider();
      Mockito.doReturn(provider).when(accessor).getCredentialsProvider();

      s3Client = accessor.createS3Client();

      Mockito.verify(accessor, Mockito.times(1)).getCredentialsProvider();
      Mockito.verify(accessor, Mockito.times(1)).createAmazonS3ClientBuilder();
      Mockito.verify(accessor, Mockito.times(1)).createClientConfiguration();

      Assert.assertEquals(Region.US_West, s3Client.getRegion());
    } finally {
      if (s3Client != null) {
        s3Client.shutdown();
      }
    }

    //endpoint with signer override
    connectionConfigs = new ConnectionConfigs() {
      @Override
      public int getConnectionTimeoutMillis() {
        return 1;
      }

      @Override
      public int getSocketTimeoutMillis() {
        return 2;
      }

      @Override
      public int getMaxErrorRetry() {
        return 3;
      }

      @Override
      public boolean isChunkedEncodingEnabled() {
        return true;
      }

      @Override
      public boolean isProxyEnabled() {
        return false;
      }

      @Override
      public String getProxyHost() {
        return "host";
      }

      @Override
      public int getProxyPort() {
        return 4;
      }

      @Override
      public boolean isProxyAuthenticationEnabled() {
        return false;
      }

      @Override
      public CredentialValue getProxyUser() {
        return () -> "user";
      }

      @Override
      public CredentialValue getProxyPassword() {
        return () -> "password";
      }

      @Override
      public boolean isUseEndpoint() {
        return true;
      }

      @Override
      public String getEndpoint() {
        return "http://foo";
      }

      @Override
      public String getRegion() {
        return "xyz";
      }
    };

    accessor = new S3Accessor(credentialsConfigs, connectionConfigs, null, null);
    s3Client = null;
    try {
      accessor = Mockito.spy(accessor);
      AWSCredentialsProvider provider = accessor.createCredentialsProvider();
      Mockito.doReturn(provider).when(accessor).getCredentialsProvider();

      s3Client = accessor.createS3Client();

      Assert.assertEquals("xyz", s3Client.getSignerRegionOverride());

      URL url = s3Client.getUrl("b", "k");
      Assert.assertTrue(url.toExternalForm().startsWith("http://foo"));
    } finally {
      if (s3Client != null) {
        s3Client.shutdown();
      }
    }

    //endpoint no signer override
    connectionConfigs = new ConnectionConfigs() {
      @Override
      public int getConnectionTimeoutMillis() {
        return 1;
      }

      @Override
      public int getSocketTimeoutMillis() {
        return 2;
      }

      @Override
      public int getMaxErrorRetry() {
        return 3;
      }

      @Override
      public boolean isChunkedEncodingEnabled() {
        return true;
      }

      @Override
      public boolean isProxyEnabled() {
        return false;
      }

      @Override
      public String getProxyHost() {
        return "host";
      }

      @Override
      public int getProxyPort() {
        return 4;
      }

      @Override
      public boolean isProxyAuthenticationEnabled() {
        return false;
      }

      @Override
      public CredentialValue getProxyUser() {
        return () -> "user";
      }

      @Override
      public CredentialValue getProxyPassword() {
        return () -> "password";
      }

      @Override
      public boolean isUseEndpoint() {
        return true;
      }

      @Override
      public String getEndpoint() {
        return "http://foo";
      }

      @Override
      public String getRegion() {
        return "";
      }
    };

    accessor = new S3Accessor(credentialsConfigs, connectionConfigs, null, null);
    s3Client = null;
    try {
      accessor = Mockito.spy(accessor);
      AWSCredentialsProvider provider = accessor.createCredentialsProvider();
      Mockito.doReturn(provider).when(accessor).getCredentialsProvider();

      s3Client = accessor.createS3Client();

      Assert.assertEquals(null, s3Client.getSignerRegionOverride());

      URL url = s3Client.getUrl("b", "k");
      Assert.assertTrue(url.toExternalForm().startsWith("http://foo"));
    } finally {
      if (s3Client != null) {
        s3Client.shutdown();
      }
    }

  }

  @Test
  public void testCreateTransferManager() throws Exception {
    CredentialsConfigs credentialsConfigs = new CredentialsConfigs() {
      @Override
      public CredentialValue getAccessKey() {
        return () -> "access";
      }

      @Override
      public CredentialValue getSecretKey() {
        return () -> "secret";
      }
    };

    //no endpoint
    ConnectionConfigs connectionConfigs = new ConnectionConfigs() {
      @Override
      public int getConnectionTimeoutMillis() {
        return 1;
      }

      @Override
      public int getSocketTimeoutMillis() {
        return 2;
      }

      @Override
      public int getMaxErrorRetry() {
        return 3;
      }

      @Override
      public boolean isChunkedEncodingEnabled() {
        return true;
      }

      @Override
      public boolean isProxyEnabled() {
        return false;
      }

      @Override
      public String getProxyHost() {
        return "host";
      }

      @Override
      public int getProxyPort() {
        return 4;
      }

      @Override
      public boolean isProxyAuthenticationEnabled() {
        return false;
      }

      @Override
      public CredentialValue getProxyUser() {
        return () -> "user";
      }

      @Override
      public CredentialValue getProxyPassword() {
        return () -> "password";
      }

      @Override
      public boolean isUseEndpoint() {
        return false;
      }

      @Override
      public String getEndpoint() {
        return null;
      }

      @Override
      public String getRegion() {
        return Region.US_West.getFirstRegionId();
      }
    };

    TransferManagerConfigs transferManagerConfigs = new TransferManagerConfigs() {
      @Override
      public int getThreads() {
        return 5;
      }

      @Override
      public long getMinimumUploadPartSize() {
        return 6;
      }

      @Override
      public long getMultipartUploadThreshold() {
        return 7;
      }
    };

    S3Accessor accessor = new S3Accessor(credentialsConfigs, connectionConfigs, transferManagerConfigs, null);
    accessor = Mockito.spy(accessor);
    ExecutorFactory executorFactory = Mockito.mock(ExecutorFactory.class);
    ExecutorService executorService = Mockito.mock(ExecutorService.class);
    Mockito.when(executorFactory.newExecutor()).thenReturn(executorService);
    Mockito.doReturn(executorFactory).when(accessor).createExecutorFactory(Mockito.eq(5));

    AmazonS3Client s3Client = Mockito.mock(AmazonS3Client.class);

    TransferManager transferManager = accessor.createTransferManager(s3Client);
    try {
      Mockito.verify(accessor, Mockito.times(1)).createExecutorFactory(5);

      Assert.assertEquals(6, transferManager.getConfiguration().getMinimumUploadPartSize());
      Assert.assertEquals(7, transferManager.getConfiguration().getMultipartUploadThreshold());
    } finally {
      transferManager.shutdownNow(true);
      Mockito.verify(executorService, Mockito.times(1)).shutdownNow();
    }
  }

  @Test
  public void testEncryptionMetadata() throws Exception {

    //null
    S3Accessor accessor = new S3Accessor(null, null, null, null);
    Assert.assertNull(accessor.createEncryptionMetadataBuilder().build());

    //NONE
    SseConfigs configs = new SseConfigs() {
      @Override
      public SseOption getEncryption() {
        return SseOption.NONE;
      }

      @Override
      public Map<String, CredentialValue> getEncryptionContext() {
        return null;
      }

      @Override
      public CredentialValue getKmsKeyId() {
        return null;
      }

      @Override
      public CredentialValue getCustomerKey() {
        return null;
      }

      @Override
      public CredentialValue getCustomerKeyMd5() {
        return null;
      }
    };

    accessor = new S3Accessor(null, null, null, configs);
    Assert.assertNull(accessor.createEncryptionMetadataBuilder().build());

    //S3
    configs = new SseConfigs() {
      @Override
      public SseOption getEncryption() {
        return SseOption.S3;
      }

      @Override
      public Map<String, CredentialValue> getEncryptionContext() {
        return Collections.emptyMap();
      }

      @Override
      public CredentialValue getKmsKeyId() {
        return () -> "kms";
      }

      @Override
      public CredentialValue getCustomerKey() {
        return null;
      }

      @Override
      public CredentialValue getCustomerKeyMd5() {
        return null;
      }
    };

    accessor = new S3Accessor(null, null, null, configs);
    ObjectMetadata metadata = accessor.createEncryptionMetadataBuilder().build();
    Assert.assertNotNull(metadata);
    Assert.assertEquals(SSEAlgorithm.AES256.getAlgorithm(), metadata.getSSEAlgorithm());

    //KMS
    configs = new SseConfigs() {
      @Override
      public SseOption getEncryption() {
        return SseOption.KMS;
      }

      @Override
      public Map<String, CredentialValue> getEncryptionContext() {
        return ImmutableMap.of(
            "x", () -> "X",
            "", () -> "empty-key",
            "y", () -> "Y"
        );
      }

      @Override
      public CredentialValue getKmsKeyId() {
        return () -> "kms";
      }

      @Override
      public CredentialValue getCustomerKey() {
        return null;
      }

      @Override
      public CredentialValue getCustomerKeyMd5() {
        return null;
      }
    };

    accessor = new S3Accessor(null, null, null, configs);
    metadata = accessor.createEncryptionMetadataBuilder().build();
    Assert.assertNotNull(metadata);
    Assert.assertEquals(SSEAlgorithm.KMS.getAlgorithm(), metadata.getSSEAlgorithm());
    Assert.assertEquals("kms", metadata.getRawMetadataValue(Headers.SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID));
    Object encryptionContext = metadata.getRawMetadataValue("x-amz-server-side-encryption-context");
    Assert.assertNotNull(encryptionContext);
    String json = new String(Base64.getDecoder().decode(String.valueOf(encryptionContext)), StandardCharsets.UTF_8);
    Assert.assertEquals("{\"x\":\"X\",\"y\":\"Y\"}", json);
  }

  @Test
  public void testEncryptionContextEmpty() throws Exception {
    SseConfigs configs = new SseConfigs() {
      @Override
      public SseOption getEncryption() {
        return SseOption.KMS;
      }

      @Override
      public Map<String, CredentialValue> getEncryptionContext() {
        return Collections.emptyMap();
      }

      @Override
      public CredentialValue getKmsKeyId() {
        return () -> "kms";
      }

      @Override
      public CredentialValue getCustomerKey() {
        return null;
      }

      @Override
      public CredentialValue getCustomerKeyMd5() {
        return null;
      }
    };

    S3Accessor accessor = new S3Accessor(null, null, null, configs);
    ObjectMetadata metadata = accessor.createEncryptionMetadataBuilder().build();
    Assert.assertNotNull(metadata);
    Assert.assertNull(metadata.getRawMetadataValue("x-amz-server-side-encryption-context"));

    configs = new SseConfigs() {
      @Override
      public SseOption getEncryption() {
        return SseOption.KMS;
      }

      @Override
      public Map<String, CredentialValue> getEncryptionContext() {
        return ImmutableMap.of("", () -> "empty-key");
      }

      @Override
      public CredentialValue getKmsKeyId() {
        return () -> "kms";
      }

      @Override
      public CredentialValue getCustomerKey() {
        return null;
      }

      @Override
      public CredentialValue getCustomerKeyMd5() {
        return null;
      }
    };

    accessor = new S3Accessor(null, null, null, configs);
    metadata = accessor.createEncryptionMetadataBuilder().build();
    Assert.assertNotNull(metadata);
    Assert.assertNull(metadata.getRawMetadataValue("x-amz-server-side-encryption-context"));
  }

  @Test
  public void testS3AccessorInitClose() throws Exception {
    CredentialsConfigs credentialsConfigs = new CredentialsConfigs() {
      @Override
      public CredentialValue getAccessKey() {
        return () -> "access";
      }

      @Override
      public CredentialValue getSecretKey() {
        return () -> "secret";
      }
    };

    //no endpoint
    ConnectionConfigs connectionConfigs = new ConnectionConfigs() {
      @Override
      public int getConnectionTimeoutMillis() {
        return 1;
      }

      @Override
      public int getSocketTimeoutMillis() {
        return 2;
      }

      @Override
      public int getMaxErrorRetry() {
        return 3;
      }

      @Override
      public boolean isChunkedEncodingEnabled() {
        return true;
      }

      @Override
      public boolean isProxyEnabled() {
        return false;
      }

      @Override
      public String getProxyHost() {
        return "host";
      }

      @Override
      public int getProxyPort() {
        return 4;
      }

      @Override
      public boolean isProxyAuthenticationEnabled() {
        return false;
      }

      @Override
      public CredentialValue getProxyUser() {
        return () -> "user";
      }

      @Override
      public CredentialValue getProxyPassword() {
        return () -> "password";
      }

      @Override
      public boolean isUseEndpoint() {
        return false;
      }

      @Override
      public String getEndpoint() {
        return null;
      }

      @Override
      public String getRegion() {
        return Region.US_West.getFirstRegionId();
      }
    };

    TransferManagerConfigs transferManagerConfigs = new TransferManagerConfigs() {
      @Override
      public int getThreads() {
        return 5;
      }

      @Override
      public long getMinimumUploadPartSize() {
        return 6;
      }

      @Override
      public long getMultipartUploadThreshold() {
        return 7;
      }
    };

    S3Accessor accessor = new S3Accessor(credentialsConfigs, connectionConfigs, transferManagerConfigs, null);
    try {
      accessor.init();
      Assert.assertNotNull(accessor.getCredentialsProvider());
      Assert.assertNotNull(accessor.getS3Client());
      Assert.assertNotNull(accessor.getTransferManager());
      Assert.assertNotNull(accessor.getEncryptionMetadataBuilder());
      Assert.assertNotNull(accessor.getUploader());
    } finally {
      accessor.close();
    }
    Assert.assertNull(accessor.getS3Client());
    Assert.assertFalse(accessor.hasTransferManager());
  }

  @Test
  public void testUploader() throws Exception {
    S3Accessor accessor = new S3Accessor(null, null, null, null);
    accessor = Mockito.spy(accessor);
    Mockito.doReturn(true).when(accessor).hasTransferManager();
    S3Accessor.EncryptionMetadataBuilder metadataBuilder = Mockito.mock(S3Accessor.EncryptionMetadataBuilder.class);
    ObjectMetadata metadata = new ObjectMetadata();
    Mockito.when(metadataBuilder.build()).thenReturn(metadata);
    Mockito.doReturn(metadataBuilder).when(accessor).getEncryptionMetadataBuilder();

    TransferManager transferManager = Mockito.mock(TransferManager.class);
    Mockito.doReturn(transferManager).when(accessor).getTransferManager();
    Upload upload = Mockito.mock(Upload.class);
    Mockito.when(transferManager.upload(Mockito.any(PutObjectRequest.class))).thenReturn(upload);

    S3Accessor.Uploader uploader = accessor.getUploader();
    Assert.assertNotNull(uploader);

    InputStream is = Mockito.mock(InputStream.class);

    Upload uploadGot = uploader.upload("b", "k", is);

    Assert.assertEquals(upload, uploadGot);

    ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
    Mockito.verify(transferManager, Mockito.times(1)).upload(captor.capture());
    PutObjectRequest request = captor.getValue();
    Assert.assertEquals("b", request.getBucketName());
    Assert.assertEquals("k", request.getKey());
    Assert.assertEquals(is, request.getInputStream());
    Assert.assertEquals(metadata, request.getMetadata());
  }

}
