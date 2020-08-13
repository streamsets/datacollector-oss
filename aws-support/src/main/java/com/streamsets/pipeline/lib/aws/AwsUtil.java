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
package com.streamsets.pipeline.lib.aws;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;

public class AwsUtil {
  public static AWSCredentialsProvider getCredentialsProvider(
      CredentialValue accessKeyId,
      CredentialValue secretKey
  ) throws StageException {
    AWSCredentialsProvider credentialsProvider;
    if (accessKeyId != null && secretKey != null && !accessKeyId.get().isEmpty() && !secretKey.get().isEmpty()) {
      credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(
          accessKeyId.get(),
          secretKey.get()
      ));
    } else {
      credentialsProvider = new DefaultAWSCredentialsProviderChain();
    }
    return credentialsProvider;
  }

  public static AwsRequestSigningApacheInterceptor getAwsSigV4Interceptor(String awsServiceName,
                                                                          AwsRegion awsRegion,
                                                                          String otherEndpoint,
                                                                          CredentialValue awsAccessKeyId,
                                                                          CredentialValue awsSecretAccessKey)
      throws StageException {
    AWS4Signer signer = new AWS4Signer();
    signer.setServiceName(awsServiceName);

    if (awsRegion == AwsRegion.OTHER) {
      if (otherEndpoint == null || otherEndpoint.isEmpty()) {
        return null;
      }
      signer.setRegionName(otherEndpoint);
    } else {
      signer.setRegionName(awsRegion.getId());
    }

    return new AwsRequestSigningApacheInterceptor(
        awsServiceName,
        signer,
        AwsUtil.getCredentialsProvider(awsAccessKeyId, awsSecretAccessKey));
  }
}
