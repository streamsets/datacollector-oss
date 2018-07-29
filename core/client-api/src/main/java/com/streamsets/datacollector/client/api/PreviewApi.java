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
package com.streamsets.datacollector.client.api;

import com.streamsets.datacollector.client.ApiClient;
import com.streamsets.datacollector.client.ApiException;
import com.streamsets.datacollector.client.Configuration;
import com.streamsets.datacollector.client.Pair;
import com.streamsets.datacollector.client.TypeRef;
import com.streamsets.datacollector.client.model.PreviewInfoJson;
import com.streamsets.datacollector.client.model.PreviewOutputJson;
import com.streamsets.datacollector.client.model.RawPreview;
import com.streamsets.datacollector.client.model.StageOutputJson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PreviewApi {
  private ApiClient apiClient;

  public PreviewApi() {
    this(Configuration.getDefaultApiClient());
  }

  public PreviewApi(ApiClient apiClient) {
    this.apiClient = apiClient;
  }

  public ApiClient getApiClient() {
    return apiClient;
  }

  public void setApiClient(ApiClient apiClient) {
    this.apiClient = apiClient;
  }


  /**
   * Run Pipeline preview
   *
   * @param pipelineId
   * @param stageOutputsToOverrideJson
   * @param rev
   * @param batchSize
   * @param batches
   * @param skipTargets
   * @param endStage
   * @param timeout
   * @return PreviewInfoJson
   */
  public PreviewInfoJson previewWithOverride (String pipelineId, List<StageOutputJson> stageOutputsToOverrideJson,
                                              String rev, Integer batchSize, Integer batches, Boolean skipTargets,
                                              String endStage, Long timeout) throws ApiException {
    Object postBody = stageOutputsToOverrideJson;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling previewWithOverride");
    }

    // verify the required parameter 'stageOutputsToOverrideJson' is set
    if (stageOutputsToOverrideJson == null) {
      throw new ApiException(400,
          "Missing the required parameter 'stageOutputsToOverrideJson' when calling previewWithOverride");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/preview".replaceAll("\\{format\\}","json")
      .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "rev", rev));

    queryParams.addAll(apiClient.parameterToPairs("", "batchSize", batchSize));

    queryParams.addAll(apiClient.parameterToPairs("", "batches", batches));

    queryParams.addAll(apiClient.parameterToPairs("", "skipTargets", skipTargets));

    queryParams.addAll(apiClient.parameterToPairs("", "endStage", endStage));

    queryParams.addAll(apiClient.parameterToPairs("", "timeout", timeout));






    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<PreviewInfoJson>() {};
    return apiClient.invokeAPI(path, "POST", queryParams, postBody, postBinaryBody, headerParams, formParams, accept, contentType, authNames, returnType);




  }

  /**
   * Return Preview Data by previewer ID
   *
   * @param pipelineId
   * @param previewerId
   * @return PreviewOutputJson
   */
  public PreviewOutputJson getPreviewData (String pipelineId, String previewerId) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling getPreviewData");
    }

    // verify the required parameter 'previewerId' is set
    if (previewerId == null) {
      throw new ApiException(400, "Missing the required parameter 'previewerId' when calling getPreviewData");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/preview/{previewerId}".replaceAll("\\{format\\}","json")
      .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()))
      .replaceAll("\\{" + "previewerId" + "\\}", apiClient.escapeString(previewerId.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();







    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<PreviewOutputJson>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept, contentType, authNames, returnType);




  }

  /**
   * Stop Preview by previewer ID
   *
   * @param pipelineId
   * @param previewerId
   * @return PreviewInfoJson
   */
  public PreviewInfoJson stopPreview (String pipelineId, String previewerId) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling stopPreview");
    }

    // verify the required parameter 'previewerId' is set
    if (previewerId == null) {
      throw new ApiException(400, "Missing the required parameter 'previewerId' when calling stopPreview");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/preview/{previewerId}".replaceAll("\\{format\\}","json")
      .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()))
      .replaceAll("\\{" + "previewerId" + "\\}", apiClient.escapeString(previewerId.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();







    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<PreviewInfoJson>() {};
    return apiClient.invokeAPI(path, "DELETE", queryParams, postBody, postBinaryBody, headerParams, formParams, accept, contentType, authNames, returnType);




  }

  /**
   * Return Preview status by previewer ID
   *
   * @param pipelineId
   * @param previewerId
   * @return PreviewInfoJson
   */
  public PreviewInfoJson getPreviewStatus (String pipelineId, String previewerId) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling getPreviewStatus");
    }

    // verify the required parameter 'previewerId' is set
    if (previewerId == null) {
      throw new ApiException(400, "Missing the required parameter 'previewerId' when calling getPreviewStatus");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/preview/{previewerId}/status".replaceAll("\\{format\\}","json")
      .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()))
      .replaceAll("\\{" + "previewerId" + "\\}", apiClient.escapeString(previewerId.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();







    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<PreviewInfoJson>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept, contentType, authNames, returnType);




  }

  /**
   * Get raw source preview data for pipeline name and revision
   *
   * @param pipelineId
   * @param rev
   * @return RawPreview
   */
  public RawPreview rawSourcePreview (String pipelineId, String rev) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling rawSourcePreview");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/rawSourcePreview".replaceAll("\\{format\\}","json")
      .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "rev", rev));






    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<RawPreview>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept, contentType, authNames, returnType);




  }

  /**
   * Validate pipeline configuration and return validation status and issues
   *
   * @param pipelineId
   * @param rev
   * @param timeout
   * @return PreviewInfoJson
   */
  public PreviewInfoJson validateConfigs (String pipelineId, String rev, Long timeout) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling validateConfigs");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/validate".replaceAll("\\{format\\}","json")
      .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "rev", rev));

    queryParams.addAll(apiClient.parameterToPairs("", "timeout", timeout));






    final String[] accepts = {
      "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<PreviewInfoJson>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept, contentType, authNames, returnType);




  }

}
