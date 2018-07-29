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
import com.streamsets.datacollector.client.model.AlertInfoJson;
import com.streamsets.datacollector.client.model.ErrorMessageJson;
import com.streamsets.datacollector.client.model.MetricRegistryJson;
import com.streamsets.datacollector.client.model.PipelineStateJson;
import com.streamsets.datacollector.client.model.RecordJson;
import com.streamsets.datacollector.client.model.SampledRecordJson;
import com.streamsets.datacollector.client.model.SnapshotDataJson;
import com.streamsets.datacollector.client.model.SnapshotInfoJson;
import com.streamsets.datacollector.client.model.SourceOffsetJson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ManagerApi {
  private ApiClient apiClient;

  public ManagerApi() {
    this(Configuration.getDefaultApiClient());
  }

  public ManagerApi(ApiClient apiClient) {
    this.apiClient = apiClient;
  }

  public ApiClient getApiClient() {
    return apiClient;
  }

  public void setApiClient(ApiClient apiClient) {
    this.apiClient = apiClient;
  }


  /**
   * Delete alert by Pipeline name, revision and Alert ID
   *
   * @param pipelineId
   * @param rev
   * @param alertId
   * @return Boolean
   */
  public Boolean deleteAlert (String pipelineId, String rev, String alertId) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling deleteAlert");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/alerts".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "rev", rev));

    queryParams.addAll(apiClient.parameterToPairs("", "alertId", alertId));






    final String[] accepts = {
        "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<Boolean>() {};
    return apiClient.invokeAPI(path, "DELETE", queryParams, postBody, postBinaryBody, headerParams, formParams, accept, contentType, authNames, returnType);




  }

  /**
   * Returns error messages by stage instance name and size
   *
   * @param pipelineId
   * @param rev
   * @param stageInstanceName
   * @param size
   * @return List<ErrorMessageJson>
   */
  public List<ErrorMessageJson> getErrorMessages (String pipelineId, String rev, String stageInstanceName, Integer size) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling getErrorMessages");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/errorMessages".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "rev", rev));

    queryParams.addAll(apiClient.parameterToPairs("", "stageInstanceName", stageInstanceName));

    queryParams.addAll(apiClient.parameterToPairs("", "size", size));






    final String[] accepts = {
        "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<List<ErrorMessageJson>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);




  }

  /**
   * Returns error records by stage instance name and size
   *
   * @param pipelineId
   * @param rev
   * @param stageInstanceName
   * @param size
   * @return List<RecordJson>
   */
  public List<RecordJson> getErrorRecords (String pipelineId, String rev, String stageInstanceName, Integer size)
      throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling getErrorRecords");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/errorRecords".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "rev", rev));

    queryParams.addAll(apiClient.parameterToPairs("", "stageInstanceName", stageInstanceName));

    queryParams.addAll(apiClient.parameterToPairs("", "size", size));






    final String[] accepts = {
        "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<List<RecordJson>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);




  }

  /**
   * Find history by pipeline name
   *
   * @param pipelineId
   * @param rev
   * @param fromBeginning
   * @return List<PipelineStateJson>
   */
  public List<PipelineStateJson> getHistory (String pipelineId, String rev, Boolean fromBeginning) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling getHistory");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/history".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "rev", rev));

    queryParams.addAll(apiClient.parameterToPairs("", "fromBeginning", fromBeginning));






    final String[] accepts = {
        "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<List<PipelineStateJson>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);




  }

  /**
   * Delete history by pipeline name
   *
   * @param pipelineId
   * @param rev
   * @return void
   */
  public void deleteHistory (String pipelineId, String rev) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling deleteHistory");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/history".replaceAll("\\{format\\}","json")
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





    apiClient.invokeAPI(path, "DELETE", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, null);




  }

  /**
   * Return Pipeline Metrics
   *
   * @param pipelineId
   * @param rev
   * @return MetricRegistryJson
   */
  public MetricRegistryJson getMetrics (String pipelineId, String rev) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling getMetrics");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/metrics".replaceAll("\\{format\\}","json")
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





    TypeRef returnType = new TypeRef<MetricRegistryJson>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);




  }

  /**
   * Reset Origin Offset
   *
   * @param pipelineId
   * @param rev
   * @return void
   */
  public void resetOffset (String pipelineId, String rev) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling resetOffset");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/resetOffset".replaceAll("\\{format\\}","json")
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





    apiClient.invokeAPI(path, "POST", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, null);




  }

  /**
   * Returns Sampled records by sample ID and size
   *
   * @param pipelineId
   * @param rev
   * @param sampleId
   * @param sampleSize
   * @return List<SampledRecordJson>
   */
  public List<SampledRecordJson> getSampledRecords (String pipelineId, String rev, String sampleId,
                                                    Integer sampleSize) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling getSampledRecords");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/sampledRecords".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "rev", rev));

    queryParams.addAll(apiClient.parameterToPairs("", "sampleId", sampleId));

    queryParams.addAll(apiClient.parameterToPairs("", "sampleSize", sampleSize));






    final String[] accepts = {
        "application/json"
    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<List<SampledRecordJson>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);




  }

  /**
   * Return Snapshot data
   *
   * @param pipelineId
   * @param snapshotName
   * @param rev
   * @return SnapshotDataJson
   */
  public SnapshotDataJson getSnapshot (String pipelineId, String snapshotName, String rev) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling getSnapshot");
    }

    // verify the required parameter 'snapshotName' is set
    if (snapshotName == null) {
      throw new ApiException(400, "Missing the required parameter 'snapshotName' when calling getSnapshot");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/snapshot/{snapshotName}".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()))
        .replaceAll("\\{" + "snapshotName" + "\\}", apiClient.escapeString(snapshotName.toString()));

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





    TypeRef returnType = new TypeRef<SnapshotDataJson>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);




  }

  /**
   * Capture Snapshot
   *
   * @param pipelineId
   * @param snapshotName
   * @param snapshotLabel
   * @param rev
   * @param batches
   * @param batchSize
   * @return void
   */
  public void captureSnapshot (String pipelineId, String snapshotName, String snapshotLabel, String rev,
                               Integer batches, Integer batchSize) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling captureSnapshot");
    }

    // verify the required parameter 'snapshotName' is set
    if (snapshotName == null) {
      throw new ApiException(400, "Missing the required parameter 'snapshotName' when calling captureSnapshot");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/snapshot/{snapshotName}".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()))
        .replaceAll("\\{" + "snapshotName" + "\\}", apiClient.escapeString(snapshotName.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "snapshotLabel", snapshotLabel));

    queryParams.addAll(apiClient.parameterToPairs("", "rev", rev));

    queryParams.addAll(apiClient.parameterToPairs("", "batches", batches));

    queryParams.addAll(apiClient.parameterToPairs("", "batchSize", batchSize));






    final String[] accepts = {

    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    apiClient.invokeAPI(path, "PUT", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, null);




  }

  /**
   * Delete Snapshot data
   *
   * @param pipelineId
   * @param snapshotName
   * @param rev
   * @return void
   */
  public void deleteSnapshot (String pipelineId, String snapshotName, String rev) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling deleteSnapshot");
    }

    // verify the required parameter 'snapshotName' is set
    if (snapshotName == null) {
      throw new ApiException(400, "Missing the required parameter 'snapshotName' when calling deleteSnapshot");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/snapshot/{snapshotName}".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()))
        .replaceAll("\\{" + "snapshotName" + "\\}", apiClient.escapeString(snapshotName.toString()));

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





    apiClient.invokeAPI(path, "DELETE", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, null);




  }

  /**
   * Return Snapshot status
   *
   * @param pipelineId
   * @param snapshotName
   * @param rev
   * @return SnapshotInfoJson
   */
  public SnapshotInfoJson getSnapshotStatus (String pipelineId, String snapshotName, String rev) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling getSnapshotStatus");
    }

    // verify the required parameter 'snapshotName' is set
    if (snapshotName == null) {
      throw new ApiException(400, "Missing the required parameter 'snapshotName' when calling getSnapshotStatus");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/snapshot/{snapshotName}/status".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()))
        .replaceAll("\\{" + "snapshotName" + "\\}", apiClient.escapeString(snapshotName.toString()));

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





    TypeRef returnType = new TypeRef<SnapshotInfoJson>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);




  }

  /**
   * Returns Snapshot Info for the given pipeline
   *
   * @param pipelineId
   * @param rev
   * @return List<SnapshotInfoJson>
   */
  public List<SnapshotInfoJson> getSnapshotsInfo (String pipelineId, String rev) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling getSnapshotsInfo");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/snapshots".replaceAll("\\{format\\}","json")
        .replaceAll("\\{" + "pipelineId" + "\\}", apiClient.escapeString(pipelineId.toString()));

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();


    queryParams.addAll(apiClient.parameterToPairs("", "rev", rev));






    final String[] accepts = {

    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<List<SnapshotInfoJson>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);




  }

  /**
   * Start Pipeline
   *
   * @param pipelineId
   * @param rev
   * @param runtimeParameters
   * @return PipelineStateJson
   */
  public PipelineStateJson startPipeline (
      String pipelineId,
      String rev,
      Map<String, Object> runtimeParameters
  ) throws ApiException {
    Object postBody = runtimeParameters;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling startPipeline");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/start".replaceAll("\\{format\\}","json")
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





    TypeRef returnType = new TypeRef<PipelineStateJson>() {};
    return apiClient.invokeAPI(path, "POST", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);




  }

  /**
   * Returns Pipeline Status for the given pipeline
   *
   * @param pipelineId
   * @param rev
   * @return PipelineStateJson
   */
  public PipelineStateJson getPipelineStatus (String pipelineId, String rev) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling getPipelineStatus");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/status".replaceAll("\\{format\\}","json")
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





    TypeRef returnType = new TypeRef<PipelineStateJson>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);




  }

  /**
   * Stop Pipeline
   *
   * @param pipelineId
   * @param rev
   * @return PipelineStateJson
   */
  public PipelineStateJson stopPipeline (String pipelineId, String rev) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling stopPipeline");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/stop".replaceAll("\\{format\\}","json")
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

    TypeRef returnType = new TypeRef<PipelineStateJson>() {};
    return apiClient.invokeAPI(path, "POST", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }


  /**
   * Force Stop Pipeline
   *
   * @param pipelineId
   * @param rev
   * @return PipelineStateJson
   */
  public PipelineStateJson forceStopPipeline (String pipelineId, String rev) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling stopPipeline");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/forceStop".replaceAll("\\{format\\}","json")
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

    TypeRef returnType = new TypeRef<PipelineStateJson>() {};
    return apiClient.invokeAPI(path, "POST", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);
  }

  /**
   * Returns alerts triggered for all pipelines
   *
   * @return List<AlertInfoJson>
   */
  public List<AlertInfoJson> getAllAlerts () throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/pipelines/alerts".replaceAll("\\{format\\}","json");

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();







    final String[] accepts = {

    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<List<AlertInfoJson>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);




  }

  /**
   * Returns all Snapshot Info
   *
   * @return List<SnapshotInfoJson>
   */
  public List<SnapshotInfoJson> getAllSnapshotsInfo () throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/pipelines/snapshots".replaceAll("\\{format\\}","json");

    // query params
    List<Pair> queryParams = new ArrayList<Pair>();
    Map<String, String> headerParams = new HashMap<String, String>();
    Map<String, Object> formParams = new HashMap<String, Object>();







    final String[] accepts = {

    };
    final String accept = apiClient.selectHeaderAccept(accepts);

    final String[] contentTypes = {

    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };





    TypeRef returnType = new TypeRef<List<SnapshotInfoJson>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, returnType);




  }

  /**
   * Returns all Pipeline Status
   *
   * @return Map<String, PipelineStateJson>
   */
  public Map<String, PipelineStateJson> getAllPipelineStatus () throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // create path and map variables
    String path = "/v1/pipelines/status".replaceAll("\\{format\\}","json");

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

    TypeRef returnType = new TypeRef<Map<String, PipelineStateJson>>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams,
        accept, contentType, authNames, returnType);

  }

  /**
   * Return Committed Offsets
   *
   * @return SourceOffsetJson
   */
  public SourceOffsetJson getCommittedOffsets(String pipelineId, String rev) throws ApiException {
    Object postBody = null;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling getCommittedOffsets");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/committedOffsets".replaceAll("\\{format\\}","json")
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

    TypeRef returnType = new TypeRef<SourceOffsetJson>() {};
    return apiClient.invokeAPI(path, "GET", queryParams, postBody, postBinaryBody, headerParams, formParams,
        accept, contentType, authNames, returnType);
  }

  /**
   * Update Pipeline Committed Offsets.
   *
   * @param pipelineId
   * @param rev
   * @param sourceOffset
   */
  public void updateCommittedOffsets(
      String pipelineId,
      String rev,
      SourceOffsetJson sourceOffset
  ) throws ApiException {
    Object postBody = sourceOffset;
    byte[] postBinaryBody = null;

    // verify the required parameter 'pipelineId' is set
    if (pipelineId == null) {
      throw new ApiException(400, "Missing the required parameter 'pipelineId' when calling updateCommittedOffsets");
    }

    // verify the required parameter 'pipeline' is set
    if (sourceOffset == null) {
      throw new ApiException(400, "Missing the required parameter 'sourceOffset' when calling updateCommittedOffsets");
    }

    // create path and map variables
    String path = "/v1/pipeline/{pipelineId}/committedOffsets".replaceAll("\\{format\\}","json")
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
        "application/json"
    };
    final String contentType = apiClient.selectHeaderContentType(contentTypes);

    String[] authNames = new String[] { "basic" };

    apiClient.invokeAPI(path, "POST", queryParams, postBody, postBinaryBody, headerParams, formParams, accept,
        contentType, authNames, null);
  }

}
