/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.restapi.rbean.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.rbean.json.RJson;
import com.streamsets.pipeline.api.impl.Utils;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class RestRequest<T> {
  public static final String ENVELOPE_VERSION = "1";

  private String envelopeVersion = ENVELOPE_VERSION;
  private T data;

  public RestRequest() {
  }

  public String getEnvelopeVersion() {
    return envelopeVersion;
  }

  public RestRequest<T> setEnvelopeVersion(String envelopeVersion) {
    this.envelopeVersion = envelopeVersion;
    return this;
  }

  public T getData() {
    return data;
  }

  public RestRequest<T> setData(T data) {
    this.data = data;
    return this;
  }

  @Override
  public String toString() {
    return "RestRequest{" + "version='" + envelopeVersion + '\'' + ", data=" + data + '}';
  }

  /**
   * Deserializes a RestRequest (the exact type of the payload must be specified with a Jackson TypeReference) from
   * a String.
   * <p/>
   * Typically this is used in a RestResource that is uploading a request and file(s) in a multipart, where the
   * RestRequest&lt;T&gt; is a part of the multipart (which by convention it should be named "restRequest").
   * <p/>
   * Example:
   * <pre>
   *   @Path("/pageId=RPersonManagePage")
   *   @POST
   *   @Consumes(MediaType.MULTIPART_FORM_DATA)
   *   @Produces(MediaType.APPLICATION_JSON)
   *   public OkRestResponse&lt;RPerson&gt; uploadBeanAndFile(@FormDataParam("restRequest") String restRequest, @FormDataParam("upload") InputStream upload) throws IOException  {
   *     RestRequest&tlg;RPerson&gt; request = getRequest(requestPayload, new TypeReference&tlg;&gt;(){});
   *     return new OkRestResponse&tlg;RPerson&gt;().getData(getBackend().uploadPersonAndFiles(request.getData(), ImmutableMap.of("file", upload)));
   *   }
   * </pre>
   */
  public static <T> RestRequest<T> getRequest(@NotNull String restRequest, TypeReference<RestRequest<T>> typeReference)
      throws IOException {
    RestRequest<T> request = ObjectMapperFactory.get().readValue(restRequest, typeReference);
    Utils.checkArgument(request != null, "There is no RestRequest payload");
    Utils.checkArgument(request.getData() != null, "RestRequest does not contain any data");
    return request;
  }

}
