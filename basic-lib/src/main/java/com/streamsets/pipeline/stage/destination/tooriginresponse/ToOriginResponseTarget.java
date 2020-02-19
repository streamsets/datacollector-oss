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
package com.streamsets.pipeline.stage.destination.tooriginresponse;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.RecordTarget;
import com.streamsets.pipeline.stage.origin.restservice.RestServiceReceiver;

import java.util.Map;

public class ToOriginResponseTarget extends RecordTarget {

  private final int statusCode;
  private final Map<String, String> headers;

  ToOriginResponseTarget(int statusCode, Map<String, String> headers) {
    this.statusCode = statusCode;
    this.headers = headers;
  }

  @Override
  protected void write(Record record) {
    record.getHeader().setAttribute(
        RestServiceReceiver.STATUS_CODE_RECORD_HEADER_ATTR_NAME,
        String.valueOf(this.statusCode)
    );
    if (headers != null) {
      headers.forEach((k, v) ->
          record.getHeader().setAttribute(RestServiceReceiver.RESPONSE_HEADER_ATTR_NAME_PREFIX + k, v));
    }
    getContext().toSourceResponse(record);
  }

}
