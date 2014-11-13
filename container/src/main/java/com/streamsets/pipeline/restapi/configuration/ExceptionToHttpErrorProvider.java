/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.restapi.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Jersey provider that converts KMS exceptions into detailed HTTP errors.
 */
@Provider
public class ExceptionToHttpErrorProvider implements ExceptionMapper<Exception> {
  private static Logger LOG = LoggerFactory.getLogger(ExceptionToHttpErrorProvider.class);

  private static final String ERROR_JSON = "RemoteException";
  private static final String ERROR_EXCEPTION_JSON = "exception";
  private static final String ERROR_CLASSNAME_JSON = "javaClassName";
  private static final String ERROR_MESSAGE_JSON = "message";
  private static final String ERROR_LOCALIZED_MESSAGE_JSON = "localizedMessage";
  private static final String ENTER = System.getProperty("line.separator");

  protected Response createResponse(Response.Status status, Throwable ex) {
    Map<String, Object> json = new LinkedHashMap<String, Object>();
    json.put(ERROR_MESSAGE_JSON, getOneLineMessage(ex, false));
    json.put(ERROR_LOCALIZED_MESSAGE_JSON, getOneLineMessage(ex, true));
    json.put(ERROR_EXCEPTION_JSON, ex.getClass().getSimpleName());
    json.put(ERROR_CLASSNAME_JSON, ex.getClass().getName());
    Map<String, Object> response = new LinkedHashMap<String, Object>();
    response.put(ERROR_JSON, json);
    return Response.status(status).type(MediaType.APPLICATION_JSON).
        entity(response).build();
  }

  protected String getOneLineMessage(Throwable exception, boolean localized) {
    String message = (localized) ? exception.getLocalizedMessage() : exception.getMessage();
    if (message != null) {
      int i = message.indexOf(ENTER);
      if (i > -1) {
        message = message.substring(0, i);
      }
    }
    return message;
  }

  @Override
  public Response toResponse(Exception ex) {
    LOG.error("REST API call error: {}", ex.getMessage(), ex);
    return createResponse(Status.INTERNAL_SERVER_ERROR, ex);
  }

}
