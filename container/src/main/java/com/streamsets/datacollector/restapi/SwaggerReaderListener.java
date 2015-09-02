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
package com.streamsets.datacollector.restapi;

import io.swagger.annotations.Api;
import io.swagger.jaxrs.Reader;
import io.swagger.jaxrs.config.ReaderListener;
import io.swagger.models.Model;
import io.swagger.models.Operation;
import io.swagger.models.Path;
import io.swagger.models.Swagger;
import io.swagger.models.auth.BasicAuthDefinition;
import io.swagger.models.parameters.BodyParameter;
import io.swagger.models.parameters.Parameter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Swagger adds Resource constructor parameters to all the rest apis as body parameter.
 * Added Swagger Reader Listener to remove additional global parameters added by swagger.
 */
@Api
public class SwaggerReaderListener implements ReaderListener {
  public void beforeScan(Reader reader, Swagger swagger) {
  }

  public void afterScan(Reader reader, Swagger swagger) {
    Map<String, Path> paths = swagger.getPaths();
    Set<String> referencesToRemove = new HashSet<>();

    for(String pathName: paths.keySet()) {
      Path path = paths.get(pathName);

      List<Operation> operations = path.getOperations();

      for(Operation operation: operations) {
        List<Parameter> newList = new ArrayList<>();
        List<Parameter> parameterList = operation.getParameters();

        for(Parameter parameter: parameterList) {
          String in = parameter.getIn();
          String name = parameter.getName();
          if(!"body".equals(in) || !"body".equals(name)) {
            newList.add(parameter);
          } else if(parameter instanceof BodyParameter){
            Model schema = ((BodyParameter) parameter).getSchema();
            referencesToRemove.add(schema.getReference());
          }
        }

        operation.setParameters(newList);
      }

    }

    Map<String, Model> definitons = swagger.getDefinitions();
    for(String reference: referencesToRemove) {
      if(definitons.get(reference) != null) {
        definitons.remove(reference);
      }
    }

    swagger.securityDefinition("basic", new BasicAuthDefinition());

  }
}
