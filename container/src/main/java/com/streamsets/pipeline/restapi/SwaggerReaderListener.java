/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import io.swagger.annotations.Api;
import io.swagger.jaxrs.Reader;
import io.swagger.jaxrs.config.ReaderListener;
import io.swagger.models.Model;
import io.swagger.models.Operation;
import io.swagger.models.Path;
import io.swagger.models.Swagger;
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

  }
}
