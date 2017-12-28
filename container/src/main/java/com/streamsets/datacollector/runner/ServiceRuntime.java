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
package com.streamsets.datacollector.runner;

import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.creation.PipelineBean;
import com.streamsets.datacollector.creation.ServiceBean;
import com.streamsets.datacollector.runner.service.DataGeneratorServiceWrapper;
import com.streamsets.datacollector.runner.service.DataParserServiceWrapper;
import com.streamsets.datacollector.util.LambdaUtil;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.service.Service;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.api.service.dataformats.DataGenerator;
import com.streamsets.pipeline.api.service.dataformats.DataParser;
import com.streamsets.pipeline.api.service.dataformats.DataParserException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is general wrapper on top of Service implementation. This actual instance will be returned when Stage calls
 * getContext().getService(Class) and hence it needs to implement all supported services. We return this wrapping
 * object rather then Service instance itself because we need to wrap each method execution to change active class
 * loader and execute the code in privileged mode to apply the proper permissions.
 */
public class ServiceRuntime implements DataFormatGeneratorService, DataFormatParserService {

  // Static list with all supported services
  private static Set<Class> SUPPORTED_SERVICES = ImmutableSet.of(
    DataFormatGeneratorService.class,
    DataFormatParserService.class
  );

  /**
   * Return true if and only given service interface is supported by this instance.
   *
   * @param serviceInterface Service interface
   * @return True if given interface is supported by this runtime
   */
  public static boolean supports(Class serviceInterface) {
    return SUPPORTED_SERVICES.contains(serviceInterface);
  }

  private final PipelineBean pipelineBean;
  private final ServiceBean serviceBean;
  private Service.Context context;

  public ServiceRuntime(
      PipelineBean pipelineBean,
      ServiceBean serviceBean
  ) {
    this.pipelineBean = pipelineBean;
    this.serviceBean = serviceBean;
  }

  public ServiceBean getServiceBean() {
    return serviceBean;
  }

  public void setContext(Service.Context context) {
    this.context = context;
  }

  public List<Issue> init() {
    return LambdaUtil.privilegedWithClassLoader(
        serviceBean.getDefinition().getStageClassLoader(),
        () -> (List)serviceBean.getService().init(context)
    );
  }

  public void destroy() {
    LambdaUtil.privilegedWithClassLoader(
      serviceBean.getDefinition().getStageClassLoader(),
      () -> { serviceBean.getService().destroy(); return null; }
    );
  }

  @Override // From DataFormatGeneratorService
  public DataGenerator getGenerator(OutputStream os) throws IOException {
    ClassLoader cl = serviceBean.getDefinition().getStageClassLoader();

    return LambdaUtil.privilegedWithClassLoader(
      cl,
      IOException.class,
      () -> new DataGeneratorServiceWrapper(cl, ((DataFormatGeneratorService)serviceBean.getService()).getGenerator(os))
    );
  }

  @Override // From DataFormatParserService
  public DataParser getParser(String id, InputStream is, String offset) throws DataParserException {
    ClassLoader cl = serviceBean.getDefinition().getStageClassLoader();

     return LambdaUtil.privilegedWithClassLoader(
      cl,
      DataParserException.class,
      () -> new DataParserServiceWrapper(cl, ((DataFormatParserService)serviceBean.getService()).getParser(id, is, offset))
    );
  }

  @Override // From DataFormatParserService
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    ClassLoader cl = serviceBean.getDefinition().getStageClassLoader();

    return LambdaUtil.privilegedWithClassLoader(
      cl,
      DataParserException.class,
      () -> new DataParserServiceWrapper(cl, ((DataFormatParserService)serviceBean.getService()).getParser(id, reader, offset))
    );
  }

  @Override // From DataFormatParserService
  public DataParser getParser(String id, Map<String, Object> metadata, FileRef fileRef) throws DataParserException {
    ClassLoader cl = serviceBean.getDefinition().getStageClassLoader();

    return LambdaUtil.privilegedWithClassLoader(
      cl,
      DataParserException.class,
      () -> new DataParserServiceWrapper(cl, ((DataFormatParserService)serviceBean.getService()).getParser(id, metadata, fileRef))
    );
  }
}
