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
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.service.Service;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.api.service.dataformats.DataGenerator;
import com.streamsets.pipeline.api.service.dataformats.DataParser;
import com.streamsets.pipeline.api.service.dataformats.DataParserException;
import com.streamsets.pipeline.api.service.dataformats.SdcRecordGeneratorService;
import com.streamsets.pipeline.api.service.dataformats.WholeFileChecksumAlgorithm;
import com.streamsets.pipeline.api.service.dataformats.WholeFileExistsAction;
import com.streamsets.pipeline.api.service.dataformats.log.LogParserService;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is general wrapper on top of Service implementation. This actual instance will be returned when Stage calls
 * getContext().getServiceClass(Class) and hence it needs to implement all supported services. We return this wrapping
 * object rather then Service instance itself because we need to wrap each method execution to change active class
 * loader and execute the code in privileged mode to apply the proper permissions.
 */
public class ServiceRuntime implements DataFormatGeneratorService, DataFormatParserService, LogParserService, SdcRecordGeneratorService, SshTunnelService {

  // Static list with all supported services
  private static Set<Class> SUPPORTED_SERVICES = ImmutableSet.of(
    DataFormatGeneratorService.class,
    DataFormatParserService.class,
    SdcRecordGeneratorService.class,
    LogParserService.class,
    SshTunnelService.class
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

  @Override // From DataFormatGeneratorService, SdcRecordGeneratorService
  public DataGenerator getGenerator(OutputStream os) throws IOException {
    ClassLoader cl = serviceBean.getDefinition().getStageClassLoader();

    return LambdaUtil.privilegedWithClassLoader(
      cl,
      IOException.class,
      () -> {
        if(serviceBean.getService() instanceof DataFormatGeneratorService) {
          return new DataGeneratorServiceWrapper(cl, ((DataFormatGeneratorService)serviceBean.getService()).getGenerator(os));
        } else if(serviceBean.getService() instanceof  SdcRecordGeneratorService) {
          return new DataGeneratorServiceWrapper(cl, ((SdcRecordGeneratorService)serviceBean.getService()).getGenerator(os));
        } else {
          throw new IllegalStateException("Called on wrong service: " + serviceBean.getService().getClass().getCanonicalName());
        }
      }
    );
  }

  @Override
  public boolean isPlainTextCompatible() {
     return LambdaUtil.privilegedWithClassLoader(
        serviceBean.getDefinition().getStageClassLoader(),
        () -> ((DataFormatGeneratorService)serviceBean.getService()).isPlainTextCompatible()
    );
  }

  @Override // From DataFormatParserService, DataFormatGeneratorService
  public String getCharset() {
    return LambdaUtil.privilegedWithClassLoader(
        serviceBean.getDefinition().getStageClassLoader(),
        () -> {
          if(serviceBean.getService() instanceof DataFormatGeneratorService) {
            return ((DataFormatGeneratorService)serviceBean.getService()).getCharset();
          } else if(serviceBean.getService() instanceof  DataFormatParserService) {
            return ((DataFormatParserService) serviceBean.getService()).getCharset();
          } else {
            throw new IllegalStateException("Called on wrong service: " + serviceBean.getService().getClass().getCanonicalName());
          }
        }
    );
  }

  @Override // From DataFormatParserService
  @Deprecated
  public void setStringBuilderPoolSize(int poolSize) {
    LambdaUtil.privilegedWithClassLoader(
      serviceBean.getDefinition().getStageClassLoader(),
      () -> { ((DataFormatParserService)serviceBean.getService()).setStringBuilderPoolSize(poolSize); return null; }
    );
  }

  @Override // From DataFormatParserService
  @Deprecated
  public int getStringBuilderPoolSize() {
    return LambdaUtil.privilegedWithClassLoader(
        serviceBean.getDefinition().getStageClassLoader(),
        () -> ((DataFormatParserService)serviceBean.getService()).getStringBuilderPoolSize()
    );
  }

  @Override // From DataFormatParserService, DataFormatGeneratorService
  public boolean isWholeFileFormat() {
    return LambdaUtil.privilegedWithClassLoader(
        serviceBean.getDefinition().getStageClassLoader(),
        () -> {
          if(serviceBean.getService() instanceof DataFormatGeneratorService) {
            return ((DataFormatGeneratorService)serviceBean.getService()).isWholeFileFormat();
          } else if(serviceBean.getService() instanceof  DataFormatParserService) {
            return ((DataFormatParserService) serviceBean.getService()).isWholeFileFormat();
          } else {
            throw new IllegalStateException("Called on wrong service: " + serviceBean.getService().getClass().getCanonicalName());
          }
        }
    );
  }

  @Override // From DataFormatGeneratorService
  public String wholeFileFilename(Record record) throws StageException {
   return LambdaUtil.privilegedWithClassLoader(
        serviceBean.getDefinition().getStageClassLoader(),
         StageException.class,
        () -> ((DataFormatGeneratorService)serviceBean.getService()).wholeFileFilename(record)
    );
  }

  @Override // From DataFormatGeneratorService
  public WholeFileExistsAction wholeFileExistsAction() {
    return LambdaUtil.privilegedWithClassLoader(
      serviceBean.getDefinition().getStageClassLoader(),
      () -> ((DataFormatGeneratorService)serviceBean.getService()).wholeFileExistsAction()
    );
  }

  @Override // From DataFormatGeneratorService
  public boolean wholeFileIncludeChecksumInTheEvents() {
    return LambdaUtil.privilegedWithClassLoader(
      serviceBean.getDefinition().getStageClassLoader(),
      () -> ((DataFormatGeneratorService)serviceBean.getService()).wholeFileIncludeChecksumInTheEvents()
    );
  }

  @Override // From DataFormatGeneratorService
  public WholeFileChecksumAlgorithm wholeFileChecksumAlgorithm() {
    return LambdaUtil.privilegedWithClassLoader(
      serviceBean.getDefinition().getStageClassLoader(),
      () -> ((DataFormatGeneratorService)serviceBean.getService()).wholeFileChecksumAlgorithm()
    );
  }

  @Override // From DataFormatParserService
  public long suggestedWholeFileBufferSize() {
    return LambdaUtil.privilegedWithClassLoader(
      serviceBean.getDefinition().getStageClassLoader(),
      () -> ((DataFormatParserService)serviceBean.getService()).suggestedWholeFileBufferSize()
    );
  }

  @Override // From DataFormatParserService
  public Double wholeFileRateLimit() throws StageException {
    return LambdaUtil.privilegedWithClassLoader(
      serviceBean.getDefinition().getStageClassLoader(),
      () -> ((DataFormatParserService)serviceBean.getService()).wholeFileRateLimit()
    );
  }

  @Override // From DataFormatParserService
  public boolean isWholeFileChecksumRequired() {
    return LambdaUtil.privilegedWithClassLoader(
      serviceBean.getDefinition().getStageClassLoader(),
      () -> ((DataFormatParserService)serviceBean.getService()).isWholeFileChecksumRequired()
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

  @Override
  public DataParser getLogParser(String id, Reader reader, long offset) throws DataParserException {
    ClassLoader cl = serviceBean.getDefinition().getStageClassLoader();

    return LambdaUtil.privilegedWithClassLoader(
        cl,
        DataParserException.class,
        () -> new DataParserServiceWrapper(
            cl,
            ((LogParserService) serviceBean.getService()).getLogParser(id, reader, offset)
        )
    );
  }

  @Override // From SshTunnelService
  public boolean isEnabled() {
      return LambdaUtil.privilegedWithClassLoader(
          serviceBean.getDefinition().getStageClassLoader(),
          () -> ((SshTunnelService)serviceBean.getService()).isEnabled()
      );
  }

  @Override // From SshTunnelService
  public Map<HostPort, HostPort>  start(List<HostPort> targetHostsPorts) {
    return LambdaUtil.privilegedWithClassLoader(
        serviceBean.getDefinition().getStageClassLoader(),
        () -> ((SshTunnelService)serviceBean.getService()).start(targetHostsPorts)
    );
  }

  @Override // From SshTunnelService
  public void healthCheck() {
    LambdaUtil.privilegedWithClassLoader(
        serviceBean.getDefinition().getStageClassLoader(),
        () -> {
          ((SshTunnelService) serviceBean.getService()).healthCheck();
          return null;
        }
    );
  }

  @Override // From SshTunnelService
  public void stop() {
    LambdaUtil.privilegedWithClassLoader(
        serviceBean.getDefinition().getStageClassLoader(),
        () -> {
          ((SshTunnelService) serviceBean.getService()).stop();
          return null;
        }
    );
  }
}
