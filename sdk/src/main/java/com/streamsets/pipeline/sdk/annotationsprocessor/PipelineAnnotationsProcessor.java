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
package com.streamsets.pipeline.sdk.annotationsprocessor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageErrorDef;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

@SupportedAnnotationTypes({"com.streamsets.pipeline.api.StageDef",
  "com.streamsets.pipeline.api.StageErrorDef"})
@SupportedSourceVersion(SourceVersion.RELEASE_7)
public class PipelineAnnotationsProcessor extends AbstractProcessor {

  private Map<String, String> stageNameToVersionMap = null;

  /*An instance of StageCollection collects all the stage definitions and configurations
  in maps and will later be serialized into json.*/
  private List<StageDefinition> stageDefinitions = null;

  /*The compiler may call this annotation processor multiple times in different rounds.
  We just need to process and generate only once*/
  private boolean generated = false;
  /*Captures if there is an error while processing stages*/
  private boolean stageDefValidationError = false;
  /*captures if there is an error while processing stage error definitions*/
  private boolean stageErrorDefValidationFailure = false;
  /*name of the enum that defines the error strings*/
  private String stageErrorDefEnumName = null;
  /*literal vs value maps for the stage error def enum*/
  private Map<String, String> stageErrorDefLiteralMap;
  private Map<StageDefinition, String> stageDefinitionToClassMap;

  private final ObjectMapper json;

  public PipelineAnnotationsProcessor() {
    super();
    stageDefinitions = new ArrayList<StageDefinition>();
    stageNameToVersionMap = new HashMap<String, String>();
    stageErrorDefLiteralMap = new HashMap<String, String>();
    json = new ObjectMapper();
    json.enable(SerializationFeature.INDENT_OUTPUT);
    stageDefinitionToClassMap = new HashMap<StageDefinition, String>();
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {

    //process classes annotated with StageDef annotation
    for(Element e : roundEnv.getElementsAnnotatedWith(StageDef.class)) {
      ElementKind eKind = e.getKind();
      //It will most likely be a class. being extra safe
      if(eKind.isClass()) {
        TypeElement typeElement = (TypeElement) e;
        StageDef stageDefAnnotation = e.getAnnotation(StageDef.class);

        validateStages(typeElement, stageDefAnnotation);
        if(stageDefValidationError) {
          continue;
        }
        stageDefinitions.add(createStageConfig(stageDefAnnotation, typeElement));
      }
    }

    //process enums with @StageErrorDef annotation
    if(roundEnv.getElementsAnnotatedWith(StageErrorDef.class).size() > 1) {
      processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
        "Expected one Stage Error Definition enum but found " +
        roundEnv.getElementsAnnotatedWith(StageErrorDef.class).size());
    } else {
      for (Element e : roundEnv.getElementsAnnotatedWith(StageErrorDef.class)) {
        ElementKind eKind = e.getKind();
        //It will most likely be a class. being extra safe
        if (eKind.isClass()) {
          TypeElement typeElement = (TypeElement) e;
          StageErrorDef stageErrorDef = e.getAnnotation(StageErrorDef.class);
          validateErrorDefinition(typeElement);
          if(stageErrorDefValidationFailure) {
            continue;
          }
          createStageErrorDef(typeElement);
        }
      }
    }

    if(!generated) {
      //generate a json file for the StageCollection object
      if(!stageDefValidationError) {
        generateConfigFile();
        generateStageBundles();
      }
      //generate a error bundle
      if(!stageErrorDefValidationFailure &&
        stageErrorDefEnumName != null) {
        generateErrorBundle();
      }
    }

    return (true);
  }

  private void createStageErrorDef(TypeElement typeElement) {
    stageErrorDefEnumName = typeElement.getQualifiedName().toString();
    List<? extends Element> enclosedElements = typeElement.getEnclosedElements();
    List<VariableElement> variableElements = ElementFilter.fieldsIn(enclosedElements);
    for (VariableElement variableElement : variableElements) {
      if(variableElement.getKind() == ElementKind.ENUM_CONSTANT) {
        stageErrorDefLiteralMap.put(variableElement.getSimpleName().toString(),
          (String) variableElement.getConstantValue());
      }
    }
  }

  /**
   * Validates the Stage Error Definition
   * Requires that it be enum which implements interface com.streamsets.pipeline.api.ErrorId
   *
   * @param typeElement
   */
  private void validateErrorDefinition(TypeElement typeElement) {
    //must be enum
    if(typeElement.getKind() != ElementKind.ENUM) {
      //Stage does not implement one of the Stage interface or extend the base stage class
      //This must be flagged as a compiler error.
      processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
        "Stage Error Definition " + typeElement.getQualifiedName()
          + " must be an enum.");
      stageDefValidationError = true;
      return;
    }
    //must implement com.streamsets.pipeline.api.ErrorId
    String type = getTypeFromElement(typeElement);
    if(type.isEmpty() || !type.equals("ERROR")) {
      //Stage does not implement one of the Stage interface or extend the base stage class
      //This must be flagged as a compiler error.
      processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
        "Stage Error Definition " + typeElement.getQualifiedName()
          + " does not implement interface 'com.streamsets.pipeline.api.ErrorId'.");
      stageDefValidationError = true;
      return;
    }
  }

  /**
   * Generates <stageName>-bundle.properties file for each stage definition.
   */
  private void generateStageBundles() {
    //get source location
    for(StageDefinition s : stageDefinitions) {
      try {
        String stageClass = stageDefinitionToClassMap.get(s);
        FileObject resource = processingEnv.getFiler().createResource(StandardLocation.CLASS_OUTPUT,
          stageClass.substring(0, stageClass.lastIndexOf('.')),
          s.getName() + "-bundle.properties", (Element[]) null);
        PrintWriter pw = new PrintWriter(resource.openWriter());
        pw.println("stage.label=" + s.getLabel());
        pw.println("stage.description=" + s.getDescription());
        for(ConfigDefinition c : s.getConfigDefinitions()) {
          pw.println("config." + c.getName() + ".label=" + c.getLabel());
          pw.println("config." + c.getName() + ".description=" + c.getDescription());
        }
        pw.flush();
        pw.close();
      } catch (IOException e) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getMessage());
      }
    }
  }

  /**
   * Generates the "PipelineStages.json" file with the configuration options
   */
  private void generateConfigFile() {
    try {
        FileObject resource = processingEnv.getFiler().createResource(StandardLocation.CLASS_OUTPUT, "",
          "PipelineStages.json", (Element[])null);
        json.writeValue(resource.openOutputStream(), stageDefinitions);
        generated = true;
    } catch (IOException e) {
      processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getMessage());
    }
  }

  /**
   * Generates <stageErrorDef>-bundle.properties file.
   */
  private void generateErrorBundle() {
    try {
      FileObject resource = processingEnv.getFiler().createResource(StandardLocation.CLASS_OUTPUT,
        stageErrorDefEnumName.substring(0, stageErrorDefEnumName.lastIndexOf('.')),
      stageErrorDefEnumName.substring(stageErrorDefEnumName.lastIndexOf('.') + 1,
        stageErrorDefEnumName.length())
        + "-bundle.properties", (Element[])null);
      PrintWriter pw = new PrintWriter(resource.openWriter());
      for(Map.Entry<String, String> e : stageErrorDefLiteralMap.entrySet()) {
        pw.println(e.getKey() + "=" + e.getValue());
      }
      pw.flush();
      pw.close();
    } catch (IOException e) {
      processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getMessage());
    }
  }

  /**
   * Creates and populates an instance of StageConfiguration from the Stage definition
   * @param stageDefAnnotation
   * @param typeElement
   * @return
   */
  private StageDefinition createStageConfig(StageDef stageDefAnnotation,
                                            TypeElement typeElement) {

    //Process all fields with ConfigDef annotation
    List< ConfigDefinition> configDefinitions = new ArrayList<ConfigDefinition>();
    List<? extends Element> enclosedElements = typeElement.getEnclosedElements();
    List<VariableElement> variableElements = ElementFilter.fieldsIn(enclosedElements);
    for (VariableElement variableElement : variableElements) {
      ConfigDef configDefAnnot = variableElement.getAnnotation(ConfigDef.class);
      if(configDefAnnot != null) {

        ConfigDefinition configDefinition = new ConfigDefinition(
          configDefAnnot.name(),
          configDefAnnot.type(),
          configDefAnnot.label(),
          configDefAnnot.description(),
          configDefAnnot.defaultValue(),
          configDefAnnot.required(),
          ""/*group name - need to remove it*/);
        configDefinitions.add(configDefinition);
      }
    }

    StageDefinition stageDefinition = new StageDefinition(
      typeElement.getQualifiedName().toString(),
      stageDefAnnotation.name(),
      stageDefAnnotation.version(),
      stageDefAnnotation.label(),
      stageDefAnnotation.description(),
      StageType.valueOf(getTypeFromElement(typeElement)),
      configDefinitions);

    stageDefinitionToClassMap.put(stageDefinition, typeElement.getQualifiedName().toString());
    return stageDefinition;
  }

  /**
   * Runs validations on
   *
   * @param typeElement
   * @param stageDefAnnotation
   */
  private void validateStages(TypeElement typeElement, StageDef stageDefAnnotation) {
    validateInterface(typeElement);
    validateAndCacheStageDef(stageDefAnnotation);
  }

  /**
   * Validates that a stage definition with the same name and version is not
   * already encountered. If encountered, the "error" flag is set to true.
   *
   * If not, the current stage name and version is cached.
   *
   * @param stageDefAnnotation
   */
  private void validateAndCacheStageDef(StageDef stageDefAnnotation) {

    if(stageNameToVersionMap.containsKey(stageDefAnnotation.name()) &&
      stageNameToVersionMap.get(stageDefAnnotation.name()).equals(stageDefAnnotation.version())) {
      //found more than one stage with same name and version
      processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
        "Multiple stage definitions found with the same name and version. Name : " + stageDefAnnotation.name() +
          ", Version : " + stageDefAnnotation.version());
      //Continue for now to find out if there are more issues.
      stageDefValidationError = true;
    } else {
      stageNameToVersionMap.put(stageDefAnnotation.name(), stageDefAnnotation.version());
    }
  }

  /**
   * Validates that the stage definition implements the expected interface or
   * extends from the expected abstract base class.
   *
   * @param typeElement
   */
  private void validateInterface(TypeElement typeElement) {
    if(getTypeFromElement(typeElement).isEmpty()) {
      //Stage does not implement one of the Stage interface or extend the base stage class
      //This must be flagged as a compiler error.
      processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
        "Stage " + typeElement.getSimpleName()
          + " neither extends one of BaseSource, BaseProcessor, BaseTarget classes nor implements one of Source, Processor, Target interface.");
      //Continue for now to find out if there are more issues.
      stageDefValidationError = true;
    }
  }

  /**
   * Infers the type of stage based on the interface implemented or the
   * abstract class extended.
   *
   * @param typeElement
   * @return
   */
  private String getTypeFromElement(TypeElement typeElement) {

    String result = null;

    //Check if the stage extends one of the abstract classes
    TypeMirror typeMirror = typeElement.getSuperclass();
    if(typeMirror != null) {
      if (typeMirror.toString().equals("com.streamsets.pipeline.api.base.BaseSource")) {
        result = "SOURCE";
      } else if (typeMirror.toString().equals("com.streamsets.pipeline.api.base.BaseProcessor")
        || typeMirror.toString().equals("com.streamsets.pipeline.api.base.RecordProcessor")
        || typeMirror.toString().equals("com.streamsets.pipeline.api.base.SingleLaneProcessor")
        || typeMirror.toString().equals("com.streamsets.pipeline.api.base.SingleLaneRecordProcessor")) {
        result = "PROCESSOR";
      } else if (typeMirror.toString().equals("com.streamsets.pipeline.api.base.BaseTarget")) {
        result = "TARGET";
      }
    }

    if(result != null) {
      return result;
    }

    //if not, check if it implements one of the interfaces
    List<? extends TypeMirror> interfaces = typeElement.getInterfaces();

    if(interfaces.size() != 0) {
      if (interfaces.get(0).toString().contains("Source")) {
        result = "SOURCE";
      } else if (interfaces.get(0).toString().contains("Processor")) {
        result = "PROCESSOR";
      } else if (interfaces.get(0).toString().contains("Target")) {
        result = "TARGET";
      } else if (interfaces.get(0).toString().contains("ErrorId")) {
        result = "ERROR";
      }
    }

    if(result != null) {
      return result;
    }

    return "";
  }

}