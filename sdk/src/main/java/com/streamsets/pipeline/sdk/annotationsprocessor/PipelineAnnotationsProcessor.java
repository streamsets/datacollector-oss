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
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.config.*;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.lang.model.type.MirroredTypeException;
import javax.lang.model.type.TypeKind;
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


  private final ObjectMapper json;

  public PipelineAnnotationsProcessor() {
    super();
    stageDefinitions = new ArrayList<StageDefinition>();
    stageNameToVersionMap = new HashMap<String, String>();
    stageErrorDefLiteralMap = new HashMap<String, String>();
    json = new ObjectMapper();
    json.enable(SerializationFeature.INDENT_OUTPUT);
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

        stageDefinitions.add(createStageConfig(stageDefAnnotation, typeElement));
      }
    }

    //process enums with @StageErrorDef annotation
    if(roundEnv.getElementsAnnotatedWith(StageErrorDef.class).size() > 1) {
      printError("stagedeferror.validation.multiple.enums",
        "Expected one Stage Error Definition enum but found %s",
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

    //generate stuff only in the last round
    if(roundEnv.processingOver()) {
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

    return true;
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
      printError("stagedeferror.validation.not.an.enum",
        "Stage Error Definition %s must be an enum", typeElement.getQualifiedName());
      stageDefValidationError = true;
      return;
    }
    //must implement com.streamsets.pipeline.api.ErrorId
    String type = getTypeFromElement(typeElement);
    if(type.isEmpty() || !type.equals("ERROR")) {
      //Stage does not implement one of the Stage interface or extend the base stage class
      //This must be flagged as a compiler error.
      printError("stagedeferror.validation.enum.does.not.implement.interface",
        "Stage Error Definition %s does not implement interface 'com.streamsets.pipeline.api.ErrorId'.",
        typeElement.getQualifiedName());
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
        FileObject resource = processingEnv.getFiler().createResource(StandardLocation.CLASS_OUTPUT,
          s.getClassName().substring(0, s.getClassName().lastIndexOf('.')),
          s.getClassName().substring(s.getClassName().lastIndexOf(".") + 1) + "-bundle.properties", (Element[]) null);
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

        //validate field with ConfigDef annotation
        if(!validateConfigDefAnnotation(typeElement, variableElement, configDefAnnot)) {
          continue;
        }

        ModelDefinition model = null;

        if(configDefAnnot.type().equals(ConfigDef.Type.MODEL)) {
          FieldSelector fieldSelector = variableElement.getAnnotation(FieldSelector.class);
          if(fieldSelector != null) {
            model = new ModelDefinition(ModelType.FIELD_SELECTOR, null, null, null);
          }
          FieldModifier fieldModifier = variableElement.getAnnotation(FieldModifier.class);
          if (fieldModifier != null) {
            model = new ModelDefinition(ModelType.FIELD_MODIFIER, getFieldModifierType(fieldModifier.type()),
              null, null);
          }
        }

        ConfigDefinition configDefinition = new ConfigDefinition(
          configDefAnnot.name(),
          configDefAnnot.type(),
          configDefAnnot.label(),
          configDefAnnot.description(),
          configDefAnnot.defaultValue(),
          configDefAnnot.required(),
          ""/*group name - need to remove it*/,
          variableElement.getSimpleName().toString(),
          model);
        configDefinitions.add(configDefinition);
      }
    }

    StageDefinition stageDefinition = null;
    if(validateStageDef(typeElement, stageDefAnnotation)) {
      stageDefinition = new StageDefinition(
        typeElement.getQualifiedName().toString(),
        stageDefAnnotation.name(),
        stageDefAnnotation.version(),
        stageDefAnnotation.label(),
        stageDefAnnotation.description(),
        StageType.valueOf(getTypeFromElement(typeElement)),
        configDefinitions);
    } else {
      stageDefValidationError = true;
    }

    return stageDefinition;
  }

  /**
   * Validates that the Stage definition is valid.
   * The following validations are done:
   * <ul>
   *   <li>The Stage implementation extends/implements the expected base classes/interfaces</li>
   *   <li>Stage implementation or its base class must declare a public constructor</li>
   * </ul>
   * @param typeElement
   * @param stageDefAnnotation
   * @return
   */
  private boolean validateStageDef(TypeElement typeElement, StageDef stageDefAnnotation) {
    boolean validInterface = validateInterface(typeElement);
    boolean validStage = validateAndCacheStageDef(stageDefAnnotation);
    boolean validConstructor = validateStageForConstructor(typeElement);
    return validInterface && validStage && validConstructor;
  }

  /**
   * A stage implementation should have a default constructor or no constructor.
   * In case, the stage implementation has no constructor, the same should
   * also apply to all its parent classes.
   *
   * @param typeElement
   * @return
   */
  private boolean validateStageForConstructor(TypeElement typeElement) {
    //indicates whether validation failed
    boolean validationError = false;
    //indicates whether default constructor was found
    boolean foundDefConstr = false;

    TypeElement te = typeElement;

    while(foundDefConstr == false &&
      validationError == false && te != null) {
      List<? extends Element> enclosedElements = te.getEnclosedElements();
      List<ExecutableElement> executableElements = ElementFilter.constructorsIn(enclosedElements);
      for(ExecutableElement e : executableElements) {
        //found one or more constructors, check for default constr
        if(e.getSimpleName().toString().equals("<init>")
          && e.getModifiers().contains(Modifier.PUBLIC)) {
          if(e.getParameters().size() == 0) {
            //found default constructor
            foundDefConstr = true;
            break;
          }
        }
      }
      //There are constructors but did not find default constructor
      if(executableElements.size() > 0 && !foundDefConstr) {
        validationError = true;
      }
      //get super class and run the same checks
      TypeMirror superClass = te.getSuperclass();
      if(superClass != null) {

        Element e = processingEnv.getTypeUtils().asElement(superClass);
        if(e != null && e.getKind().equals(ElementKind.CLASS)) {
          te = (TypeElement)e;
        } else {
          te = null;
        }
      } else {
        te = null;
      }
    }
    if(validationError) {
      printError("stage.validation.no.default.constructor",
        "The Stage %s has constructor with arguments but no default constructor.",
        typeElement.getSimpleName());
    }

    return !validationError;
  }

  private FieldModifierType getFieldModifierType(FieldModifier.Type type) {
    if(type.equals(FieldModifier.Type.PROVIDED)) {
      return FieldModifierType.PROVIDED;
    } else if (type.equals(FieldModifier.Type.SUGGESTED)) {
      return FieldModifierType.SUGGESTED;
    }
    //default
    return FieldModifierType.SUGGESTED;
  }

  /**
   * Validates the field on which the ConfigDef annotation is specified.
   * The following validations are done:
   * <ul>
   *   <li>The field must be declared as public</li>
   *   <li>The type of the field must match the type specified in the ConfigDef annotation</li>
   *   <li>If the type is "MODEL" then exactly one of "FieldSelector" or "FieldModifier" annotation must be present</li>
   * </ul>
   *
   * @param variableElement
   * @param configDefAnnot
   */
  private boolean validateConfigDefAnnotation(Element typeElement, VariableElement variableElement, ConfigDef configDefAnnot) {
    boolean valid = true;
    //field must be declared public
    if(!variableElement.getModifiers().contains(Modifier.PUBLIC)) {
      printError("field.validation.not.public",
        "The field %s has \"ConfigDef\" annotation but is not declared public. Configuration fields must be declared public.",
        typeElement.getSimpleName().toString() + variableElement.getSimpleName().toString());
      valid = false;
    }

    if(variableElement.getModifiers().contains(Modifier.FINAL)) {
      printError("field.validation.final.field",
        "The field %s has \"ConfigDef\" annotation and is declared final. Configuration fields must not be declared final.",
        typeElement.getSimpleName().toString() + variableElement.getSimpleName().toString()
        );
      valid = false;
    }

    if(variableElement.getModifiers().contains(Modifier.STATIC)) {
      printError("field.validation.static.field",
        "The field %s has \"ConfigDef\" annotation and is declared static. Configuration fields must not be declared final.",
        typeElement.getSimpleName().toString() + variableElement.getSimpleName().toString());
      valid = false;
    }

    //type match
    TypeMirror fieldType = variableElement.asType();
    if(configDefAnnot.type().equals(ConfigDef.Type.MODEL)) {
      FieldModifier fieldModifier = variableElement.getAnnotation(FieldModifier.class);
      FieldSelector fieldSelector = variableElement.getAnnotation(FieldSelector.class);

      if(fieldModifier == null && fieldSelector == null ) {
        printError("field.validation.no.model.annotation",
          "The type of field %s is declared as \"MODEL\". Exactly one of 'FieldSelector' or 'FieldModifier' annotation is expected.",
          typeElement.getSimpleName().toString() + variableElement.getSimpleName().toString());
        valid = false;
      } else if (fieldModifier !=null && fieldSelector != null) {
        //both cannot be present
        printError("field.validation.multiple.model.annotations",
          "The field %s is annotated with both 'FieldSelector' and 'FieldModifier' annotations. Only one of those annotation is expected.",
          typeElement.getSimpleName().toString() + variableElement.getSimpleName().toString());
        valid = false;
      }

      if(fieldModifier != null) {
        if(!fieldType.toString().equals("java.util.Map<java.lang.String,java.lang.String>")) {
          printError("field.validation.type.is.not.map",
            "The type of the field %s is expected to be Map<String, String>.",
            typeElement.getSimpleName().toString() + variableElement.getSimpleName().toString());
          valid = false;
        }
      }

      if(fieldSelector != null) {
        if(!fieldType.toString().equals("java.util.List<java.lang.String>")) {
          printError("field.validation.type.is.not.list",
            "The type of the field %s is expected to be List<String>.",
            typeElement.getSimpleName().toString() + variableElement.getSimpleName().toString());
          valid = false;
        }
      }

    } else if (configDefAnnot.type().equals(ConfigDef.Type.BOOLEAN)) {
      if(!fieldType.getKind().equals(TypeKind.BOOLEAN)) {
        printError("field.validation.type.is.not.boolean",
          "The type of the field %s is expected to be boolean.",
          typeElement.getSimpleName().toString() + variableElement.getSimpleName().toString());
        valid = false;
      }
    } else if (configDefAnnot.type().equals(ConfigDef.Type.INTEGER)) {
      if(!(fieldType.getKind().equals(TypeKind.INT) || fieldType.getKind().equals(TypeKind.LONG))) {
        printError("field.validation.type.is.not.int.or.long",
          "The type of the field %s is expected to be either an int or a long.",
          typeElement.getSimpleName().toString() + variableElement.getSimpleName().toString());
        valid = false;
      }
    } else if (configDefAnnot.type().equals(ConfigDef.Type.STRING)) {
      if(!fieldType.toString().equals("java.lang.String")) {
        printError("field.validation.type.is.not.string",
          "The type of the field %s is expected to be String.",
          typeElement.getSimpleName().toString() + variableElement.getSimpleName().toString());
        valid = false;
      }
    }
    return valid;
  }

  /**
   * Validates that a stage definition with the same name and version is not
   * already encountered. If encountered, the "error" flag is set to true.
   *
   * If not, the current stage name and version is cached.
   *
   * @param stageDefAnnotation
   */
  private boolean validateAndCacheStageDef(StageDef stageDefAnnotation) {

    if(stageNameToVersionMap.containsKey(stageDefAnnotation.name()) &&
      stageNameToVersionMap.get(stageDefAnnotation.name()).equals(stageDefAnnotation.version())) {
      //found more than one stage with same name and version
      printError("stagedef.validation.duplicate.stages",
        "Multiple stage definitions found with the same name and version. Name %s, Version %s",
          stageDefAnnotation.name(), stageDefAnnotation.version());
      //Continue for now to find out if there are more issues.
      return false;
    }
    stageNameToVersionMap.put(stageDefAnnotation.name(), stageDefAnnotation.version());
    return true;
  }

  /**
   * Validates that the stage definition implements the expected interface or
   * extends from the expected abstract base class.
   *
   * @param typeElement
   */
  private boolean validateInterface(TypeElement typeElement) {
    if(getTypeFromElement(typeElement).isEmpty()) {
      //Stage does not implement one of the Stage interface or extend the base stage class
      //This must be flagged as a compiler error.
      printError("stagedef.validation.does.not.implement.interface",
        "Stage %s neither extends one of BaseSource, BaseProcessor, BaseTarget classes nor implements one of Source, Processor, Target interface.",
        typeElement.getQualifiedName().toString());
      //Continue for now to find out if there are more issues.
      return false;
    }
    return true;
  }

  /**
   * Infers the type of stage based on the interface implemented or the
   * abstract class extended.
   *
   * @param typeElement
   * @return
   */
  private String getTypeFromElement(TypeElement typeElement) {

    //Check if the stage extends one of the abstract classes
    for(TypeMirror typeMirror : allSuperTypes(typeElement)) {
      if (typeMirror.toString().equals("com.streamsets.pipeline.api.base.BaseSource")
        || typeMirror.toString().equals("com.streamsets.pipeline.api.Source")) {
        return "SOURCE";
      } else if (typeMirror.toString().equals("com.streamsets.pipeline.api.base.BaseProcessor")
        || typeMirror.toString().equals("com.streamsets.pipeline.api.base.RecordProcessor")
        || typeMirror.toString().equals("com.streamsets.pipeline.api.base.SingleLaneProcessor")
        || typeMirror.toString().equals("com.streamsets.pipeline.api.base.SingleLaneRecordProcessor")
        || typeMirror.toString().equals("com.streamsets.pipeline.api.Processor")) {
        return "PROCESSOR";
      } else if (typeMirror.toString().equals("com.streamsets.pipeline.api.base.BaseTarget")
        || typeMirror.toString().equals("com.streamsets.pipeline.api.Target")) {
        return "TARGET";
      } else if (typeMirror.toString().equals("com.streamsets.pipeline.api.ErrorId")) {
        return "ERROR";
      }
    }
    return "";
  }

  private static TypeMirror getTypeProvider(FieldModifier fieldModifier) {
    try
    {
      fieldModifier.valuesProvider(); // this should throw
    }
    catch( MirroredTypeException mte )
    {
      return mte.getTypeMirror();
    }
    return null;
  }

  private List<TypeMirror> allSuperTypes(TypeElement element){
    List<TypeMirror> allSuperTypes=new ArrayList<TypeMirror>();
    Queue<TypeMirror> runningList=new LinkedList<TypeMirror>();
    runningList.add(element.asType());
    while (runningList.size() != 0) {
      TypeMirror currentType=runningList.poll();
      if (currentType.getKind() != TypeKind.NONE) {
        allSuperTypes.add(currentType);
        Element currentElement= processingEnv.getTypeUtils().asElement(currentType);
        if (currentElement.getKind() == ElementKind.CLASS ||
          currentElement.getKind() == ElementKind.INTERFACE||
          currentElement.getKind() == ElementKind.ENUM) {
          TypeElement currentTypeElement=(TypeElement)currentElement;
          runningList.offer(currentTypeElement.getSuperclass());
          for(TypeMirror t : currentTypeElement.getInterfaces()) {
            runningList.offer(t);
          }
        }
      }
    }
    allSuperTypes.remove(element.asType());
    return allSuperTypes;
  }
  
  private void printError(String bundleKey,
                          String template, Object... args) {

    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
      String.format(template, args));
  }


}