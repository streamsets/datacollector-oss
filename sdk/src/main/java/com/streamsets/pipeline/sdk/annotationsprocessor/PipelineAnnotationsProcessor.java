/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.FieldValueChooser;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfig;
import com.streamsets.pipeline.api.LanePredicateMapping;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.ConfigGroupDefinition;
import com.streamsets.pipeline.config.ModelDefinition;
import com.streamsets.pipeline.config.ModelType;
import com.streamsets.pipeline.config.RawSourceDefinition;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.el.ElConstantDefinition;
import com.streamsets.pipeline.el.ElFunctionDefinition;
import com.streamsets.pipeline.restapi.bean.BeanHelper;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.MirroredTypeException;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;


@SupportedAnnotationTypes({"com.streamsets.pipeline.api.StageDef",
  "com.streamsets.pipeline.api.GenerateResourceBundle"})
@SupportedSourceVersion(SourceVersion.RELEASE_7)
public class PipelineAnnotationsProcessor extends AbstractProcessor {

  /**************** constants ********************/

  private static final String SOURCE = "SOURCE";
  private static final String PROCESSOR = "PROCESSOR";
  private static final String TARGET = "TARGET";
  private static final String ERROR = "ERROR";
  private static final String LABEL = "LABEL";
  private static final String SEPARATOR = ".";
  private static final String DOT = ".";
  private static final String DEFAULT_CONSTRUCTOR = "<init>";
  private static final String PIPELINE_STAGES_JSON = "PipelineStages.json";
  private static final String DC_RESOURCE_BUNDLES_JSON = "datacollector-resource-bundles.json";
  private static final String MAP_TYPE_WITH_KEY = "java.util.Map<java.lang.String,";
  private static final String VARIABLE_OUTPUT_STREAMS_CLASS = StageDef.VariableOutputStreams.class.getName();
  private static final String DEFAULT_OUTPUT_STREAMS_CLASS = StageDef.DefaultOutputStreams.class.getName();

  /**************** private variables ************/

  /*Map that keeps track of all the encountered stage implementations and the versions*/
  private Map<String, String> stageNameToVersionMap = null;
  /*An instance of StageCollection collects all the stage definitions and configurations
  in maps and will later be serialized into json.*/
  private List<StageDefinition> stageDefinitions = null;
  /*Indicates if there is an error while processing stages*/
  private boolean stageDefValidationError = false;
  /*Indicates if there is an error while processing stage error definition enum*/
  private boolean enumValidationFailure = false;
  /*Map of enum names that need resource bundles to names of resource bundles*/
  private Set<String> enumsNeedingResourceBundles;
  /*literal vs value maps for the stage error def enum*/
  //private Map<String, Map<String, String>> errorEnumToLiteralsMap;
  /*Json object mapper to generate json file for the stages*/
  private final ObjectMapper json;
  /*Set of stage names for which resource bundles must be generated*/
  private final Set<String> stagesNeedingResourceBundles;


  /***********************************************/
  /**************** public API *******************/
  /***********************************************/

  public PipelineAnnotationsProcessor() {
    super();
    stageDefinitions = new ArrayList<>();
    stageNameToVersionMap = new HashMap<>();
//    errorEnumToLiteralsMap = new HashMap<>();
    json = new ObjectMapper();
    json.enable(SerializationFeature.INDENT_OUTPUT);
    stagesNeedingResourceBundles = new HashSet<>();
    enumsNeedingResourceBundles = new HashSet<>();
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {

    //process classes annotated with StageDef annotation
    //Also note down stages which need resource bundle generation
    for(Element e : roundEnv.getElementsAnnotatedWith(StageDef.class)) {
      ElementKind eKind = e.getKind();
      //It will most likely be a class. being extra safe
      if(eKind.isClass()) {
        stageDefinitions.add(createStageConfig((TypeElement)e));
      }
    }

    //Find all elements that need resource bundle generation and separate out the enums implementing ErrorId interface
    //validate such enums and mark them for resource bundle generation
    for(Element e : roundEnv.getElementsAnnotatedWith(GenerateResourceBundle.class)) {
      ElementKind eKind = e.getKind();
      if(eKind.isClass()) {
        TypeElement typeElement = (TypeElement) e;
        String elementName = StageHelper.getStageNameFromClassName(typeElement.getQualifiedName().toString());
        if(stageNameToVersionMap.containsKey(elementName)) {
          //these are stages needing resource bundles.
          stagesNeedingResourceBundles.add(typeElement.getQualifiedName().toString());
        } else if(validateEnums(typeElement)) {
          //As of now these have to be enums that implement ErrorId. Validate and note down enums needing resource
          //bundle generation
          //createErrorEnum(typeElement);
          enumsNeedingResourceBundles.add(typeElement.getQualifiedName().toString());
        } else {
          //error scenario - neither a stage nor enum but has GenerateResourceBundle annotation on it
          printError("validation.not.a.stage.or.enum",
              "Class {} is neither a stage implementation nor an ErrorId implementation but is annotated with " +
                  "'GenerateResourceBundle' annotation. This annotation is supported only on stage or ErrorId " +
                  "implementations", typeElement.getQualifiedName());
        }
      }
    }

    //generate stuff only in the last round.
    // Last round is meant for cleanup and stuff but it is ok to generate
    // because they are not source files.
    if(roundEnv.processingOver()) {
      //generate a json file containing all the stage definitions
      //generate a -bundle.properties file containing the labels and descriptions of
      // configuration options
      if(!stageDefValidationError) {
        generateConfigFile();
        generateClassNeedingResourceBundle();
      }
    }
    return true;
  }

  /**************************************************************************/
  /********************* Private helper methods *****************************/
  /**************************************************************************/

  /**
   * generates datacollector-resource-bundles.json which contains names of all stages, enums that need resource bundle
   * generation.
   */
  private void generateClassNeedingResourceBundle() {
    Set<String> stagesAndEnumsNeedingResourceBundle = new HashSet<>();
    stagesAndEnumsNeedingResourceBundle.addAll(stagesNeedingResourceBundles);
    stagesAndEnumsNeedingResourceBundle.addAll(enumsNeedingResourceBundles);
    if(!stagesAndEnumsNeedingResourceBundle.isEmpty()) {
      try {
        FileObject resource = processingEnv.getFiler().createResource(StandardLocation.CLASS_OUTPUT, "",
          DC_RESOURCE_BUNDLES_JSON);
        json.writeValue(resource.openOutputStream(), stagesAndEnumsNeedingResourceBundle);
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
        PIPELINE_STAGES_JSON);
      json.writeValue(resource.openOutputStream(), BeanHelper.wrapStageDefinitions(stageDefinitions));
    } catch (IOException e) {
      processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getMessage());
    }
  }

  /**
   * Creates and populates an instance of StageConfiguration from the Stage definition
   * @param typeElement The type element on which the stage annotation is present
   * @return returns a StageDefinition object
   */
  private StageDefinition createStageConfig(TypeElement typeElement) {
    StageDef stageDefAnnotation = typeElement.getAnnotation(StageDef.class);

    boolean errorStage = typeElement.getAnnotation(ErrorStage.class) != null;

    //Process all fields with ConfigDef annotation
    List< ConfigDefinition> configDefinitions = getConfigDefsFromTypeElement(typeElement);

    StageDefinition stageDefinition = null;
    if(validateStageDef(typeElement, stageDefAnnotation)) {
      RawSource rawSourceAnnot = typeElement.getAnnotation(RawSource.class);
      RawSourceDefinition rawSourceDefinition = null;
      if(rawSourceAnnot != null) {
        rawSourceDefinition = getRawSourceDefinition(rawSourceAnnot);
      }

      boolean variableOutputStreams = isVariableOutputStreams(stageDefAnnotation);
      int outputStreams = getOutputStreams(typeElement, stageDefAnnotation);
      String outputStreamsLabelProviderClass = getOutputStreamLabelsProviderClass(stageDefAnnotation);

      StageType stageType = StageType.valueOf(getStageTypeFromElement(typeElement));
      HideConfig hideConfigAnnotation = typeElement.getAnnotation(HideConfig.class);

      boolean requiredFields = stageType != StageType.SOURCE &&
                               (hideConfigAnnotation == null || !hideConfigAnnotation.requiredFields());
      boolean onRecordError = hideConfigAnnotation == null || !hideConfigAnnotation.onErrorRecord();
      String stageName = StageHelper.getStageNameFromClassName(typeElement.getQualifiedName().toString());
      stageDefinition = new StageDefinition(
          typeElement.getQualifiedName().toString(),
          stageName,
          stageDefAnnotation.version(),
          stageDefAnnotation.label(),
          stageDefAnnotation.description(),
          stageType,
          errorStage,
          requiredFields,
          onRecordError,
          configDefinitions,
          rawSourceDefinition,
          stageDefAnnotation.icon(),
          getConfigOptionGroupsForStage(typeElement),
          variableOutputStreams,
          outputStreams,
          outputStreamsLabelProviderClass
      );
    } else {
      stageDefValidationError = true;
    }
    return stageDefinition;
  }

  private ConfigGroupDefinition getConfigOptionGroupsForStage(TypeElement typeElement) {
    Map<String, List<VariableElement>> allConfigGroups = getAllConfigGroups(typeElement);

    Map<String, List<String>> classNameToGroupsMap = new HashMap<>();
    List<Map<String, String>> groupNameToLabelMapList = new ArrayList<>();

    for(Map.Entry<String, List<VariableElement>> v : allConfigGroups.entrySet()) {
      List<String> groupNames = new ArrayList<>();
      classNameToGroupsMap.put(v.getKey(), groupNames);
      for(VariableElement variableElement : v.getValue()) {
        groupNames.add(variableElement.getSimpleName().toString());
        Map<String, String> groupNameToLabelMap = new LinkedHashMap<>();
        groupNameToLabelMap.put("name", variableElement.getSimpleName().toString());
        groupNameToLabelMap.put("label", (String) variableElement.getConstantValue());
        groupNameToLabelMapList.add(groupNameToLabelMap);
      }
    }

    return new ConfigGroupDefinition(classNameToGroupsMap, groupNameToLabelMapList);
  }

  private List<ConfigDefinition> getConfigDefsFromTypeElement(TypeElement typeElement) {
    List<ConfigDefinition> configDefinitions = new ArrayList<>();
    Set<String> configPropsToSkip = new HashSet<>();
    HideConfig hideConfigAnnotation = typeElement.getAnnotation(HideConfig.class);
    if(hideConfigAnnotation != null) {
      for(String config : hideConfigAnnotation.value()) {
        configPropsToSkip.add(config);
      }
    }
    Map<String, VariableElement> allFields = getAllFields(typeElement);
    for (VariableElement variableElement : allFields.values()) {
      if(configPropsToSkip.contains(variableElement.getSimpleName().toString())) {
        continue;
      }
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
            ModelType modelType = ModelType.FIELD_SELECTOR_MULTI_VALUED;
            if(fieldSelector.singleValued()) {
              modelType = ModelType.FIELD_SELECTOR_SINGLE_VALUED;
            }
            model = new ModelDefinition(modelType, null, null, null, null);
          }
          FieldValueChooser fieldValueChooser = variableElement.getAnnotation(FieldValueChooser.class);
          //processingEnv.
          if (fieldValueChooser != null) {
            model = new ModelDefinition(ModelType.FIELD_VALUE_CHOOSER,
                                        getValuesProvider(fieldValueChooser)
                , null, null, null);
          }
          ValueChooser valueChooser = variableElement.getAnnotation(ValueChooser.class);
          if(valueChooser != null) {
            model = new ModelDefinition(ModelType.VALUE_CHOOSER,
                                        getValuesProvider(valueChooser)
                , null, null, null);
          }
          LanePredicateMapping lanePredicateMapping = variableElement.getAnnotation(LanePredicateMapping.class);
          if (lanePredicateMapping != null) {
            model = new ModelDefinition(ModelType.LANE_PREDICATE_MAPPING, null, null, null, null);
          }
          ComplexField complexField = variableElement.getAnnotation(ComplexField.class);
          if(complexField != null) {
            String typeName = getTypeNameFromComplexField(variableElement);
            model = new ModelDefinition(ModelType.COMPLEX_FIELD, null, null, null,
              getConfigDefsFromTypeElement(getTypeElementFromName(typeName)));
          }
        }

        ConfigDefinition configDefinition = new ConfigDefinition(
            variableElement.getSimpleName().toString(),
            configDefAnnot.type(),
            configDefAnnot.label(),
            configDefAnnot.description(),
            getDefaultValue(variableElement, configDefAnnot.defaultValue()),
            configDefAnnot.required(),
            configDefAnnot.group(),
            variableElement.getSimpleName().toString(),
            model,
            configDefAnnot.dependsOn(),
            getTriggeredByValues(configDefAnnot.triggeredByValue(), allFields.get(configDefAnnot.dependsOn())),
            configDefAnnot.displayPosition(),
            new ArrayList<ElFunctionDefinition>(),
            new ArrayList<ElConstantDefinition>(),
            configDefAnnot.min(),
            configDefAnnot.max(),
            getMimeString(configDefAnnot),
            configDefAnnot.lines());
        configDefinitions.add(configDefinition);
      }
    }
    return configDefinitions;
  }

  private String getMimeString(ConfigDef configDefAnnot) {
    switch(configDefAnnot.mode()) {
      case JSON:
        return "application/json";
      case PLAIN_TEXT:
        return "text/plain";
      case PYTHON:
        return "text/x-python";
      case JAVASCRIPT:
        return "text/javascript";
      case RUBY:
        return "text/x-ruby";
      case JAVA:
        return "text/x-java";
      case SCALA:
        return "text/x-scala";
      default:
        return null;
    }
  }

  private String getTypeNameFromComplexField(VariableElement variableElement) {
    String typeName = variableElement.asType().toString();
    if(typeName.startsWith("java.util.List")) {
      typeName = typeName.substring(15, typeName.length() - 1);
    }
    return typeName;
  }

  /**
   * Converts the argument string into the corresponding type.
   * If the argument cannot be converted to Integer or Long successfully a default value of 0 is returned.
   *
   * @param variableElement
   * @param defaultValue
   * @return
   */
  private Object getDefaultValue(VariableElement variableElement, String defaultValue) {
    TypeKind typeKind = variableElement.asType().getKind();
    if(typeKind.equals(TypeKind.BOOLEAN)) {
      return Boolean.parseBoolean(defaultValue);
    } else if (typeKind.equals(TypeKind.INT)) {
      try {
        return Integer.parseInt(defaultValue);
      } catch (NumberFormatException e) {
        return 0;
      }
    } else if (typeKind.equals(TypeKind.LONG)) {
      try {
        return Long.parseLong(defaultValue);
      } catch (NumberFormatException e) {
        return 0;
      }
    } else if (typeKind.equals(TypeKind.CHAR)) {
      if(defaultValue != null && !defaultValue.isEmpty()) {
        return defaultValue.charAt(0);
      }
      return ' ';
    }
    //If String or Model, return the string as is
    return defaultValue;
  }

  private List<Object> getTriggeredByValues(String[] defaultValue, VariableElement dependsOn) {
    List<Object> result = new ArrayList<>();
    for(String value : defaultValue) {
      TypeKind typeKind = dependsOn.asType().getKind();
      if (typeKind.equals(TypeKind.BOOLEAN)) {
        result.add(Boolean.parseBoolean(value));
      } else if (typeKind.equals(TypeKind.INT)) {
        try {
          result.add(Integer.parseInt(value));
        } catch (NumberFormatException e) {
          //Invalid type
        }
      } else if (typeKind.equals(TypeKind.LONG)) {
        try {
          result.add(Long.parseLong(value));
        } catch (NumberFormatException e) {
          //Invalid type
        }
      } else {
        //char, enum, list and map are not handled here
        result.add(value);
      }
    }
    //If String or Model, return the string as is
    return result;
  }

  private RawSourceDefinition getRawSourceDefinition(RawSource rawSourceAnnot) {
    //process all fields annotated with ConfigDef annotation in raw source provider
    String rawSourcePreviewerClass = getRawSourcePreviewer(rawSourceAnnot);
    TypeElement typeElement = getTypeElementFromName(rawSourcePreviewerClass);
    List<ConfigDefinition> configDefs = getConfigDefsFromTypeElement(typeElement);
    return new RawSourceDefinition(rawSourcePreviewerClass, rawSourceAnnot.mimeType(), configDefs);
  }

  /**
   * Returns all fields present in the class hierarchy of the type element
   * @param typeElement
   * @return
   */
  private Map<String, VariableElement> getAllFields(TypeElement typeElement) {
    List<Element> enclosedElements = new ArrayList<>();
    List<TypeMirror> allSuperTypes = getAllSuperTypes(typeElement);
    //We like the fields to be returned in the order of their declaration in the class.
    //Also the properties declared in the base class should precede the properties declared in the derived class.
    for(int i = allSuperTypes.size() -1; i>=0; i--) {
      TypeMirror typeMirror = allSuperTypes.get(i);
      //All super types are TypeElements, getAllSuperTypes method already does this check
      TypeElement t = (TypeElement)processingEnv.getTypeUtils().asElement(typeMirror);
      if(t.getEnclosedElements() != null) {
        enclosedElements.addAll(t.getEnclosedElements());
      }
    }
    enclosedElements.addAll(typeElement.getEnclosedElements());
    List<VariableElement> variableElements = ElementFilter.fieldsIn(enclosedElements);
    Map<String, VariableElement> result = new LinkedHashMap<>();
    for(VariableElement v : variableElements) {
      result.put(v.getSimpleName().toString(), v);
    }
    return result;
  }

  /**
   * Infers the type of stage based on the interface implemented or the
   * abstract class extended.
   *
   * @param typeElement the element from which the type must be extracted
   * @return the type
   */
  private String getStageTypeFromElement(TypeElement typeElement) {

    //Check if the stage extends one of the abstract classes
    for(TypeMirror typeMirror : getAllSuperTypes(typeElement)) {
      if (isSource(typeMirror)) {
        return SOURCE;
      } else if (isProcessor(typeMirror)) {
        return PROCESSOR;
      } else if (isTarget(typeMirror)) {
        return TARGET;
      } else if (isError(typeMirror)) {
        return ERROR;
      } else if (isLabel(typeMirror)) {
        return LABEL;
      }
    }
    return "";
  }

  private boolean isError(TypeMirror typeMirror) {
    if(typeMirror.toString().equals("com.streamsets.pipeline.api.ErrorCode")) {
      return true;
    }
    return false;
  }

  private boolean isLabel(TypeMirror typeMirror) {
    if(typeMirror.toString().equals("com.streamsets.pipeline.api.Label")) {
      return true;
    }
    return false;
  }

  private boolean isSource(TypeMirror typeMirror) {
    if(typeMirror.toString().equals("com.streamsets.pipeline.api.base.BaseSource")
      || typeMirror.toString().equals("com.streamsets.pipeline.api.Source")) {
      return true;
    }
    return false;
  }

  private boolean isProcessor(TypeMirror typeMirror) {
    if(typeMirror.toString().equals("com.streamsets.pipeline.api.base.BaseProcessor")
      || typeMirror.toString().equals("com.streamsets.pipeline.api.base.RecordProcessor")
      || typeMirror.toString().equals("com.streamsets.pipeline.api.base.SingleLaneProcessor")
      || typeMirror.toString().equals("com.streamsets.pipeline.api.base.SingleLaneRecordProcessor")
      || typeMirror.toString().equals("com.streamsets.pipeline.api.Processor")) {
      return true;
    }
    return false;
  }

  private boolean isTarget(TypeMirror typeMirror) {
    if(typeMirror.toString().equals("com.streamsets.pipeline.api.base.BaseTarget")
      || typeMirror.toString().equals("com.streamsets.pipeline.api.Target")) {
      return true;
    }
    return false;
  }

  /**
   * Recursively finds all super types of this element including the interfaces
   * @param element the element whose super types must be found
   * @return the list of super types in no particular order
   */
  private List<TypeMirror> getAllSuperTypes(TypeElement element){
    List<TypeMirror> allSuperTypes=new ArrayList<>();
    Queue<TypeMirror> runningList=new LinkedList<>();
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
      Utils.format(template, args));
  }

  private String getValuesProvider(FieldValueChooser fieldValueChooser) {
    //Not the best way of getting the TypeMirror of the ChooserValues implementation
    //Find a better solution
    TypeMirror valueProviderTypeMirror = null;
    try {
      fieldValueChooser.value();
    } catch (MirroredTypeException e) {
      valueProviderTypeMirror = e.getTypeMirror();
    }

    return getClassNameFromTypeMirror(valueProviderTypeMirror);
  }

  private String getClassNameFromTypeMirror(TypeMirror valueProviderTypeMirror) {
    List<String> outerClasses = new ArrayList<>();

    //check if the values provider is inner class
    //Make sure values provider implementation is top level
    Element e = processingEnv.getTypeUtils().asElement(valueProviderTypeMirror);
    Element enclosingElement = e.getEnclosingElement();
    while(!enclosingElement.getKind().equals(ElementKind.PACKAGE)) {
      outerClasses.add(enclosingElement.getSimpleName().toString());
      enclosingElement = enclosingElement.getEnclosingElement();
    }

    //append package name
    PackageElement packageElement = (PackageElement) enclosingElement;
    StringBuilder sb = new StringBuilder();
    sb.append(packageElement.getQualifiedName().toString());
    sb.append(DOT);
    //append outer class names followed by '$', in the reverse order
    if(!outerClasses.isEmpty()) {
      for(int i = outerClasses.size()-1 ; i >=0; i--) {
        sb.append(outerClasses.get(i)).append("$");
      }
    }

    //finally append the values provider name
    sb.append(getTypeElementFromMirror(valueProviderTypeMirror).getSimpleName().toString());

    return sb.toString();
  }

  private String getValuesProvider(ValueChooser valueChooser) {
    //Not the best way of getting the TypeMirror of the ChooserValues implementation
    //Find a better solution
    TypeMirror valueProviderTypeMirror = null;
    try {
      valueChooser.value();
    } catch (MirroredTypeException e) {
      valueProviderTypeMirror = e.getTypeMirror();
    }

    if(valueProviderTypeMirror !=null) {
      return getTypeElementFromMirror(valueProviderTypeMirror).getQualifiedName().toString();
    }
    return null;
  }

  private String getRawSourcePreviewer(RawSource rawSource) {
    //Not the best way of getting the TypeMirror of the ChooserValues implementation
    //Find a better solution
    TypeMirror rspTypeMirror = null;
    try {
      rawSource.rawSourcePreviewer();
    } catch (MirroredTypeException e) {
      rspTypeMirror = e.getTypeMirror();
    }

    if(rspTypeMirror !=null) {
      return getTypeElementFromMirror(rspTypeMirror).getQualifiedName().toString();
    }
    return null;
  }

  private TypeElement getTypeElementFromMirror(TypeMirror typeMirror) {
    return getTypeElementFromName(typeMirror.toString());
  }

  private TypeElement getTypeElementFromName(String qualifiedName) {
    return processingEnv.getElementUtils().getTypeElement(qualifiedName);
  }

  /**************************************************************************/
  /***************** Validation related methods *****************************/
  /**************************************************************************/

  /**
   * Validates that the stage definition implements the expected interface or
   * extends from the expected abstract base class.
   *
   * Also validates that the stage is not an inner class.
   *
   * @param typeElement
   */
  private boolean validateInterface(TypeElement typeElement) {
    Element enclosingElement = typeElement.getEnclosingElement();
    if(!enclosingElement.getKind().equals(ElementKind.PACKAGE)) {
      printError("stagedef.validation.not.outer.class",
        "Stage {} is an inner class. Inner class Stage implementations are not supported",
        typeElement.getSimpleName().toString());
    }
    if(getStageTypeFromElement(typeElement).isEmpty()) {
      //Stage does not implement one of the Stage interface or extend the base stage class
      //This must be flagged as a compiler error.
      printError("stagedef.validation.does.not.implement.interface",
        "Stage {} neither extends one of BaseSource, BaseProcessor, BaseTarget classes nor implements one of Source, " +
          "Processor, Target interface.",
        typeElement.getQualifiedName().toString());
      //Continue for now to find out if there are more issues.
      return false;
    }
    return true;
  }

  /**
   * Validates the field on which the ConfigDef annotation is specified.
   * The following validations are done:
   * <ul>
   *   <li>The field must be declared as public</li>
   *   <li>The type of the field must match the type specified in the ConfigDef annotation</li>
   *   <li>If the type is "MODEL" then exactly one of "FieldSelector" or "FieldValueChooser" or "ValueChooser"
   *   annotation must be present</li>
   * </ul>
   *
   * @param variableElement
   * @param configDefAnnot
   */
  private boolean validateConfigDefAnnotation(TypeElement typeElement, VariableElement variableElement,
                                              ConfigDef configDefAnnot) {
    boolean valid = true;
    //field must be declared public
    if(!variableElement.getModifiers().contains(Modifier.PUBLIC)) {
      printError("field.validation.not.public",
        "The field {} has \"ConfigDef\" annotation but is not declared public. " +
          "Configuration fields must be declared public.",
        typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
      valid = false;
    }
    //field must not be final
    if(variableElement.getModifiers().contains(Modifier.FINAL)) {
      printError("field.validation.final.field",
        "The field {} has \"ConfigDef\" annotation and is declared final. " +
          "Configuration fields must not be declared final.",
        typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString()
      );
      valid = false;
    }
    //field must not be static
    if(variableElement.getModifiers().contains(Modifier.STATIC)) {
      printError("field.validation.static.field",
        "The field {} has \"ConfigDef\" annotation and is declared static. " +
          "Configuration fields must not be declared final.",
        typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
      valid = false;
    }

    if(configDefAnnot.type().equals(ConfigDef.Type.MODEL)) {
      valid &= validateModelConfig(typeElement, variableElement);
    } else {
      valid &= validateModelAnnotationsAreNotPresent(typeElement, variableElement);
      valid &= validateNonModelConfig(configDefAnnot, typeElement, variableElement);
    }

    //validate DependsOn and triggered by
    valid &= validateDependsOn(typeElement, variableElement, configDefAnnot);

    //validate min and max
    valid &= validateMinMax(typeElement, variableElement, configDefAnnot);

    return valid;
  }

  private boolean validateMinMax(TypeElement typeElement, VariableElement variableElement, ConfigDef configDefAnnot) {
    boolean valid = true;
    TypeMirror fieldType = variableElement.asType();
    //if min and max has non default values make sure that the type of the field is int or long
    if (configDefAnnot.max() != Long.MAX_VALUE || configDefAnnot.min() != Long.MIN_VALUE) {
      if (!(fieldType.getKind().equals(TypeKind.INT) || fieldType.getKind().equals(TypeKind.LONG))) {
        printError("field.validation.min.max.specified.for.non.number",
          "The type of the field {} is not a number but has min and max values specified in the configuration.",
          typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
        valid = false;
      }
    }
    return valid;
  }

  private boolean validateDependsOn(TypeElement typeElement, VariableElement variableElement, ConfigDef configDefAnnot) {
    String dependsOnConfig = configDefAnnot.dependsOn();
    if(dependsOnConfig != null && !dependsOnConfig.isEmpty()) {
      boolean valid = false;
      for(VariableElement v : getAllFields(typeElement).values()) {
        if(v.getSimpleName().toString().equals(dependsOnConfig) && v.getAnnotation(ConfigDef.class) != null) {
          valid = true;
        }
      }
      if(!valid) {
        printError("field.validation.dependsOn",
          "The \"ConfigDef\" annotation for field {} indicates that it depends on field '{}' which does not exist or " +
            "is not a configuration option.",
          typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString(),
          dependsOnConfig);
      }
      return valid;
    }
    return true;
  }

  private boolean validateModelAnnotationsAreNotPresent(Element typeElement, VariableElement variableElement) {
    FieldValueChooser fieldValueChooser = variableElement.getAnnotation(FieldValueChooser.class);
    FieldSelector fieldSelector = variableElement.getAnnotation(FieldSelector.class);
    ValueChooser valueChooser = variableElement.getAnnotation(ValueChooser.class);

    if(fieldValueChooser != null || fieldSelector != null || valueChooser != null) {
      printError("field.validation.model.annotations.present",
          "The type of field {} is not declared as \"MODEL\". 'FieldSelector' or 'FieldValueChooser' or " +
              "'ValueChooser' annotation is not expected, but is present.",
          typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
      return false;
    }
    return true;
  }

  private boolean validateNonModelConfig(ConfigDef configDefAnnot, Element typeElement,
                                         VariableElement variableElement) {
    boolean valid = true;
    //type match
    TypeMirror fieldType = variableElement.asType();
    if (configDefAnnot.type().equals(ConfigDef.Type.BOOLEAN)) {
      if(!fieldType.getKind().equals(TypeKind.BOOLEAN)) {
        printError("field.validation.type.is.not.boolean",
            "The type of the field {} is expected to be boolean.",
            typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
        valid = false;
      }
      if (fieldType.getKind().equals(TypeKind.BOOLEAN)) {
        String value = configDefAnnot.defaultValue();
        if(!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
          printError("field.validation.default.value.not.boolean",
              "The type of the field {} is Boolean but the default value supplied is not true or false.",
              typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
          valid = false;
        }
      }
    } else if (configDefAnnot.type().equals(ConfigDef.Type.NUMBER)) {
      if(!(fieldType.getKind().equals(TypeKind.INT) || fieldType.getKind().equals(TypeKind.LONG))) {
        printError("field.validation.type.is.not.int.or.long",
            "The type of the field {} is expected to be either an int or a long.",
            typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
        valid = false;
      }
      //validate the default values
      if (fieldType.getKind().equals(TypeKind.INT)) {
        try {
          Integer.parseInt(configDefAnnot.defaultValue());
        } catch (NumberFormatException e) {
          printError("field.validation.default.value.not.int",
              "The type of the field {} is Integer but the default value supplied is not Integer.",
              typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
          valid = false;
        }
      }
      if (fieldType.getKind().equals(TypeKind.LONG)) {
        try {
          Integer.parseInt(configDefAnnot.defaultValue());
        } catch (NumberFormatException e) {
          printError("field.validation.default.value.not.long",
              "The type of the field {} is Long but the default value supplied is not Long.",
              typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
          valid = false;
        }
      }
    } else if (configDefAnnot.type().equals(ConfigDef.Type.STRING)) {
      if(!fieldType.toString().equals("java.lang.String")) {
        printError("field.validation.type.is.not.string",
            "The type of the field {} is expected to be String.",
            typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
        valid = false;
      }
    } else if (configDefAnnot.type().equals(ConfigDef.Type.MAP)) {
      if(!fieldType.toString().startsWith("java.util.Map<java.lang.String,")) {
        printError("field.validation.type.is.not.map",
                   "The type of the field {} is expected to be Map<String, ?>.",
                   typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
        valid = false;
      }
    } else if (configDefAnnot.type().equals(ConfigDef.Type.TEXT)) {
      if(!fieldType.toString().equals("java.lang.String")) {
        printError("field.validation.type.is.not.string",
          "The type of the field {} is expected to be String.",
          typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
        valid = false;
      }
    }

    if(configDefAnnot.mode() != ConfigDef.Mode.PLAIN_TEXT && !configDefAnnot.type().equals(ConfigDef.Type.TEXT)) {
      printError("field.validation.type.is.not.text",
        "The type of the field {} is expected to be ConfigDef.Type.TEXT.",
        typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
      valid = false;
    }

    if(!configDefAnnot.type().equals(ConfigDef.Type.TEXT) && configDefAnnot.lines() != 0) {
      printError("field.validation.lines.is.not.zero",
        "The type of the field {} is not ConfigDef.Type.TEXT. The value expected for lines() is zero.",
        typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
      valid = false;
    }

    return valid;
  }

  private static final Set<Class<? extends Annotation>> MODEL_ANNOTATIONS = new LinkedHashSet<>();
  private static final Set<String> MODEL_ANNOTATIONS_NAMES = new LinkedHashSet<>();

  static {
    MODEL_ANNOTATIONS.add(FieldValueChooser.class);
    MODEL_ANNOTATIONS.add(FieldSelector.class);
    MODEL_ANNOTATIONS.add(ValueChooser.class);
    MODEL_ANNOTATIONS.add(LanePredicateMapping.class);
    MODEL_ANNOTATIONS.add(ComplexField.class);
    for (Class<?> klass : MODEL_ANNOTATIONS) {
      MODEL_ANNOTATIONS_NAMES.add(klass.getSimpleName());
    }
  }

  private boolean hasModelAnnotation(VariableElement variableElement) {
    int models = 0;
    for (Class<? extends Annotation> annotationClass : MODEL_ANNOTATIONS) {
      if (variableElement.getAnnotation(annotationClass) != null) {
        models++;
      }
    }
    return models == 1;
  }

  /**
   * Validates FieldValueChooser, FieldSelector and ValueChooser annotations
   *
   * @param typeElement
   * @param variableElement
   * @return
   */
  private boolean validateModelConfig(Element typeElement, VariableElement variableElement) {
    boolean valid = true;

    //Field is marked as model.
    //Carry out model related validations
    if(!hasModelAnnotation(variableElement)) {
      printError("field.validation.no.model.annotation",
          "The type of field {} is declared as \"MODEL\". Exactly one of '{}' annotations is expected.",
          typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString(),
          MODEL_ANNOTATIONS_NAMES);
      valid = false;
    }

    //Validate model annotations

    FieldValueChooser fieldValueChooser = variableElement.getAnnotation(FieldValueChooser.class);
    if (fieldValueChooser != null) {
      valid &= validateFieldModifier(typeElement, variableElement, fieldValueChooser);
    }

    ValueChooser valueChooser = variableElement.getAnnotation(ValueChooser.class);
    if (valueChooser != null) {
      valid &= validateDropDown(typeElement, variableElement, valueChooser);
    }

    FieldSelector fieldSelector = variableElement.getAnnotation(FieldSelector.class);
    if (fieldSelector != null) {
      valid &= validateFieldSelector(typeElement, variableElement, fieldSelector);
    }

    LanePredicateMapping lanePredicateMapping = variableElement.getAnnotation(LanePredicateMapping.class);
    if (lanePredicateMapping != null) {
      valid &= validateLanePredicateMapping(typeElement, variableElement);
    }

    ComplexField complexField = variableElement.getAnnotation(ComplexField.class);
    if (complexField != null) {
      valid &= validateComplexField(typeElement, variableElement);
    }
    return valid;
  }

  private boolean validateComplexField(Element typeElement, VariableElement variableElement) {
    boolean valid = true;
    String typeName = getTypeNameFromComplexField(variableElement);
    TypeElement t = getTypeElementFromName(typeName);

    if(t.getKind() != ElementKind.CLASS) {
      printError("ComplexField.type.not class",
        "Complex Field type '{}' is not declared as class. " +
          "A Complex Field type must be a class.",
        t.getSimpleName().toString());
      valid = false;
    }

    Element enclosingElement = t.getEnclosingElement();
    if(!enclosingElement.getKind().equals(ElementKind.PACKAGE)) {
      //inner class, check for static modifier
      if(!t.getModifiers().contains(Modifier.STATIC)) {
        printError("ComplexField.type.inner.class.and.not.static",
          "Complex Field type '{}' is an inner class but is not declared as static. " +
            "Inner class Complex Field types must be declared static.",
          getClassNameFromTypeMirror(t.asType()));
        valid = false;
      }
    }

    for (VariableElement v : getAllFields(t).values()) {
      ComplexField complexField = v.getAnnotation(ComplexField.class);
      if(complexField != null) {
        printError("ComplexField.type.declares.complex.fields",
          "Complex Field type '{}' has configuration properties which are complex fields." +
            "Complex Field types must not define fields which are complex types.",
          t.getSimpleName().toString());
        valid = false;
      }
      ConfigDef configDefAnnot = v.getAnnotation(ConfigDef.class);
      if(configDefAnnot != null) {
        valid &= validateConfigDefAnnotation(t, v, configDefAnnot);
      }
    }

    return valid;
  }

  private boolean validateFieldSelector(Element typeElement, VariableElement variableElement,
                                        FieldSelector fieldSelector) {
    boolean valid = true;
    if (!fieldSelector.singleValued() &&
      !variableElement.asType().toString().equals("java.util.List<java.lang.String>")) {
      printError("field.validation.type.is.not.list",
          "Field {} is annotated as multi valued FieldSelector. The type of the field {} expected to be List<String>.",
          typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
      valid = false;
    }
    if (fieldSelector.singleValued() && !variableElement.asType().toString().equals("java.lang.String")) {
      printError("field.validation.type.is.not.String",
        "Field {} is annotated as single valued FieldSelector. The type of the field is expected to be String.",
        typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
      valid = false;
    }
    return valid;
  }

  private boolean validateLanePredicateMapping(Element typeElement, VariableElement variableElement) {
    boolean valid = true;
    if (!variableElement.asType().toString().equals(
      "java.util.List<java.util.Map<java.lang.String,java.lang.String>>")) {
      printError("field.validation.type.is.not.map",
                 "The type of the field {} is expected to be Map<String, String>.",
                 typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
      valid = false;
    }
    return valid;
  }

  private boolean validateDropDown(Element typeElement, VariableElement variableElement, ValueChooser valueChooser) {
    boolean valid = true;

    //The type of could be string or an enum
    valid &= checkIfTypeIsEnumOrString(typeElement, variableElement, variableElement.asType().toString());
    //A chooserValues is expected.
    //check if the chooserValues is specified and that implements the correct base class
    //Not the best way of getting the TypeMirror of the ChooserValues implementation
    //Find a better solution
    TypeMirror valueProviderTypeMirror = null;
    try {
      valueChooser.value();
    } catch (MirroredTypeException e) {
      valueProviderTypeMirror = e.getTypeMirror();
    }
    valid &= validateValueProvider(typeElement, variableElement, valueProviderTypeMirror);
    return valid;
  }

  private boolean validateFieldModifier(Element typeElement, VariableElement variableElement,
                                        FieldValueChooser fieldValueChooser) {
    boolean valid = true;
    TypeMirror fieldType = variableElement.asType();
    String type = fieldType.toString();
    //FIXME<Hari>: Investigate and find a better way to do this
    //check if type is Map<String, String> or Map<String, Enum>
    if(type.contains(MAP_TYPE_WITH_KEY)) {
      //get the second type in the map
      String valueType = type.substring(MAP_TYPE_WITH_KEY.length(), type.length() -1);
      //valueType must be string or enum
      if (!valueType.equals("java.lang.String")) {
        //check if it is an enum
        Element e = getTypeElementFromName(valueType);
        if(!e.getKind().equals(ElementKind.ENUM)) {
          printError("field.validation.type.is.not.map",
              "The type of the field {} is expected to be Map<String, String> or Map<String, Enum>.",
              typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
          valid = false;
        }
      }
    } else {
      printError("field.validation.type.is.not.map",
          "The type of the field {} is expected to be Map<String, String> or Map<String, Enum>.",
          typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
      valid = false;
    }

    //A chooserValues is expected.
    //check if the chooserValues is specified and that implements the correct base class
    //Not the best way of getting the TypeMirror of the ChooserValues implementation
    //Find a better solution
    TypeMirror valueProviderTypeMirror = null;
    try {
      fieldValueChooser.value();
    } catch (MirroredTypeException e) {
      valueProviderTypeMirror = e.getTypeMirror();
    }

    valid &= validateValueProvider(typeElement, variableElement, valueProviderTypeMirror);
    return valid;
  }

  private boolean validateValueProvider(Element typeElement, VariableElement variableElement,
                                     TypeMirror valueProviderTypeMirror) {
    boolean valid = true;
    if (valueProviderTypeMirror == null) {
      printError("field.validation.chooserValues.not.supplied",
          "The field {} marked with ValueChooser annotation and the type is \"PROVIDED\" but no ChooserValues " +
            "implementation is supplied",
          typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
      valid = false;
    }
    //if ChooserValues is inner class then it must be static so that it can be instantiated using reflection
    Element e = processingEnv.getTypeUtils().asElement(valueProviderTypeMirror);
    Element enclosingElement = e.getEnclosingElement();
    if(!enclosingElement.getKind().equals(ElementKind.PACKAGE)) {
      //inner class, check for static modifier
      if(!e.getModifiers().contains(Modifier.STATIC)) {
        printError("chooserValues.inner.class.and.not.static",
            "ChooserValues implementation '{}' is an inner class but is not declared as static. " +
                "Inner class ChooserValues implementations must be declared static.",
            getClassNameFromTypeMirror(valueProviderTypeMirror));
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
  private boolean validateAndCacheStageDef(TypeElement typeElement, StageDef stageDefAnnotation) {
    String stageName = StageHelper.getStageNameFromClassName(typeElement.getQualifiedName().toString());
    if(stageNameToVersionMap.containsKey(stageName) &&
      stageNameToVersionMap.get(stageName).equals(stageDefAnnotation.version())) {
      //found more than one stage with same name and version
      printError("stagedef.validation.duplicate.stages",
        "Multiple stage definitions found with the same name and version. Name {}, Version {}",
          stageName, stageDefAnnotation.version());
      //Continue for now to find out if there are more issues.
      return false;
    }
    stageNameToVersionMap.put(stageName, stageDefAnnotation.version());
    return true;
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
    boolean valid = true;
    valid &= validateInterface(typeElement);
    valid &= validateAndCacheStageDef(typeElement, stageDefAnnotation);
    valid &= validateStageForConstructor(typeElement);
    valid &= validateIconExists(typeElement, stageDefAnnotation);
    valid &= validateRawSource(typeElement);
    valid &= validateOptionGroups(typeElement);
    valid &= validateMultipleOutputStreams(typeElement, stageDefAnnotation);
    return valid;
  }

  private boolean validateOptionGroups(TypeElement typeElement) {
    boolean valid = true;
    List<TypeMirror> allTypes = new ArrayList<>();
    allTypes.add(typeElement.asType());
    allTypes.addAll(getAllSuperTypes(typeElement));
    Map<VariableElement, String> allConfigGroups = new HashMap<>();

    for(TypeMirror typeMirror : allTypes) {
      TypeElement t = (TypeElement)processingEnv.getTypeUtils().asElement(typeMirror);
      ConfigGroups configGroups = t.getAnnotation(ConfigGroups.class);
      if(configGroups != null) {
        TypeMirror cgTypeMirror = null;
        try {
          configGroups.value();
        } catch (MirroredTypeException e) {
          cgTypeMirror = e.getTypeMirror();
        }

        //Make sure raw source previewer implementation is top level
        Element cgElement = processingEnv.getTypeUtils().asElement(cgTypeMirror);
        if(cgElement.getKind() != ElementKind.ENUM) {
          printError("configOptionGroup.not.enum",
            "'{}' implements interface 'ConfigGroups.Groups' but is not an enum." +
              " An implementation of 'ConfigGroups.Groups' must be an enum.",
            cgElement.getSimpleName().toString());
          valid = false;
        }
        List<? extends Element> enclosedElements = cgElement.getEnclosedElements();
        for(VariableElement v : ElementFilter.fieldsIn(enclosedElements)) {
          if(v.getKind() == ElementKind.ENUM_CONSTANT) {
            allConfigGroups.put(v, cgElement.getSimpleName().toString());
          }
        }
      }
    }

    Set<String> groups = new HashSet<>();
    for(Map.Entry<VariableElement, String> v : allConfigGroups.entrySet()) {
      if(groups.contains(v.getKey().getSimpleName().toString())) {
        printError("configOptionGroup.duplicate.group.name",
          "Duplicate option group name {} encountered in Stage {}.",
          v.getValue() + SEPARATOR + v.getKey().getSimpleName().toString(), typeElement.getSimpleName().toString());
        valid = false;
      } else {
        groups.add(v.getKey().getSimpleName().toString());
      }
    }

    //check if the group names used in the config defs of type element are valid
    for(VariableElement v : getAllFields(typeElement).values()) {
      ConfigDef configDef = v.getAnnotation(ConfigDef.class);
      if(configDef != null && !configDef.group().isEmpty()) {
        if(!groups.contains(configDef.group())) {
          printError("ConfigDef.invalid.group.name",
            "Invalid group name {} specified in the \"ConfigDef\" annotation for field {}.",configDef.group(),
            typeElement.getSimpleName().toString() + SEPARATOR + v.getSimpleName().toString());
          valid = false;
        }
      }
    }

    return valid;
  }

  private Map<String, List<VariableElement>> getAllConfigGroups(TypeElement typeElement) {
    List<TypeMirror> allTypes = new ArrayList<>();
    allTypes.add(typeElement.asType());
    allTypes.addAll(getAllSuperTypes(typeElement));
    Map<String, List<VariableElement>> allConfigGroups = new LinkedHashMap<>();

    for(int i = allTypes.size()-1; i>=0; i--) {
      TypeElement t = (TypeElement)processingEnv.getTypeUtils().asElement(allTypes.get(i));
      ConfigGroups configGroups = t.getAnnotation(ConfigGroups.class);
      if(configGroups != null) {
        TypeMirror cgTypeMirror = null;
        try {
          configGroups.value();
        } catch (MirroredTypeException e) {
          cgTypeMirror = e.getTypeMirror();
        }

        //Make sure raw source previewer implementation is top level
        Element cgElement = processingEnv.getTypeUtils().asElement(cgTypeMirror);
        List<? extends Element> enclosedElements = cgElement.getEnclosedElements();
        List<VariableElement> variableElements = new ArrayList<>();
        allConfigGroups.put(getClassNameFromTypeMirror(cgTypeMirror), variableElements);
        for(VariableElement v : ElementFilter.fieldsIn(enclosedElements)) {
          if(v.getKind() == ElementKind.ENUM_CONSTANT) {
            variableElements.add(v);
          }
        }
      }
    }
    return allConfigGroups;
  }

  private boolean validateRawSource(TypeElement typeElement) {
    boolean valid = true;
    //check if RawSource annotation is present on stage
    RawSource rawSourceAnnot = typeElement.getAnnotation(RawSource.class);
    if(rawSourceAnnot != null) {
      //make sure that the current stage is a Source
      String stageTypeFromElement = getStageTypeFromElement(typeElement);
      if(stageTypeFromElement == null || !SOURCE.equals(stageTypeFromElement)) {
        printError("rawSource.validation.not.applied.on.source",
            "Annotation RawSource is applied on stage {} which is not a \"Source\".",
            typeElement.getQualifiedName());
        valid = false;
      }
      //Not the best way of getting the TypeMirror of the ChooserValues implementation
      //Find a better solution
      TypeMirror rspTypeMirror = null;
      try {
        rawSourceAnnot.rawSourcePreviewer();
      } catch (MirroredTypeException e) {
        rspTypeMirror = e.getTypeMirror();
      }

      //Since the rawSourcePreviewer is mandatory property, it cannot be null
      //and the generics enforce that it has to implement the required interface.
      assert(rspTypeMirror != null);

      //Make sure raw source previewer implementation is top level
      Element e = processingEnv.getTypeUtils().asElement(rspTypeMirror);
      Element enclosingElement = e.getEnclosingElement();
      if(!enclosingElement.getKind().equals(ElementKind.PACKAGE)) {
        printError("rawSource.validation.rawSourcePreviewer.not.outer.class",
            "RawSourcePreviewer {} is an inner class. Inner class RawSourcePreviewer implementations are not " +
              "supported.",rspTypeMirror.toString());
        valid = false;
      }
    }

    return valid;
  }

  /**
   * Validates that there exists a file as specified in the "icon" literal
   * of the StageDef annotation and that it ends with a ".svg" extension
   *
   * @param stageDefAnnotation
   * @return
   */
  private boolean validateIconExists(TypeElement typeElement, StageDef stageDefAnnotation) {
    // The following validations are done in the specified order in order to flag maximum
    // errors in one go
    //1. Check if the file exists
    //2. Access it
    //3. Check if it ends with .svg or .png extension
    boolean valid = true;
    if(stageDefAnnotation.icon() != null && !stageDefAnnotation.icon().isEmpty()) {
      try {
        FileObject resource = processingEnv.getFiler().getResource(StandardLocation.CLASS_PATH, ""
          , stageDefAnnotation.icon());
        if (resource == null) {
          printError("stagedef.validation.icon.file.does.not.exist",
            "Stage Definition {} supplies an icon {} which does not exist.",
            typeElement.getQualifiedName(), stageDefAnnotation.icon());
          valid = false;
        }
      } catch (IOException e) {
        printError("stagedef.validation.cannot.access.icon.file",
          "Stage Definition {} supplies an icon {} which cannot be accessed for the following reason : {}.",
          typeElement.getQualifiedName(), stageDefAnnotation.icon(), e.getMessage());
        valid = false;
      }

      if (!stageDefAnnotation.icon().endsWith(".svg") && !stageDefAnnotation.icon().endsWith(".png")) {
        printError("stagedef.validation.icon.not.svg",
          "Stage Definition {} supplies an icon {} which is not in an \"svg\" file.",
          typeElement.getQualifiedName(), stageDefAnnotation.icon());
        valid = false;
      }
    }
    return valid;
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

    while(!foundDefConstr &&
      !validationError && te != null) {
      List<? extends Element> enclosedElements = te.getEnclosedElements();
      List<ExecutableElement> executableElements = ElementFilter.constructorsIn(enclosedElements);
      for(ExecutableElement e : executableElements) {
        //found one or more constructors, check for default constr
        if(e.getSimpleName().toString().equals(DEFAULT_CONSTRUCTOR)
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
        "The Stage {} has constructor with arguments but no default constructor.",
        typeElement.getSimpleName());
    }

    return !validationError;
  }

  /**
   * Validates the Stage Error Definition
   * Requires that it be enum which implements interface com.streamsets.pipeline.api.ErrorCode
   *
   * @param typeElement
   */
  private boolean validateEnums(TypeElement typeElement) {
    boolean valid = true;
    //must be enum
    if(typeElement.getKind() != ElementKind.ENUM) {
      //Stage does not implement one of the Stage interface or extend the base stage class
      //This must be flagged as a compiler error.
      printError("stagedeferror.validation.not.an.enum",
        "Stage Error Definition {} must be an enum", typeElement.getQualifiedName());
      valid = false;
    }
    //must implement com.streamsets.pipeline.api.ErrorCode or com.streamsets.pipeline.api.Label
    String type = getStageTypeFromElement(typeElement);
    if(type.isEmpty() || !(type.equals("ERROR") || type.equals("LABEL"))) {
      //Stage does not implement one of the Stage interface or extend the base stage class
      //This must be flagged as a compiler error.
      printError("stagedef.validation.enum.does.not.implement.interface",
        "Stage Enum {} does not implement interface 'com.streamsets.pipeline.api.ErrorCode' " +
          "or 'com.streamsets.pipeline.api.Label'.",
        typeElement.getQualifiedName());
      valid = false;
    }
    enumValidationFailure &= !valid;
    return valid;
  }

  private boolean checkIfTypeIsEnumOrString(Element typeElement, VariableElement variableElement, String typeName) {
    if (!typeName.equals("java.lang.String")) {
      //check if it is an enum
      Element e = getTypeElementFromName(typeName);
      if(!e.getKind().equals(ElementKind.ENUM)) {
        printError("field.validation.type.is.not.String.or.Enum",
            "The type of the field {} is expected to be String or Enum.",
            typeElement.getSimpleName().toString() + SEPARATOR + variableElement.getSimpleName().toString());
        return false;
      }
    }
    return true;
  }

  private boolean validateMultipleOutputStreams(TypeElement typeElement, StageDef stageDef) {
    boolean valid = true;

    String stageType = getStageTypeFromElement(typeElement);

    if(TARGET.equals(stageType) && !isDefaultOutputStream(stageDef)) {
      printError("stagedef.validation.target.defines.outputStreams",
        "The stage {} is a target but identifies an output streams provider class '{}' which is not" +
          " \"com.streamsets.pipeline.api.StageDef$DefaultOutputStreams\".",
        typeElement.getSimpleName().toString(), getOutputStreamLabelsProviderClass(stageDef));
      valid = false;
    }

    if(isDefaultOutputStream(stageDef) && !stageDef.outputStreamsDrivenByConfig().isEmpty()) {
      printError("stagedef.validation.target.defines.DefaultOutputStreams.and.outputStreamsDrivenByConfig",
        "The stage {} identifies \"com.streamsets.pipeline.api.StageDef$DefaultOutputStreams\" as the output streams " +
          "provider class. It should not specify a value {} for 'outputStreamsDrivenByConfig'.",
        typeElement.getSimpleName().toString(), stageDef.outputStreamsDrivenByConfig());
      valid = false;
    }

    if(isVariableOutputStreams(stageDef) && !TARGET.equals(stageType)) {
      if(SOURCE.equals(stageType) || PROCESSOR.equals(stageType)) {
        if(stageDef.outputStreamsDrivenByConfig().isEmpty()) {
          printError("stagedef.validation.variableOutputStreams.but.no.outputStreamsDrivenByConfig",
            "The stage {} identifies \"com.streamsets.pipeline.api.StageDef$VariableOutputStreams\" as the output " +
              "streams provider class but does not specify the 'outputStreamsDrivenByConfig' option.",
            typeElement.getSimpleName().toString());
          valid = false;
        } else {
          boolean found = false;
          String drivenByConfig = stageDef.outputStreamsDrivenByConfig();
          for(VariableElement v : getAllFields(typeElement).values()) {
            if(v.getSimpleName().toString().equals(drivenByConfig) && v.getAnnotation(ConfigDef.class) != null) {
              found = true;
            }
          }
          if(!found) {
            printError("stagedef.validation.outputStreamsDrivenByConfig.invalid",
              "The stage {} indicates a value '{}' for literal 'outputStreamsDrivenByConfig' but no configuration " +
                "option is found with that name.",
              typeElement.getSimpleName().toString(), drivenByConfig);
          }
        }
      } else {
        printError("stagedef.validation.variableOutputStreams.not.source.or.processor",
          "The stage {} identifies \"com.streamsets.pipeline.api.StageDef$VariableOutputStreams\" as the output " +
            "streams provider class but is not a source or processor.", typeElement.getSimpleName().toString());
        valid = false;
      }
    }
    return valid;
  }

  public int getOutputStreams(TypeElement typeElement, StageDef stageDefAnnotation) {
    String stageType = getStageTypeFromElement(typeElement);
    if(TARGET.equals(stageType)) {
      return 0;
    }
    if(SOURCE.equals(stageType) || PROCESSOR.equals(stageType)) {
      List<? extends Element> enclosedElements = getOutputStreamEnumsTypeElement(stageDefAnnotation)
        .getEnclosedElements();
      List<VariableElement> variableElements = ElementFilter.fieldsIn(enclosedElements);
      int cardinality = 0;
      for (VariableElement variableElement : variableElements) {
        if(variableElement.getKind() == ElementKind.ENUM_CONSTANT) {
          cardinality++;
        }
      }
      return cardinality;
    }
    return 1;
  }

  public String getOutputStreamLabelsProviderClass(StageDef stageDef) {
    //Not the best way of getting the TypeMirror of the ChooserValues implementation
    //Find a better solution
    TypeMirror outputStreamsEnum = null;
    try {
      stageDef.outputStreams();
    } catch (MirroredTypeException e) {
      outputStreamsEnum = e.getTypeMirror();
    }
    return getClassNameFromTypeMirror(outputStreamsEnum);
  }

  private TypeElement getOutputStreamEnumsTypeElement(StageDef stageDef) {
    //Not the best way of getting the TypeMirror of the ChooserValues implementation
    //Find a better solution
    TypeMirror outputStreamsEnum = null;
    try {
      stageDef.outputStreams();
    } catch (MirroredTypeException e) {
      outputStreamsEnum = e.getTypeMirror();
    }
    return getTypeElementFromMirror(outputStreamsEnum);
  }

  private boolean isVariableOutputStreams(StageDef stageDef) {
    String outputStreamsName = getOutputStreamLabelsProviderClass(stageDef);
    if(outputStreamsName.equals(VARIABLE_OUTPUT_STREAMS_CLASS)) {
      return true;
    }
    return false;
  }

  private boolean isDefaultOutputStream(StageDef stageDef) {
    String outputStreamsName = getOutputStreamLabelsProviderClass(stageDef);
    if(outputStreamsName.equals(DEFAULT_OUTPUT_STREAMS_CLASS)) {
      return true;
    }
    return false;
  }

}