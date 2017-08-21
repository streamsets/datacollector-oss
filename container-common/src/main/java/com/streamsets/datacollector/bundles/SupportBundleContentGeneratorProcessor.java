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
package com.streamsets.datacollector.bundles;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.io.IOException;
import java.io.Writer;
import java.util.Set;

/**
 * Serialize all Content generator class as part of the compilation to a resource file that then can be easily read
 * and processed by the SupportBundleManager.
 */
@SupportedAnnotationTypes({"com.streamsets.datacollector.bundles.BundleContentGeneratorDef"})
public class SupportBundleContentGeneratorProcessor extends AbstractProcessor {

  public static final String RESOURCE_NAME = "support-bundle-list";

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    Set<? extends Element> elements = roundEnv.getElementsAnnotatedWith(BundleContentGeneratorDef.class);
    if(elements != null && elements.size() == 0) {
      return true;
    }

    // Write all the classes to the output file
    try {
      FileObject file = processingEnv.getFiler().createResource(StandardLocation.CLASS_OUTPUT, "", RESOURCE_NAME);
      try(Writer writer = file.openWriter()) {

        for (Element element : elements) {
          if (element.getKind() != ElementKind.CLASS) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "Skipping not a class object " + element.toString());
            continue;
          }

          writer.append(element.toString());
          writer.append("\n");
        }
      }
    } catch (IOException e) {
      processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "Can't generate descriptor " + e.getMessage());
      return false;
    }

    return true;
  }
}
