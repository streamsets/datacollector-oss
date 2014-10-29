The file "javax.annotation.processing.Processor" contains list of fully qualified names of annotation processors which must be run by the java compiler.
Ideally this file must be located in /META-INF/services.

However, having the file in that location causes issues during the compilation of the annotation processor itself and the mock stage definitions in this project which are used for testing.

As a workaround the "javax.annotation.processing.Processor" file is checked-in at this location.
It will be moved into the /META-INF/services directory after compiling the test classes using the maven-resource-plugin.