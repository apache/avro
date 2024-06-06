/*
 * Copyright 2021 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.specific;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

public class TestSpecificData {

  @Test
  void separateThreadContextClassLoader() throws Exception {
    Schema schema = new Schema.Parser().parse(new File("src/test/resources/foo.Bar.avsc"));
    SpecificCompiler compiler = new SpecificCompiler(schema);
    compiler.setStringType(GenericData.StringType.String);
    compiler.compileToDestination(null, new File("target"));

    GenericRecord bar = new GenericData.Record(schema);
    bar.put("title", "hello");
    bar.put("created_at", 1630126246000L);

    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(bar, encoder);
    encoder.flush();
    byte[] data = out.toByteArray();

    JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
    StandardJavaFileManager fileManager = javac.getStandardFileManager(null, null, null);
    Iterable<? extends JavaFileObject> units = fileManager.getJavaFileObjects("target/foo/Bar.java");

    JavaCompiler.CompilationTask task1 = javac.getTask(null, fileManager, null, null, null, units);

    // FIXME: This part uses JavacTask which makes it depend on the tools.jar and
    // thus will only run in JDK 8
    // JavacTask jcTask = (JavacTask) task1;

    // Iterable<? extends Element> analyze = jcTask.analyze();

    // GeneratedCodeController ctrl = new GeneratedCodeController();
    // for (Element el : analyze) {
    // if (el.getKind() == ElementKind.CLASS) {
    // List<String> accept = el.accept(ctrl, 0);
    // assertTrue(accept.isEmpty(),
    // accept.stream().collect(Collectors.joining("\n\t")));
    // }
    // }

    javac.getTask(null, fileManager, null, null, null, units).call();
    fileManager.close();

    AtomicReference<Exception> ref = new AtomicReference<>();
    ClassLoader cl = new URLClassLoader(new URL[] { new File("target/").toURI().toURL() });
    Thread t = new Thread() {
      @Override
      public void run() {
        SpecificDatumReader<Object> reader = new SpecificDatumReader<>(schema);
        try {
          Object o = reader.read(null, DecoderFactory.get().binaryDecoder(data, null));
          System.out.println(o.getClass() + ": " + o);
        } catch (Exception ex) {
          ref.set(ex);
        }
      }
    };

    t.setContextClassLoader(cl);
    t.start();
    t.join();

    Exception ex = ref.get();
    if (ex != null) {
      ex.printStackTrace();
      fail(ex.getMessage());
    }
  }

  static class GeneratedCodeController implements ElementVisitor<List<String>, Integer> {

    @Override
    public List<String> visit(Element e, Integer integer) {
      return null;
    }

    @Override
    public List<String> visit(Element e) {
      return this.visit(e, 1);
    }

    @Override
    public List<String> visitPackage(PackageElement e, Integer integer) {
      return e.getEnclosedElements().stream().map((Element sub) -> sub.accept(this, 1)).filter(Objects::nonNull)
          .flatMap(List::stream).collect(Collectors.toList());
    }

    @Override
    public List<String> visitType(TypeElement e, Integer integer) {
      List<TypeMirror> interfaces = this.allInterfaces(e);

      List<Method> methods = interfaces.stream().filter((TypeMirror tm) -> tm.getKind() == TypeKind.DECLARED)
          .map(TypeMirror::toString).map((String typeName) -> {
            try {
              return Thread.currentThread().getContextClassLoader().loadClass(typeName);
            } catch (ClassNotFoundException ex) {
              return null;
            }
          }).filter(Objects::nonNull).map(Class::getMethods).flatMap(Arrays::stream)
          .filter((Method m) -> Modifier.isPublic(m.getModifiers()) && !Modifier.isStatic(m.getModifiers())
              && !Modifier.isFinal(m.getModifiers()) && m.getDeclaringClass() != Object.class)
          .collect(Collectors.toList());

      Stream<String> errors = e.getEnclosedElements().stream()
          .filter((Element el) -> el.getKind() == ElementKind.METHOD).map(ExecutableElement.class::cast)
          .filter((ExecutableElement declM) -> GeneratedCodeController.findFirst(declM, methods) != null)
          .filter((ExecutableElement declM) -> declM.getAnnotation(Override.class) == null)
          .map((ExecutableElement declM) -> "'" + declM.getReturnType().toString() + " " + declM.getSimpleName()
              + "(...)' method doesn't have @Override annotation");

      Stream<String> subError = e.getEnclosedElements().stream().map((Element sub) -> sub.accept(this, 1))
          .filter(Objects::nonNull).flatMap(List::stream);
      return Stream.concat(errors, subError).collect(Collectors.toList());
    }

    private List<TypeMirror> allInterfaces(TypeElement e) {
      List<TypeMirror> allInterfaces = new ArrayList<>(e.getInterfaces());

      TypeMirror superclass = e.getSuperclass();
      if (superclass != null && !Objects.equals(superclass.toString(), "java.lang.Object")) {
        allInterfaces.add(superclass);

        if (superclass.getKind() == TypeKind.DECLARED) {
          final Element element = ((DeclaredType) superclass).asElement();

          if (element instanceof TypeElement) {
            allInterfaces((TypeElement) element);
          }
        }
      }
      return allInterfaces;
    }

    private static Method findFirst(ExecutableElement ref, List<Method> methods) {
      return methods.stream().filter((Method m) -> GeneratedCodeController.areMethodSame(ref, m)).findFirst()
          .orElse(null);
    }

    private static boolean areMethodSame(ExecutableElement declaredMethod, Method interfaceMethod) {
      boolean res = Objects.equals(declaredMethod.getSimpleName().toString(), interfaceMethod.getName());
      if (!res) {
        return false;
      }

      TypeMirror type = declaredMethod.getReturnType();
      if (!type.toString().equals(interfaceMethod.getReturnType().getName())) {
        try {
          Class<?> declaredReturnedType = Thread.currentThread().getContextClassLoader().loadClass(type.toString());
          res &= interfaceMethod.getReturnType().isAssignableFrom(declaredReturnedType);
        } catch (ClassNotFoundException ex) {
          return false;
        }
      }
      List<? extends VariableElement> parameters = declaredMethod.getParameters();
      Class<?>[] parameterTypes = interfaceMethod.getParameterTypes();
      if (parameters.size() != parameterTypes.length) {
        return false;
      }
      for (int i = 0; i < parameterTypes.length; i++) {
        res &= areEquivalent(parameters.get(i), parameterTypes[i]);
      }
      return res;
    }

    private static boolean areEquivalent(VariableElement sourceParam, Class<?> typeParam) {
      // sourceParam.getSimpleName()
      // Type type = sourceParam.type;
      return Objects.equals(sourceParam.getSimpleName(), typeParam.getName());
    }

    @Override
    public List<String> visitVariable(VariableElement e, Integer integer) {
      return Collections.emptyList();
    }

    @Override
    public List<String> visitExecutable(ExecutableElement e, Integer integer) {
      return null;
    }

    @Override
    public List<String> visitTypeParameter(TypeParameterElement e, Integer integer) {
      return Collections.emptyList();
    }

    @Override
    public List<String> visitUnknown(Element e, Integer integer) {
      return Collections.emptyList();
    }
  }
}
