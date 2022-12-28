package org.apache.avro.mojo.model;

import org.apache.avro.compiler.specific.model.NamespaceToPackageMapping;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Specifies which Java packagename should be used when generating Java code
 * representing types defined in a specific AVRO namespace.
 * <p>
 * Instances are created/populated by Maven while parsing a pom.xml file.
 * </p>
 */
public class MavenNamespaceToPackageMapping {
  private String namespacePrefix;
  private String packageNamePrefix;

  public String getNamespacePrefix() {
    return namespacePrefix;
  }

  public void setNamespacePrefix(String namespacePrefix) {
    this.namespacePrefix = namespacePrefix;
  }

  public String getPackageNamePrefix() {
    return packageNamePrefix;
  }

  public void setPackageNamePrefix(String packageNamePrefix) {
    this.packageNamePrefix = packageNamePrefix;
  }

  public void validate() {
    if (StringUtils.isEmpty(namespacePrefix)) {
      throw new IllegalArgumentException("namespacePrefix must not be null");
    }

    if (StringUtils.isEmpty(packageNamePrefix)) {
      throw new IllegalArgumentException("packageNamePrefix must not be null");
    }
  }

  /**
   * Convert the maven-configuration representation of namespace mappings into the
   * SpecificCompiler equivalent.
   */
  public static List<NamespaceToPackageMapping> toCompilerType(MavenNamespaceToPackageMapping[] configs) {
    if ((configs == null) || (configs.length == 0)) {
      return Collections.emptyList();
    }

    List<NamespaceToPackageMapping> mappings = new ArrayList<>(configs.length);
    for (int i = 0; i < configs.length; ++i) {
      MavenNamespaceToPackageMapping config = configs[i];
      config.validate();
      NamespaceToPackageMapping mapping = new NamespaceToPackageMapping(config.getNamespacePrefix(),
          config.getPackageNamePrefix());
      mappings.add(mapping);
    }

    return mappings;
  }
}
