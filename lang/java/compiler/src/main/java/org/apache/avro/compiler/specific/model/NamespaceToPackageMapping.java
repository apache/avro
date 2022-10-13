package org.apache.avro.compiler.specific.model;

import org.apache.commons.lang3.StringUtils;

/**
 * Specifies which Java packagename should be used when generating Java code
 * representing types defined in a specific AVRO namespace.
 * <p>
 * Empty namespaces and empty packagenames are not accepted.
 * </p>
 */
public class NamespaceToPackageMapping {
  private final String namespacePrefix;
  private final String packageNamePrefix;
  private final int namespacePrefixLen;

  public NamespaceToPackageMapping(String namespacePrefix, String packageNamePrefix) {
    this.namespacePrefix = namespacePrefix;
    this.packageNamePrefix = packageNamePrefix;

    this.namespacePrefixLen = namespacePrefix.length();
  }

  /**
   * Return true if this mapping applies to the specified namespace.
   */
  public boolean matches(String namespace) {
    if (namespace == null) {
      return false;
    }

    if (!namespace.startsWith(namespacePrefix)) {
      return false;
    }

    if ((namespace.length() > namespacePrefixLen) && (namespace.charAt(namespacePrefixLen) != '.')) {
      return false;
    }

    return true;
  }

  /**
   * Return the Java packagename that should be used when generating code for
   * types defined in the specified AVRO namespace.
   * <p>
   * This method assumes that it is invoked only when matches() returns true.
   * </p>
   */
  public String map(String namespace) {
    if (namespace.length() == namespacePrefixLen) {
      return packageNamePrefix;
    }

    String namespaceSuffix = namespace.substring(namespacePrefixLen); // starts with "."
    return packageNamePrefix + namespaceSuffix;
  }

  public void validate() {
    if (StringUtils.isEmpty(namespacePrefix)) {
      throw new IllegalArgumentException("namespacePrefix must not be empty");
    }

    if (StringUtils.isEmpty(packageNamePrefix)) {
      throw new IllegalArgumentException("packageNamePrefix must not be empty");
    }
  }
}
