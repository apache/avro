package org.apache.avro.mojo;

import org.apache.maven.plugin.logging.Log;
import org.apache.velocity.util.StringBuilderWriter;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class TestLog implements Log {

  private final List<String> logEntries = new ArrayList<>();

  public List<String> getLogEntries() {
    return logEntries;
  }

  private void log(String level, CharSequence content, Throwable error) {
    StringBuilder buffer = new StringBuilder();
    buffer.append('[').append(level).append("]");
    if (content != null) {
      buffer.append(' ').append(content);
    }
    if (error != null) {
      buffer.append(content == null ? " " : System.lineSeparator());
      error.printStackTrace(new PrintWriter(new StringBuilderWriter(buffer)));
    }
    logEntries.add(buffer.toString());
  }

  @Override
  public boolean isDebugEnabled() {
    return true;
  }

  @Override
  public void debug(CharSequence content) {
    debug(content, null);
  }

  @Override
  public void debug(CharSequence content, Throwable error) {
    log("DEBUG", content, error);
  }

  @Override
  public void debug(Throwable error) {
    debug(null, error);
  }

  @Override
  public boolean isInfoEnabled() {
    return true;
  }

  @Override
  public void info(CharSequence content) {
    info(content, null);
  }

  @Override
  public void info(CharSequence content, Throwable error) {
    log("INFO", content, error);
  }

  @Override
  public void info(Throwable error) {
    info(null, error);
  }

  @Override
  public boolean isWarnEnabled() {
    return true;
  }

  @Override
  public void warn(CharSequence content) {
    warn(content, null);
  }

  @Override
  public void warn(CharSequence content, Throwable error) {
    log("WARN", content, error);
  }

  @Override
  public void warn(Throwable error) {
    warn(null, error);
  }

  @Override
  public boolean isErrorEnabled() {
    return true;
  }

  @Override
  public void error(CharSequence content) {
    error(content, null);
  }

  @Override
  public void error(CharSequence content, Throwable error) {
    log("ERROR", content, error);
  }

  @Override
  public void error(Throwable error) {
    error(null, error);
  }
}
