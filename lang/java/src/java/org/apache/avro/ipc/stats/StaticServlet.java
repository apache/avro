package org.apache.avro.ipc.stats;

import java.io.IOException;
import java.net.URL;

import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.resource.Resource;

/**
 * Very simple servlet class capable of serving static files.
 */
public class StaticServlet extends DefaultServlet {
  public Resource getResource(String pathInContext) {
    // Take only last slice of the URL as a filename, so we can adjust path. 
    // This also prevents mischief like '../../foo.css'
    String[] parts = pathInContext.split("/");
    String filename =  parts[parts.length - 1];

    try {
      URL resource = getClass().getClassLoader().getResource(
          "org/apache/avro/ipc/stats/static/" + filename);
      if (resource == null) { return null; }
      return Resource.newResource(resource);
    } catch (IOException e) {
      return null;
    }
  }
} 
