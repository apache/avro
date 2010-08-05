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
package org.apache.avro.ipc.trace;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class TraceClientServlet extends HttpServlet {
  public void doPost(HttpServletRequest request, 
    HttpServletResponse response)
    throws ServletException, IOException {
    response.setContentType("text/html");
    PrintWriter out = response.getWriter();

    out.println("<title>Example</title> <body bgcolor=FFFFFF>");
    out.println("<h2>Button Clicked</h2>");

    String servers = request.getParameter("servers");

    if(servers != null){
      String splitToken = System.getProperty("line.separator");
      if (splitToken == null) {
        splitToken = "\n";
      }
      String[] parts = servers.split(splitToken);
      for (String p : parts) {
        out.println(p + "<br>");
      }
    } else {
       out.println("No text entered.");
    }
  }
  
  public void doGet(HttpServletRequest request, 
      HttpServletResponse response) throws IOException {
    response.setContentType("text/html");
    PrintWriter out = response.getWriter();
    
    out.println("<html>");
    out.println("<head>Form</head>");
    out.println("<body><form method='post'>");
    out.println("<textarea name='servers'></textarea>");
    out.println("<input type='submit' name='submit'>");
    out.println("</form>");
    out.println("</html>");
  }
}
