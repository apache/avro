/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file+ * to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance+ * with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.avro.test;

import org.testng.ISuite;
import org.testng.ISuiteListener;

public class TestSuiteInterceptor implements ISuiteListener {

    public void onStart(ISuite iSuite) {
    }

    public void onFinish(ISuite iSuite) {
        System.out.println("Suite " + iSuite.getName());
        System.out.print ("Total tests run:");
        for (String key : iSuite.getResults().keySet()) {
            System.out.print(" passed " + iSuite.getResults().get(key).getTestContext().getPassedTests().size());
            System.out.print("; failed " + iSuite.getResults().get(key).getTestContext().getFailedTests().size());
            System.out.print("; skipped " + iSuite.getResults().get(key).getTestContext().getSkippedTests().size());
            System.out.println(" in " + (iSuite.getResults().get(key).getTestContext().getEndDate().getSeconds() -
                    iSuite.getResults().get(key).getTestContext().getStartDate().getSeconds()) + " s.");
        }
        System.out.println();
    }
}
