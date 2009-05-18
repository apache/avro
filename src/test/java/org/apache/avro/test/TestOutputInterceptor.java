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

import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

public class TestOutputInterceptor extends TestListenerAdapter {

    static String format = "%1$-65s%2$8s ( %3$5s ms)\n";

    @Override
    public void onTestStart(ITestResult result) {
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        System.out.format(format, result.getMethod().getTestClass().getName() + "." + result.getMethod().getMethodName(), " Pass", (result.getEndMillis() - result.getStartMillis()));
    }

    @Override
    public void onTestFailure(ITestResult result) {
        System.out.format(format, result.getMethod().getTestClass().getName() + "." + result.getMethod().getMethodName(), " Fail", (result.getEndMillis() - result.getStartMillis()));
    }
}
