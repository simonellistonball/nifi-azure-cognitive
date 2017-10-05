/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.simonellistonball.nifi.processors.azure.cognitive;

import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class AzureFaceProcessorTest {

	private TestRunner testRunner;

	@Before
	public void init() {
		testRunner = TestRunners.newTestRunner(AzureFaceProcessor.class);

	}

	@Test
	public void testProcessor() {
		testRunner.enqueue(getClass().getClassLoader().getResourceAsStream("face.jpg"));
		testRunner.setProperty(AzureFaceProcessor.REGION, "westus");
		testRunner.setProperty(AzureFaceProcessor.API_KEY, "9bba399f484548e9a11c03f3c5ce28c0");

		testRunner.run();

		testRunner.assertAllFlowFilesTransferred(AzureFaceProcessor.REL_MATCHED);

		List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(AzureFaceProcessor.REL_MATCHED);
		for (MockFlowFile flowFile : flowFiles) {
			flowFile.assertAttributeEquals("exposure", "goodExposure");
			flowFile.assertAttributeEquals("emotion", "neutral");
		}
	}

}
