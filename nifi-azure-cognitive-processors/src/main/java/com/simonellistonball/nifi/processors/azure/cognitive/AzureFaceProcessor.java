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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

@Tags({ "face", "cognitive", "azure", "image" })
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = AzureFaceProcessor.CONTENT_KEY, description = "JSON response") })
public class AzureFaceProcessor extends AbstractProcessor {

	protected static final String CONTENT_KEY = "cognitive.face.content";
	private static final String ATTRIBUTES_TO_FETCH = "age,gender,headPose,smile,facialHair,glasses,emotion,hair,makeup,occlusion,accessories,blur,exposure,noise";

	public static final String uriBase = "https://%s.api.cognitive.microsoft.com/face/v1.0/detect";
	public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
	public static final String DESTINATION_CONTENT = "flowfile-content";

	public static final PropertyDescriptor IMAGE_URL = new PropertyDescriptor.Builder().name("IMAGE_URL")
			.displayName("Image URL")
			.description(
					"A URL for the image to be sent to the face detector, if not specified the image will be taken from the content of the flowfile")
			.required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.addValidator(StandardValidators.URL_VALIDATOR).expressionLanguageSupported(true).build();

	public static final PropertyDescriptor API_KEY = new PropertyDescriptor.Builder().name("API_KEY")
			.displayName("Face API Key").description("Subscription key for the Face API").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).sensitive(true).build();

	public static final PropertyDescriptor REGION = new PropertyDescriptor.Builder().name("REGION")
			.displayName("Azure Region").description("The region dns prefix for you cognitive services instance")
			.required(true).allowableValues("westus", "westus2", "eastus", "eastus2", "westcentralus", "southcentralus",
					"westeurope", "northeurope", "southeastasia", "eastasia", "australiaeast", "brazilsouth")
			.build();

	public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder().name("DESTINATION")
			.displayName("Destination")
			.description("Store the results as an attribute, or as the content of the output flowfile").required(true)
			.allowableValues(DESTINATION_CONTENT, DESTINATION_ATTRIBUTE).defaultValue(DESTINATION_ATTRIBUTE).build();

	public static final Relationship REL_MATCHED = new Relationship.Builder().name("REL_MATCHED")
			.description("Successfully found face").build();

	public static final Relationship REL_UNMATCHED = new Relationship.Builder().name("REL_UNMATCHED")
			.description("Successfully found no faces").build();

	public static final Relationship REL_FAILURE = new Relationship.Builder().name("REL_FAILURE")
			.description("Failed to process image").build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	private String serviceUrl;

	private String subscriptionKey;
	private boolean asContent = false;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(IMAGE_URL);
		descriptors.add(API_KEY);
		descriptors.add(REGION);
		descriptors.add(DESTINATION);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_MATCHED);
		relationships.add(REL_UNMATCHED);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {
		serviceUrl = String.format(uriBase, context.getProperty(REGION).getValue());
		subscriptionKey = context.getProperty(API_KEY).getValue();
		asContent = context.getProperty(DESTINATION).getValue() == DESTINATION_CONTENT;
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}
		String imageUrl = context.getProperty(IMAGE_URL).evaluateAttributeExpressions(flowFile).getValue();
		HttpClient httpclient = HttpClientBuilder.create().build();

		try {
			HttpPost request = null;
			if (imageUrl == null) {
				request = recognizeFace(session.read(flowFile));
			} else {
				request = recognizeFace(imageUrl);
			}

			if (request != null) {
				Map<String, String> recognizeFace;
				try {
					recognizeFace = processRequest(httpclient, request);

					if (recognizeFace != null) {
						if (asContent) {
							flowFile = session.write(flowFile, new OutputStreamCallback() {
								@Override
								public void process(OutputStream out) throws IOException {
									out.write(recognizeFace.get(CONTENT_KEY).getBytes("UTF-8"));
									recognizeFace.remove(CONTENT_KEY);
								}
							});
						}
						flowFile = session.putAllAttributes(flowFile, recognizeFace);
						session.transfer(flowFile, REL_MATCHED);
					} else {
						session.transfer(flowFile, REL_UNMATCHED);
					}
				} catch (IOException e) {
					session.transfer(flowFile, REL_FAILURE);
				}
			}
		} catch (Exception e) {
			throw new ProcessException(e);
		}
	}

	private URI getUrl() throws URISyntaxException {
		URIBuilder builder = new URIBuilder(serviceUrl);

		builder.setParameter("returnFaceId", "true");
		builder.setParameter("returnFaceLandmarks", "true");
		builder.setParameter("returnFaceAttributes", ATTRIBUTES_TO_FETCH);

		// Prepare the URI for the REST API call.
		return builder.build();

	}

	private HttpPost recognizeFace(String imageUrl) throws URISyntaxException, ParseException, IOException {
		HttpPost request = new HttpPost(getUrl());
		request.setHeader("Ocp-Apim-Subscription-Key", subscriptionKey);

		request.setHeader("Content-Type", "application/json");
		StringEntity reqEntity = new StringEntity(String.format("{\"url\":\"%s\"}", imageUrl));
		request.setEntity(reqEntity);

		return request;
	}

	private HttpPost recognizeFace(InputStream image) throws URISyntaxException, ParseException, IOException {
		HttpPost request = new HttpPost(getUrl());
		request.setHeader("Ocp-Apim-Subscription-Key", subscriptionKey);

		request.setHeader("Content-Type", "application/octet-stream");
		InputStreamEntity reqEntity = new InputStreamEntity(image);
		request.setEntity(reqEntity);

		return request;
	}

	private Map<String, String> processRequest(HttpClient httpclient, HttpPost request)
			throws ClientProtocolException, IOException, ProcessException {
		HttpResponse response = httpclient.execute(request);
		HttpEntity entity = response.getEntity();
		if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
			throw new ProcessException(
					String.format("Error from Face recognition API: %s", response.getStatusLine().getReasonPhrase()));
		}
		if (entity != null) {
			String jsonString = EntityUtils.toString(entity).trim();
			Map<String, String> r = new HashMap<String, String>() {
				private static final long serialVersionUID = 1L;
				{
					put(CONTENT_KEY, jsonString);
				}
			};
			if (!asContent) {
				if (jsonString.charAt(0) == '[') {
					r.putAll(processMultipleFaces(jsonString));
				} else if (jsonString.charAt(0) == '{') {
					// TODO - this is most likely an error
				}
			}
			return r;
		}
		return null;
	}

	/**
	 * Take the response string and translate into attributes
	 * 
	 * Picks out key attributes people are likely to want to route on, other
	 * attributes are maintained in content should further processing be needed
	 * 
	 * @param jsonString
	 * @return
	 * @throws IOException
	 * @throws JsonProcessingException
	 */
	private Map<String, String> processMultipleFaces(String jsonString) throws JsonProcessingException, IOException {
		Map<String, String> result = new HashMap<String, String>();

		final ObjectMapper mapper = new ObjectMapper();
		JsonNode tree = mapper.readTree(jsonString);
		final ArrayNode arrayNode;
		if (tree.isArray()) {
			arrayNode = (ArrayNode) tree;
			for (JsonNode node : arrayNode) {
				result.put("faceId", node.findValue("faceId").asText());
				JsonNode faceAttributes = node.get("faceAttributes");
				// find the top emotion
				String emotion = null;
				double maxScore = 0;
				Iterator<Entry<String, JsonNode>> emotions = faceAttributes.get("emotion").fields();
				while (emotions.hasNext()) {
					Entry<String, JsonNode> next = emotions.next();
					double score = next.getValue().asDouble();
					if (score > maxScore) {
						maxScore = score;
						emotion = next.getKey();
					}
				}
				if (emotion != null) {
					result.put("emotion", emotion);
				}
				// quality attributes
				result.put("exposure", faceAttributes.get("exposure").findValue("exposureLevel").asText());
				result.put("blur", faceAttributes.get("blur").findValue("blurLevel").asText());
				result.put("noise", faceAttributes.get("noise").findValue("noiseLevel").asText());

			}
		}

		return result;
	}
}
