# nifi-azure-cognitive
NiFi Processors to interact with Azure Cognitive APIs

These processors are designed to enrich data with calls to the Azure cognitive APIs.

## AzureFaceProcessor - Face API 

This requires upload of the image to a URL before running if you use the Image URL property. A good pattern to achieve this would be to use the NiFi core PutAzureBlob processor, and use the attribute url returned from that to feed this processor.

Alternatively, just send the image as the body of the flow file and leave the image url property blank. 

# Testing
The tests for this processor require a valid API key for Azure Cognitive Services. You can provided this to the build with 
`mvn test -DFACE_API_KEY=<your-key>` sorry, you can't have mine, but Microsoft will give you one for free via the azure portal.
