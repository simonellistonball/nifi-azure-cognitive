# nifi-azure-cognitive
NiFi Processors to interact with Azure Cognitive APIs

These processors are designed to enrich data with calls to the Azure cognitive APIs.

## AzureFaceProcessor - Face API 

This requires upload of the image to a URL before running. A good pattern to achieve this would be to use the NiFi core PutAzureBlob processor, and use the attribute url returned from that to feed this processor.
