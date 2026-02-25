# Prosopography Research
We are working on a prosopography research tool. We track the career trajectories of international elites in unstructured text data. 

We are primarily working on this service: "C:\Users\spatt\Desktop\eliteresearchagent_v3\services\targeted_01"

We are working to answer the questions in this document: "C:\Users\spatt\Desktop\eliteresearchagent_v3\services\targeted_01\template.md"

The current project status is here: "C:\Users\spatt\Desktop\eliteresearchagent_v3\services\targeted_01\status.md"

For information about how to access the raw data on international elites, consult the 'data_loader' service: "C:\Users\spatt\Desktop\eliteresearchagent_v3\services\data_loader"

## Context
We are building a RAG-based service to determine in which "High Level Panel" each of our international elites has served. We have done so for less than half of the people in our dataset so far. 

For each person, we have around 10 sources. These sources are chunked and embedded. You can see how to access the data in the 'data_loader' service. 

For each person, we want to do a RAG-query with re-ranking over these sources to find the name of the "High Level Panel" that they served on and when. Once we have found this information, we want to substantiate it in another source. For example, let's say that we do a RAG-query and the top 3 results are from wikipedia. If we find that the High Level Panel is discussed in a wikipedia source, we then want to switch to other sources to see if we can substantiate the claim. 

For our RAG searches, we only want to include sources from the person we are studying. We DO NOT want to include sources from other people.

## Relevant files

A previous education service illustrates how to use the Cohere Command-A model for RAG prompts and retrieval: 
- "C:\Users\spatt\Desktop\eliteresearchagent_v2\services\education"
- Config shows the Cohere models to use (allow for a higher token limit): "C:\Users\spatt\Desktop\eliteresearchagent_v2\services\education\config\config.json"

## Details
For the prompt, we want to allow some thinking, followed by structured JSON output. 

If we can verify the claims in two independent sources, stop. 

Include a numeric for the number of independent sources. 


