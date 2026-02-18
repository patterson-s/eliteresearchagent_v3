# AppliedOntology_02

I have been working on a prosopographical research tool that requires a sequence of multiple tools. I am unconvinced that my current workflow is adequately completing the task.

# Career-events
We are studying career trajectories of international elites. In particular, we are interested in the geographic, organizational, and functional mobility. 

To understand trajectories, we are focused on collecting data about individual's "career-events" from unstructured text. A "career-event" is the atomic unit in someone's career. A career-event consists of: 

- organization: the organization where the career-event took place
- time: time descriptors of the career event
- function: the person's title or role 
- location: the location(s) of the career event. 

Career-events can be overlapping; for example, a diplomat who is posted in the United States could be both the representative to the UN AND the ambassador to the United States. These would be two career-events, taking place simultaneously. They would be in the same organization, could be in the same location, and at the same time, but refer to two different functions.  

# Challenge
- A primary problem is that organizations and roles are referenced in inconsistent ways in different texts. This makes our results messy and partially overlapping. 
- Another challenge is that the texts we are looking at are very different. Some are CVs that are densely packed with career-events. Others are not.

# Tools
I have been addressing this with two tools: 
- LLM tools that work with unstructured text, try to reduce redundancy and partial overlap, work with standardized output structures 
- An organizational ontology tool that helps to classify organizations for consistency. 

## Progress Log

### Step 1: Data Loader Migration (✓ Completed)
- **Date**: 2026-02-17
- **Action**: Migrated `load_data.py` from v2 to v3
- **Location**: `services/data_loader/load_data.py`
- **Status**: Successfully tested database connection and data loading
- **Functionality**: Can load career-event chunks from PostgreSQL database, filter by person, save to JSON files
- **Dependencies**: All required packages already in requirements.txt (psycopg2-binary, python-dotenv)
- **Next Steps**: Verify data quality and test integration with other tools

### Step 2: LLM Configuration Setup (✓ Completed)
- **Date**: 2026-02-17
- **Action**: Created config folder structure with LLM configuration
- **Location**: `config/` and `config/prompts/`
- **Status**: ✅ Successfully tested Cohere API connection with interactive verification
- **Files Created**:
  - `config/config.json` - LLM configuration with Cohere model settings
  - `config/prompts/test_cohere_connection.txt` - Test prompt template
  - `utils/example_cohere_api.py` - Interactive example script for transparent API testing
- **Dependencies**: Cohere package already in requirements.txt
- **API Notes**: Updated to use Cohere ClientV2 with chat() endpoint (generate() was deprecated)
- **Test Results**: 
  - ✅ Successfully connected to command-a-03-2025 model
  - ✅ Received valid JSON response matching expected format
  - ✅ Interactive script allows custom prompt input and full transparency
- **Features**:
  - Option to use predefined prompts or enter custom text
  - Shows exact API parameters before sending
  - Displays raw response with token usage
  - Requires explicit confirmation before API calls
  - Complete visibility into API interactions
- **Usage**: Run `python utils/example_cohere_api.py` to test with custom prompts
- **Next Steps**: Integrate LLM prompts with data loader for career-event extraction 

# NEXT TASK: 
For the next task, we want to use the model to identify all organizations that are mentioned in loaded from a chunk of text. There is a series of new prompts in the "config" folder: "C:\Users\spatt\Desktop\eliteresearchagent_v3\config\prompts\OrgExtraction_01.txt"; "C:\Users\spatt\Desktop\eliteresearchagent_v3\config\prompts\OrgExtraction_02.txt"; "C:\Users\spatt\Desktop\eliteresearchagent_v3\config\prompts\OrgExtraction_03.txt".

Design a service called OrgExtraction. I made a folder here: "C:\Users\spatt\Desktop\eliteresearchagent_v3\services\OrgExtraction"; the purpose of this should be to load data with the data_loader and then run these prompts on a chunk of text. The output from step 1 should be the input for step 2, and so on for step 3. 

Use the Cohere Command - A model