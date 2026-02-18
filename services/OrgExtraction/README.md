# OrgExtraction Service

A 3-stage pipeline for extracting, filtering, and classifying organizations from biographical text using Cohere LLM.

## Overview

The OrgExtraction service processes text chunks through three sequential stages:

1. **Stage 1 (Extraction)**: Extract ALL organizations mentioned in text
2. **Stage 2 (Filtering)**: Filter to organizations where the person had active roles
3. **Stage 3 (Classification)**: Classify organizations by type (IO, NatGov, private_sector, NGO, other)

## Installation

The service uses the existing project dependencies. Ensure you have:

```bash
pip install -r requirements.txt
```

## Configuration

The service uses the main project configuration in `config/config.json` and the three prompts:
- `config/prompts/OrgExtraction_01.txt` - Extraction prompt
- `config/prompts/OrgExtraction_02.txt` - Filtering prompt  
- `config/prompts/OrgExtraction_03.txt` - Classification prompt

## Usage

### Command Line Interface

```bash
# List available people in database
python services/OrgExtraction/org_extraction.py --list-people

# Process a specific person
python services/OrgExtraction/org_extraction.py --person "John Doe"

# Process and save results to JSON
python services/OrgExtraction/org_extraction.py --person "John Doe" --output results/john_doe_orgs.json

# Limit processing to first N chunks
python services/OrgExtraction/org_extraction.py --person "John Doe" --limit 5
```

### Programmatic Usage

```python
from services.OrgExtraction.org_extraction import OrgExtractionService
from services.data_loader.load_data import load_chunks_from_db

# Initialize service
service = OrgExtractionService()

# Load data
chunks = load_chunks_from_db("John Doe")

# Process chunks
results = service.process_chunks(chunks)

# Save results
service.save_results(results, Path("results/john_doe_orgs.json"))
```

## Output Structure

Each result contains:

```json
{
  "person_name": "John Doe",
  "source_url": "https://example.com",
  "chunk_id": "chunk_001",
  "original_text": "...",
  "all_organizations": [...],      // Stage 1: All extracted organizations
  "employment_organizations": [...], // Stage 2: Organizations with active roles
  "classified_organizations": [...], // Stage 3: Classified organizations
  "processing_errors": [],
  "processing_time": 12.34,
  "timestamp": "2026-02-17T12:34:56.789"
}
```

## Pipeline Details

### Stage 1: Organization Extraction
- Extracts ALL organizations mentioned in text
- Includes universities, companies, governments, NGOs, etc.
- Preserves exact names and provides supporting quotes
- Does NOT filter or judge relevance

### Stage 2: Active Role Filtering
- Determines if person had active responsibilities at each organization
- INCLUDES: employment, elected positions, board memberships, advisory roles
- EXCLUDES: awards, honorary titles, passive mentions
- Provides reasoning and confidence scores

### Stage 3: Organization Classification
- Classifies organizations into 5 categories:
  - **IO**: International organizations (UN, WHO, World Bank)
  - **NatGov**: National government agencies
  - **private_sector**: Companies and corporations
  - **NGO**: Non-governmental organizations
  - **other**: Universities, research institutes, etc.
- Provides reasoning and confidence for each classification

## Error Handling

The service handles:
- API connection errors
- JSON parsing errors
- Missing data fields
- Partial processing failures

Errors are logged and stored in the `processing_errors` field of each result.

## Testing

Run the test suite:

```bash
python services/OrgExtraction/test_org_extraction.py
```

## Dependencies

- Python 3.8+
- `cohere` - Cohere API client
- `psycopg2-binary` - PostgreSQL database access
- `python-dotenv` - Environment variable management

All dependencies are included in the main project `requirements.txt`.