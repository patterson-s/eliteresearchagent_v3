#!/usr/bin/env python3
"""
OrgExtraction Service
Processes text chunks through a 3-stage pipeline to extract, filter, and classify organizations
"""

import os
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

import cohere
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class OrganizationExtraction:
    """Data class for organization extraction results"""
    original_text: str
    person_name: str
    source_url: str
    chunk_id: str
    
    # Stage 1: All organizations extracted
    all_organizations: List[Dict[str, Any]] = None
    
    # Stage 2: Organizations with active roles
    employment_organizations: List[Dict[str, Any]] = None
    
    # Stage 3: Classified organizations
    classified_organizations: List[Dict[str, Any]] = None
    
    # Processing metadata
    processing_errors: List[str] = None
    processing_time: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "person_name": self.person_name,
            "source_url": self.source_url,
            "chunk_id": self.chunk_id,
            "original_text": self.original_text,
            "all_organizations": self.all_organizations,
            "employment_organizations": self.employment_organizations,
            "classified_organizations": self.classified_organizations,
            "processing_errors": self.processing_errors,
            "processing_time": self.processing_time,
            "timestamp": datetime.now().isoformat()
        }

class OrgExtractionService:
    """Main service for organization extraction pipeline"""
    
    def __init__(self, config_path: Optional[Path] = None):
        """Initialize the service with configuration"""
        self.config_path = config_path or Path(__file__).parent.parent.parent / "config" / "config.json"
        self.config = self._load_config()
        self.prompts = self._load_prompts()
        self.cohere_client = self._initialize_cohere_client()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logger.info(f"Loaded configuration from {self.config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise
    
    def _load_prompts(self) -> Dict[str, str]:
        """Load all organization extraction prompts"""
        prompts_dir = Path(__file__).parent.parent.parent / "config" / "prompts"
        
        prompts = {}
        for i in range(1, 4):
            prompt_file = prompts_dir / f"OrgExtraction_{i:02d}.txt"
            if prompt_file.exists():
                with open(prompt_file, 'r', encoding='utf-8') as f:
                    prompts[f"stage_{i}"] = f.read()
                logger.info(f"Loaded prompt: {prompt_file.name}")
            else:
                logger.warning(f"Prompt file not found: {prompt_file}")
        
        return prompts
    
    def _initialize_cohere_client(self) -> cohere.ClientV2:
        """Initialize Cohere API client"""
        try:
            # Load environment variables
            env_path = Path(__file__).resolve().parent.parent.parent / ".env"
            load_dotenv(env_path)
            
            api_key = os.getenv("COHERE_API_KEY")
            if not api_key:
                raise ValueError("COHERE_API_KEY not found in .env file")
            
            client = cohere.ClientV2(api_key)
            logger.info("Initialized Cohere ClientV2")
            return client
            
        except Exception as e:
            logger.error(f"Failed to initialize Cohere client: {e}")
            raise
    
    def _call_cohere_api(self, prompt: str, max_tokens: int = 4000) -> Dict[str, Any]:
        """Call Cohere API with error handling"""
        try:
            # Log prompt details for debugging
            logger.debug(f"API call - prompt length: {len(prompt)} characters")
            logger.debug(f"API call - first 100 chars: {prompt[:100]}...")
            
            response = self.cohere_client.chat(
                model=self.config.get("model", "command-a-03-2025"),
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=max_tokens,
                temperature=self.config.get("temperature", 0.1)
            )
            
            # Extract response text
            if hasattr(response, 'message') and hasattr(response.message, 'content'):
                for content_item in response.message.content:
                    if hasattr(content_item, 'text'):
                        # Get token usage from response headers if available
                        input_tokens = 0
                        output_tokens = 0
                        if hasattr(response, 'response') and hasattr(response.response, 'headers'):
                            input_tokens = int(response.response.headers.get('num_tokens', '0'))
                        
                        return {
                            "response": content_item.text,
                            "success": True,
                            "tokens": {
                                "input": input_tokens,
                                "output": output_tokens
                            }
                        }
            
            return {
                "response": str(response),
                "success": True,
                "tokens": {
                    "input": 0,
                    "output": 0
                }
            }
            
        except Exception as e:
            logger.error(f"Cohere API call failed: {e}")
            # Extract more detailed error information if available
            error_str = str(e)
            if hasattr(e, 'status_code'):
                error_str += f" (HTTP {e.status_code})"
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                error_str += f" - {e.response.text[:200]}"
            
            return {
                "response": error_str,
                "success": False,
                "tokens": {
                    "input": 0,
                    "output": 0
                }
            }
    
    def _parse_json_response(self, response_text: str) -> Dict[str, Any]:
        """Parse JSON response with error handling"""
        try:
            # Clean response text - remove markdown formatting if present
            clean_text = response_text.strip()
            
            # Remove markdown code blocks if present
            if clean_text.startswith('```json'):
                clean_text = clean_text[7:].strip()  # Remove ```json
            elif clean_text.startswith('```'):
                clean_text = clean_text[3:].strip()  # Remove ```
            
            # Remove trailing ``` if present
            if clean_text.endswith('```'):
                clean_text = clean_text[:-3].strip()
            
            # Try to parse JSON
            return json.loads(clean_text)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            logger.error(f"Response text: {response_text[:500]}...")
            
            # Try to extract JSON from the response if it's embedded
            try:
                # Look for JSON patterns in the response
                json_match = None
                if '{"organizations":' in response_text:
                    # Try to extract organizations JSON
                    start = response_text.find('{"organizations":')
                    end = response_text.rfind('}') + 1
                    if start >= 0 and end > start:
                        json_str = response_text[start:end]
                        return json.loads(json_str)
                elif '{"employment_organizations":' in response_text:
                    # Try to extract employment organizations JSON
                    start = response_text.find('{"employment_organizations":')
                    end = response_text.rfind('}') + 1
                    if start >= 0 and end > start:
                        json_str = response_text[start:end]
                        return json.loads(json_str)
                elif '{"classified_organizations":' in response_text:
                    # Try to extract classified organizations JSON
                    start = response_text.find('{"classified_organizations":')
                    end = response_text.rfind('}') + 1
                    if start >= 0 and end > start:
                        json_str = response_text[start:end]
                        return json.loads(json_str)
            except Exception as extract_e:
                logger.error(f"Failed to extract JSON from response: {extract_e}")
            
            return {"error": f"JSON parse error: {e}"}
        except Exception as e:
            logger.error(f"Unexpected error parsing response: {e}")
            return {"error": str(e)}
    
    def _clean_text_for_processing(self, text: str) -> str:
        """Clean text for better LLM processing"""
        import re
        
        # Remove HTML navigation elements and boilerplate
        lines = text.split('\n')
        clean_lines = []
        
        for line in lines:
            line = line.strip()
            
            # Skip common navigation/boilerplate lines
            skip_patterns = [
                'Skip to content',
                'Home',
                'People',
                'Faculty',
                'Papers',
                'Courses',
                'Pieces in Popular Press',
                'Short Bio',
                '| MIT Economics',
                'ABHIJIT BANERJEE DOES NOT HAVE ANY SOCIAL MEDIA ACCOUNTS'
            ]
            
            if any(pattern in line for pattern in skip_patterns):
                continue
            
            # Skip lines that are mostly navigation
            if len(line) > 0 and not line.startswith('http') and not line.startswith('@'):
                clean_lines.append(line)
        
        # Join and clean up
        clean_text = ' '.join(clean_lines)
        
        # Remove multiple spaces
        clean_text = re.sub(r'\s+', ' ', clean_text)
        
        # Fix special characters
        clean_text = clean_text.replace('Æ', 'Ae')
        clean_text = clean_text.replace('æ', 'ae')
        
        return clean_text.strip()
    
    def _create_stage_1_prompt(self, text: str) -> str:
        """Create prompt for stage 1: Extract all organizations"""
        base_prompt = self.prompts.get("stage_1", "")
        return f"{base_prompt}\n\nTEXT:\n{text}"
    
    def _create_stage_2_prompt(self, person_name: str, organizations: List[Dict[str, Any]]) -> str:
        """Create prompt for stage 2: Filter active roles"""
        base_prompt = self.prompts.get("stage_2", "")
        
        # Format organizations for prompt
        orgs_text = json.dumps({"organizations": organizations}, indent=2)
        
        return f"{base_prompt}\n\nPERSON NAME: {person_name}\n\nORGANIZATIONS:\n{orgs_text}"
    
    def _create_stage_3_prompt(self, organizations: List[Dict[str, Any]]) -> str:
        """Create prompt for stage 3: Classify organizations"""
        base_prompt = self.prompts.get("stage_3", "")
        
        # Format organizations for prompt
        orgs_text = json.dumps({"employment_organizations": organizations}, indent=2)
        
        return f"{base_prompt}\n\nORGANIZATIONS:\n{orgs_text}"
    
    def process_chunk(self, chunk: Dict[str, Any]) -> OrganizationExtraction:
        """Process a single text chunk through the 3-stage pipeline"""
        import time
        
        start_time = time.time()
        
        # Extract basic info from chunk
        text = chunk.get("text", "")
        person_name = chunk.get("person_name", "unknown")
        source_url = chunk.get("source_url", "unknown")
        chunk_id = chunk.get("chunk_id", "unknown")
        
        # Clean the text for better processing
        clean_text = self._clean_text_for_processing(text)
        
        extraction = OrganizationExtraction(
            original_text=text,
            person_name=person_name,
            source_url=source_url,
            chunk_id=chunk_id,
            processing_errors=[]
        )
        
        logger.info(f"Processing chunk {chunk_id} for {person_name}")
        logger.debug(f"Text length: {len(text)} characters")
        logger.debug(f"Text preview: {text[:100]}...")
        
        try:
            # Stage 1: Extract all organizations
            logger.info("Stage 1: Extracting all organizations...")
            stage1_prompt = self._create_stage_1_prompt(clean_text)
            stage1_response = self._call_cohere_api(stage1_prompt)
            
            if not stage1_response["success"]:
                extraction.processing_errors.append(f"Stage 1 API error: {stage1_response['response']}")
                return extraction
            
            stage1_data = self._parse_json_response(stage1_response["response"])
            if "error" in stage1_data:
                extraction.processing_errors.append(f"Stage 1 parse error: {stage1_data['error']}")
                return extraction
            
            extraction.all_organizations = stage1_data.get("organizations", [])
            logger.info(f"Stage 1: Found {len(extraction.all_organizations)} organizations")
            
            # Stage 2: Filter to active roles
            if extraction.all_organizations:
                logger.info("Stage 2: Filtering active roles...")
                stage2_prompt = self._create_stage_2_prompt(person_name, extraction.all_organizations)
                stage2_response = self._call_cohere_api(stage2_prompt)
                
                if not stage2_response["success"]:
                    extraction.processing_errors.append(f"Stage 2 API error: {stage2_response['response']}")
                    return extraction
                
                stage2_data = self._parse_json_response(stage2_response["response"])
                if "error" in stage2_data:
                    extraction.processing_errors.append(f"Stage 2 parse error: {stage2_data['error']}")
                    return extraction
                
                extraction.employment_organizations = stage2_data.get("employment_organizations", [])
                logger.info(f"Stage 2: Found {len(extraction.employment_organizations)} organizations with active roles")
            else:
                extraction.employment_organizations = []
                logger.info("Stage 2: No organizations to filter")
            
            # Stage 3: Classify organizations
            if extraction.employment_organizations:
                logger.info("Stage 3: Classifying organizations...")
                stage3_prompt = self._create_stage_3_prompt(extraction.employment_organizations)
                stage3_response = self._call_cohere_api(stage3_prompt)
                
                if not stage3_response["success"]:
                    extraction.processing_errors.append(f"Stage 3 API error: {stage3_response['response']}")
                    return extraction
                
                stage3_data = self._parse_json_response(stage3_response["response"])
                if "error" in stage3_data:
                    extraction.processing_errors.append(f"Stage 3 parse error: {stage3_data['error']}")
                    return extraction
                
                extraction.classified_organizations = stage3_data.get("classified_organizations", [])
                logger.info(f"Stage 3: Classified {len(extraction.classified_organizations)} organizations")
            else:
                extraction.classified_organizations = []
                logger.info("Stage 3: No organizations to classify")
            
        except Exception as e:
            extraction.processing_errors.append(f"Processing error: {e}")
            logger.error(f"Error processing chunk {chunk_id}: {e}")
        
        extraction.processing_time = time.time() - start_time
        logger.info(f"Completed processing chunk {chunk_id} in {extraction.processing_time:.2f} seconds")
        
        return extraction
    
    def process_chunks(self, chunks: List[Dict[str, Any]]) -> List[OrganizationExtraction]:
        """Process multiple chunks"""
        results = []
        
        for i, chunk in enumerate(chunks, 1):
            logger.info(f"\nProcessing chunk {i}/{len(chunks)}")
            result = self.process_chunk(chunk)
            results.append(result)
        
        return results
    
    def save_results(self, results: List[OrganizationExtraction], output_path: Path) -> None:
        """Save extraction results to JSON file"""
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert results to dicts
            results_dict = [result.to_dict() for result in results]
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(results_dict, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved results to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to save results: {e}")
            raise

if __name__ == "__main__":
    import argparse
    import sys
    from pathlib import Path
    
    # Add project root to Python path
    project_root = Path(__file__).resolve().parent.parent.parent
    sys.path.append(str(project_root))
    
    from services.data_loader.load_data import load_chunks_from_db, get_all_people
    
    parser = argparse.ArgumentParser(description="OrgExtraction Service - Extract and classify organizations from text chunks")
    parser.add_argument("--person", help="Person name to process")
    parser.add_argument("--all", action="store_true", help="Process all people")
    parser.add_argument("--list-people", action="store_true", help="List all people in database")
    parser.add_argument("--output", type=Path, help="Output JSON file path")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of chunks to process")
    args = parser.parse_args()
    
    # Initialize service
    service = OrgExtractionService()
    
    if args.list_people:
        people = get_all_people()
        print(f"\nFound {len(people)} people in database:")
        for i, person in enumerate(people[:20], 1):
            print(f"  {i}. {person}")
        if len(people) > 20:
            print(f"  ... and {len(people) - 20} more")
    
    elif args.person:
        logger.info(f"Loading chunks for {args.person}...")
        chunks = load_chunks_from_db(args.person)
        
        if args.limit:
            chunks = chunks[:args.limit]
            logger.info(f"Limiting to {args.limit} chunks")
        
        logger.info(f"Found {len(chunks)} chunks for {args.person}")
        
        if chunks:
            results = service.process_chunks(chunks)
            
            # Save results if output path provided
            if args.output:
                service.save_results(results, args.output)
            else:
                # Print summary
                total_orgs = sum(len(r.all_organizations or []) for r in results)
                total_employment = sum(len(r.employment_organizations or []) for r in results)
                total_classified = sum(len(r.classified_organizations or []) for r in results)
                
                print(f"\n=== SUMMARY ===")
                print(f"Processed {len(results)} chunks")
                print(f"Total organizations extracted: {total_orgs}")
                print(f"Organizations with active roles: {total_employment}")
                print(f"Organizations classified: {total_classified}")
                print(f"Total processing time: {sum(r.processing_time for r in results):.2f} seconds")
        else:
            logger.info("No chunks found for this person")
    
    elif args.all:
        logger.info("Processing all people...")
        # This would be resource-intensive, so we'll implement it carefully
        print("Note: --all flag would process all people, which may be resource-intensive.")
        print("For now, please specify a person with --person NAME")
    
    else:
        print("Use --list-people to see available people")
        print("Use --person NAME to process a specific person")
        print("Use --output PATH to save results to JSON file")