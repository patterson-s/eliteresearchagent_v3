#!/usr/bin/env python3
"""
LLM-Based Timeline Extraction for Career Events

Uses Claude API to intelligently extract time_start and time_finish from career events.
"""

import json
import os
import re
from pathlib import Path
from typing import List, Dict, Any
import anthropic
from dotenv import load_dotenv

def construct_timeline_prompt(event: Dict[str, Any]) -> str:
    """Construct a prompt to extract timeline data from a single career event."""
    
    # Create a clean representation of the event
    event_str = json.dumps(event, indent=2, ensure_ascii=False)
    
    prompt = f"""Analyze the following career event and extract the most accurate time_start and time_finish years.

Career Event:
{event_str}

Guidelines:
1. time_start should be the year when the position/award started
2. time_finish should be the year when the position/award ended
3. If the position is ongoing/current, set time_finish to null
4. If only a single year is mentioned, set both time_start and time_finish to that year
5. If no specific years can be determined, set both to null
6. Use all available information: time_markers, supporting_quotes, context, etc.
7. Be conservative - if uncertain, prefer null over guessing

Provide your response as JSON only:
{{
  "time_start": <year_or_null>,
  "time_finish": <year_or_null>,
  "reasoning": "<brief_explanation>"
}}
"""
    
    return prompt

def extract_timeline_with_llm(event: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    """Use Claude API to extract timeline data from a career event."""
    
    try:
        client = anthropic.Anthropic(api_key=api_key)
        
        prompt = construct_timeline_prompt(event)
        
        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=1000,
            temperature=0.0,  # Deterministic output
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        )
        
        # Extract the JSON response
        response_text = response.content[0].text.strip()
        
        # Clean up the response
        if response_text.startswith('```json'):
            response_text = response_text[7:].strip()
        if response_text.endswith('```'):
            response_text = response_text[:-3].strip()
        
        # Parse the JSON
        timeline_data = json.loads(response_text)
        
        return {
            'time_start': timeline_data.get('time_start'),
            'time_finish': timeline_data.get('time_finish'),
            'reasoning': timeline_data.get('reasoning', 'LLM analysis')
        }
        
    except Exception as e:
        print(f"Error extracting timeline: {e}")
        return {
            'time_start': None,
            'time_finish': None,
            'reasoning': f"Error: {str(e)}"
        }

def enhance_file_with_llm(input_path: Path, output_path: Path, api_key: str) -> bool:
    """Enhance a career events file using LLM for timeline extraction."""
    
    try:
        # Load the original data
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Make a copy for enhancement
        enhanced_data = json.loads(json.dumps(data))
        
        # Process each career event with LLM
        if 'career_events' in enhanced_data:
            for i, event in enumerate(enhanced_data['career_events']):
                print(f"  Processing event {i+1}/{len(enhanced_data['career_events'])}")
                
                # Extract timeline data using LLM
                timeline = extract_timeline_with_llm(event, api_key)
                
                # Add the timeline data to the event
                event['time_start'] = timeline['time_start']
                event['time_finish'] = timeline['time_finish']
                
                # Store reasoning for debugging (optional)
                if 'metadata' not in event:
                    event['metadata'] = {}
                event['metadata']['timeline_reasoning'] = timeline['reasoning']
        
        # Save enhanced data
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(enhanced_data, f, indent=2, ensure_ascii=False)
        
        return True
        
    except Exception as e:
        print(f"Error enhancing file: {e}")
        return False

def enhance_directory_with_llm(input_dir: Path, output_dir: Path = None, api_key: str = None) -> None:
    """Enhance all career events files in a directory using LLM."""
    
    if output_dir is None:
        output_dir = input_dir / "llm_enhanced"
    
    output_dir.mkdir(exist_ok=True)
    
    # Get API key
    if api_key is None:
        env_path = Path(__file__).resolve().parent.parent.parent / ".env"
        load_dotenv(env_path, override=True)
        api_key = os.getenv('ANTHROPIC_API_KEY')
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable not set")
    
    # Find all career events JSON files
    json_files = list(input_dir.rglob("*_career_events.json"))
    
    print(f"Found {len(json_files)} career events files to enhance with LLM")
    print("This may take a while as each event requires an API call...\n")
    
    success_count = 0
    error_count = 0
    
    for i, json_file in enumerate(json_files, 1):
        print(f"Processing file {i}/{len(json_files)}: {json_file.name}")
        
        # Create relative path in output directory
        relative_path = json_file.relative_to(input_dir)
        output_file = output_dir / relative_path
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        if enhance_file_with_llm(json_file, output_file, api_key):
            success_count += 1
            print(f"  ✓ Enhanced: {json_file.name}")
        else:
            error_count += 1
            print(f"  ✗ Failed: {json_file.name}")
    
    print(f"\nLLM Enhancement Complete:")
    print(f"  Success: {success_count}")
    print(f"  Failed: {error_count}")
    print(f"  Results saved to: {output_dir}")

def enhance_specific_file_with_llm(input_path: Path, output_path: Path = None, api_key: str = None) -> None:
    """Enhance a specific career events file using LLM."""
    
    if output_path is None:
        output_path = input_path.with_stem(input_path.stem + "_llm_enhanced")
    
    # Get API key
    if api_key is None:
        env_path = Path(__file__).resolve().parent.parent.parent / ".env"
        load_dotenv(env_path, override=True)
        api_key = os.getenv('ANTHROPIC_API_KEY')
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable not set")
    
    print(f"Enhancing {input_path.name} with LLM...")
    
    if enhance_file_with_llm(input_path, output_path, api_key):
        print(f"✓ Successfully enhanced: {output_path}")
    else:
        print(f"✗ Failed to enhance: {input_path}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='LLM-based timeline extraction for career events')
    parser.add_argument('--file', type=Path, help='Enhance a specific JSON file')
    parser.add_argument('--directory', type=Path, help='Enhance all JSON files in a directory')
    parser.add_argument('--output', type=Path, help='Output directory (for directory mode)')
    
    args = parser.parse_args()
    
    if args.file:
        enhance_specific_file_with_llm(args.file)
    elif args.directory:
        enhance_directory_with_llm(args.directory, args.output)
    else:
        print("Usage:")
        print("  python extract_timeline_with_llm.py --file path/to/file.json")
        print("  python extract_timeline_with_llm.py --directory path/to/batch_outputs")
        print("  python extract_timeline_with_llm.py --directory path/to/input --output path/to/output")