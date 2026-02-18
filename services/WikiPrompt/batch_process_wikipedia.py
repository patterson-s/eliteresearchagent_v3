#!/usr/bin/env python3
"""
Batch Wikipedia Career Event Extraction

Processes all people in the database to extract career events from their Wikipedia pages.
"""

import json
import os
import sys
from pathlib import Path
import subprocess
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def get_all_people_from_db() -> List[str]:
    """Get list of all people from the database using the existing data_loader."""
    print("Loading list of people from database...")
    
    # Add the data_loader directory to Python path so we can import the functions
    data_loader_dir = Path(__file__).resolve().parent.parent / "data_loader"
    sys.path.append(str(data_loader_dir))
    
    try:
        # Import the function directly
        from load_data import get_all_people
        
        # Get all people from database
        people = get_all_people()
        
        print(f"Found {len(people)} people in database")
        return people
    except Exception as e:
        print(f"Failed to load people list: {e}")
        return []

def extract_wikipedia_content(person_name: str) -> str:
    """Extract Wikipedia content for a person from the database."""
    print(f"Extracting Wikipedia content for {person_name}...")
    
    # Use the existing data_loader to get chunks for this person
    data_loader_path = Path(__file__).resolve().parent.parent / "data_loader" / "load_data.py"
    output_file = Path(__file__).resolve().parent / f"temp_{person_name}_chunks.json"
    
    try:
        result = subprocess.run([
            sys.executable, str(data_loader_path), "--person", person_name, "--save", str(output_file)
        ], capture_output=True, text=True, cwd=data_loader_path.parent)
        
        if result.returncode == 0:
            # Extract Wikipedia chunks
            with open(output_file, 'r', encoding='utf-8') as f:
                chunks = json.load(f)
            
            # Filter for Wikipedia chunks
            wikipedia_chunks = [
                chunk for chunk in chunks 
                if 'wikipedia' in chunk.get('source_url', '').lower()
            ]
            
            if wikipedia_chunks:
                # Sort by chunk_index and combine
                wikipedia_chunks.sort(key=lambda x: x.get('chunk_index', 0))
                full_text = '\n\n'.join(chunk['text'] for chunk in wikipedia_chunks)
                
                # Clean up temp file
                output_file.unlink()
                
                return full_text
            else:
                print(f"No Wikipedia content found for {person_name}")
                output_file.unlink()
                return ""
        else:
            print(f"Error extracting content for {person_name}: {result.stderr}")
            if output_file.exists():
                output_file.unlink()
            return ""
    except Exception as e:
        print(f"Failed to extract content for {person_name}: {e}")
        if output_file.exists():
            output_file.unlink()
        return ""

def run_extraction(person_name: str, input_file: Path, output_file: Path) -> bool:
    """Run the career event extraction for a single person."""
    print(f"Running extraction for {person_name}...")
    
    extraction_script = Path(__file__).resolve().parent / "run_extraction.py"
    
    try:
        result = subprocess.run([
            sys.executable, str(extraction_script), 
            "--input", str(input_file),
            "--output", str(output_file)
        ], capture_output=True, text=True, cwd=Path(__file__).resolve().parent)
        
        if result.returncode == 0:
            print(f"✓ Successfully processed {person_name}")
            return True
        else:
            print(f"✗ Failed to process {person_name}: {result.stderr}")
            return False
    except Exception as e:
        print(f"✗ Error processing {person_name}: {e}")
        return False

def process_single_person(person_name: str, output_dir: Path) -> Dict[str, Any]:
    """Process a single person and return the result."""
    result = {
        'person': person_name,
        'status': 'unknown',
        'message': ''
    }
    
    try:
        # Extract Wikipedia content
        wikipedia_content = extract_wikipedia_content(person_name)
        
        if not wikipedia_content:
            result['status'] = 'skipped'
            result['message'] = 'No Wikipedia content'
            return result
        
        # Create individual folder for this person
        person_dir = output_dir / person_name.replace(" ", "_")
        person_dir.mkdir(exist_ok=True)
        
        # Save Wikipedia content
        input_file = person_dir / f"{person_name}_wikipedia.txt"
        with open(input_file, 'w', encoding='utf-8') as f:
            f.write(wikipedia_content)
        
        # Run extraction
        output_file = person_dir / f"{person_name}_career_events.json"
        
        if run_extraction(person_name, input_file, output_file):
            result['status'] = 'success'
            result['message'] = 'Processed successfully'
        else:
            result['status'] = 'failed'
            result['message'] = 'Extraction failed'
            
    except Exception as e:
        result['status'] = 'failed'
        result['message'] = f'Error: {str(e)}'
    
    return result

def batch_process_all_people(parallel_workers: int = 5):
    """Process all people in the database with parallel processing."""
    print("=== Batch Wikipedia Career Event Extraction ===")
    print(f"Using {parallel_workers} parallel workers...")
    print("This may take a while depending on the number of people...\n")
    
    # Get all people
    people = get_all_people_from_db()
    if not people:
        print("No people found in database. Exiting.")
        return
    
    # Create output directory
    output_dir = Path(__file__).resolve().parent / "batch_outputs"
    output_dir.mkdir(exist_ok=True)
    
    # Process people in parallel
    start_time = time.time()
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        # Submit all tasks
        future_to_person = {
            executor.submit(process_single_person, person_name, output_dir): person_name
            for person_name in people
        }
        
        # Process results as they complete
        success_count = 0
        skip_count = 0
        error_count = 0
        
        for i, future in enumerate(as_completed(future_to_person), 1):
            person_name = future_to_person[future]
            
            try:
                result = future.result()
                
                if result['status'] == 'success':
                    success_count += 1
                    print(f"✓ {i}/{len(people)}: {person_name} - Success")
                elif result['status'] == 'skipped':
                    skip_count += 1
                    print(f"○ {i}/{len(people)}: {person_name} - Skipped ({result['message']})")
                else:
                    error_count += 1
                    print(f"✗ {i}/{len(people)}: {person_name} - Failed ({result['message']})")
                    
            except Exception as e:
                error_count += 1
                print(f"✗ {i}/{len(people)}: {person_name} - Error: {str(e)}")
    
    # Calculate duration
    duration = time.time() - start_time
    
    # Summary
    print(f"\n=== Batch Processing Complete ===")
    print(f"Total people: {len(people)}")
    print(f"Successfully processed: {success_count}")
    print(f"Skipped (no Wikipedia): {skip_count}")
    print(f"Failed: {error_count}")
    print(f"Time taken: {duration:.1f} seconds")
    print(f"Results saved to: {output_dir}")

def process_specific_people(people_list: List[str], parallel_workers: int = 5):
    """Process a specific list of people with parallel processing."""
    print(f"=== Processing {len(people_list)} Specific People ===")
    print(f"Using {parallel_workers} parallel workers...\n")
    
    # Create output directory
    output_dir = Path(__file__).resolve().parent / "batch_outputs"
    output_dir.mkdir(exist_ok=True)
    
    # Process people in parallel
    start_time = time.time()
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        # Submit all tasks
        future_to_person = {
            executor.submit(process_single_person, person_name, output_dir): person_name
            for person_name in people_list
        }
        
        # Process results as they complete
        success_count = 0
        skip_count = 0
        error_count = 0
        
        for i, future in enumerate(as_completed(future_to_person), 1):
            person_name = future_to_person[future]
            
            try:
                result = future.result()
                
                if result['status'] == 'success':
                    success_count += 1
                    print(f"✓ {i}/{len(people_list)}: {person_name} - Success")
                elif result['status'] == 'skipped':
                    skip_count += 1
                    print(f"○ {i}/{len(people_list)}: {person_name} - Skipped ({result['message']})")
                else:
                    error_count += 1
                    print(f"✗ {i}/{len(people_list)}: {person_name} - Failed ({result['message']})")
                    
            except Exception as e:
                error_count += 1
                print(f"✗ {i}/{len(people_list)}: {person_name} - Error: {str(e)}")
    
    # Calculate duration
    duration = time.time() - start_time
    
    # Summary
    print(f"\n=== Processing Complete ===")
    print(f"Total people: {len(people_list)}")
    print(f"Successfully processed: {success_count}")
    print(f"Skipped (no Wikipedia): {skip_count}")
    print(f"Failed: {error_count}")
    print(f"Time taken: {duration:.1f} seconds")
    print(f"Results saved to: {output_dir}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Batch process Wikipedia career event extraction')
    parser.add_argument('--all', action='store_true', help='Process all people in database')
    parser.add_argument('--people', nargs='+', help='Process specific people (space-separated list)')
    parser.add_argument('--workers', type=int, default=5, help='Number of parallel workers (default: 5)')
    args = parser.parse_args()
    
    if args.all:
        batch_process_all_people(args.workers)
    elif args.people:
        process_specific_people(args.people, args.workers)
    else:
        print("Usage:")
        print("  python batch_process_wikipedia.py --all --workers 5          # Process all people with 5 workers")
        print("  python batch_process_wikipedia.py --people A B C --workers 3  # Process specific people with 3 workers")