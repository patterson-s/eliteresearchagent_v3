#!/usr/bin/env python3
"""
Enhance Career Events with Timeline Data

Adds time_start and time_finish fields to career events by extracting years
from time_markers and supporting_quotes.
"""

import json
import re
from pathlib import Path
from typing import List, Dict, Any, Optional
import sys

def extract_years_from_text(text: str) -> List[int]:
    """Extract all 4-digit years from text."""
    # Find all 4-digit numbers (years)
    year_pattern = r'\b(19|20)\d{2}\b'
    years = []
    
    for match in re.finditer(year_pattern, text):
        try:
            year = int(match.group())
            if 1900 <= year <= 2050:  # Reasonable range for career events
                years.append(year)
        except ValueError:
            continue
    
    return sorted(list(set(years)))  # Remove duplicates and sort

def parse_time_marker(time_marker: str) -> Dict[str, Optional[int]]:
    """Parse time_marker field to extract start and end years."""
    result = {'start': None, 'end': None}
    
    if not time_marker:
        return result
    
    # Extract all years from the time marker
    years = extract_years_from_text(time_marker)
    
    if len(years) == 1:
        result['start'] = years[0]
        result['end'] = years[0]  # Single year event
    elif len(years) >= 2:
        result['start'] = years[0]
        result['end'] = years[-1]  # Range from first to last year
    
    # Handle common patterns
    if 'present' in time_marker.lower() or 'current' in time_marker.lower():
        result['end'] = None  # Ongoing position
    
    if '-' in time_marker or 'to' in time_marker.lower():
        # This is likely a range, we already handled it above
        pass
    
    return result

def extract_time_from_event(event: Dict[str, Any]) -> Dict[str, Optional[int]]:
    """Extract time_start and time_finish from a career event."""
    result = {'time_start': None, 'time_finish': None}
    
    # First try time_markers
    time_markers = event.get('time_markers', [])
    if isinstance(time_markers, list):
        for marker in time_markers:
            parsed = parse_time_marker(marker)
            if parsed['start'] is not None:
                result['time_start'] = parsed['start']
                result['time_finish'] = parsed['end']
                break
    elif isinstance(time_markers, str):
        parsed = parse_time_marker(time_markers)
        result['time_start'] = parsed['start']
        result['time_finish'] = parsed['end']
    
    # If no time from markers, try supporting_quotes
    if result['time_start'] is None and 'supporting_quotes' in event:
        quotes = event['supporting_quotes']
        if isinstance(quotes, list):
            for quote in quotes:
                years = extract_years_from_text(quote)
                if years:
                    result['time_start'] = years[0]
                    result['time_finish'] = years[-1] if len(years) > 1 else years[0]
                    break
        elif isinstance(quotes, str):
            years = extract_years_from_text(quotes)
            if years:
                result['time_start'] = years[0]
                result['time_finish'] = years[-1] if len(years) > 1 else years[0]
    
    # If still no time, try to find years in other text fields
    if result['time_start'] is None:
        for field in ['context', 'key_quote', 'supporting_quotes']:
            if field in event:
                text = event[field]
                if isinstance(text, str):
                    years = extract_years_from_text(text)
                    if years:
                        result['time_start'] = years[0]
                        result['time_finish'] = years[-1] if len(years) > 1 else years[0]
                        break
                elif isinstance(text, list):
                    for item in text:
                        years = extract_years_from_text(str(item))
                        if years:
                            result['time_start'] = years[0]
                            result['time_finish'] = years[-1] if len(years) > 1 else years[0]
                            break
    
    return result

def enhance_single_file(input_path: Path, output_path: Path) -> bool:
    """Enhance a single career events JSON file with timeline data."""
    try:
        # Load the original data
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Make a copy to avoid modifying original during iteration
        enhanced_data = json.loads(json.dumps(data))
        
        # Process each career event
        if 'career_events' in enhanced_data:
            for event in enhanced_data['career_events']:
                time_data = extract_time_from_event(event)
                
                # Only add time fields if we found valid data
                if time_data['time_start'] is not None:
                    event['time_start'] = time_data['time_start']
                if time_data['time_finish'] is not None:
                    event['time_finish'] = time_data['time_finish']
        
        # Save enhanced data
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(enhanced_data, f, indent=2, ensure_ascii=False)
        
        return True
        
    except Exception as e:
        print(f"Error enhancing {input_path}: {e}")
        return False

def enhance_all_files_in_directory(input_dir: Path, output_dir: Path = None) -> None:
    """Enhance all career events JSON files in a directory."""
    if output_dir is None:
        output_dir = input_dir / "enhanced"
    
    output_dir.mkdir(exist_ok=True)
    
    # Find all career events JSON files
    json_files = list(input_dir.rglob("*_career_events.json"))
    
    print(f"Found {len(json_files)} career events files to enhance")
    
    success_count = 0
    error_count = 0
    
    for i, json_file in enumerate(json_files, 1):
        print(f"Processing {i}/{len(json_files)}: {json_file.name}")
        
        # Create relative path in output directory
        relative_path = json_file.relative_to(input_dir)
        output_file = output_dir / relative_path
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        if enhance_single_file(json_file, output_file):
            success_count += 1
            print(f"  ✓ Enhanced: {json_file.name}")
        else:
            error_count += 1
            print(f"  ✗ Failed: {json_file.name}")
    
    print(f"\nEnhancement complete:")
    print(f"  Success: {success_count}")
    print(f"  Failed: {error_count}")
    print(f"  Results saved to: {output_dir}")

def enhance_specific_file(input_path: Path, output_path: Path = None) -> None:
    """Enhance a specific career events JSON file."""
    if output_path is None:
        output_path = input_path.with_stem(input_path.stem + "_enhanced")
    
    print(f"Enhancing {input_path.name}...")
    
    if enhance_single_file(input_path, output_path):
        print(f"✓ Successfully enhanced: {output_path}")
    else:
        print(f"✗ Failed to enhance: {input_path}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhance career events with timeline data')
    parser.add_argument('--file', type=Path, help='Enhance a specific JSON file')
    parser.add_argument('--directory', type=Path, help='Enhance all JSON files in a directory')
    parser.add_argument('--output', type=Path, help='Output directory (for directory mode)')
    
    args = parser.parse_args()
    
    if args.file:
        enhance_specific_file(args.file)
    elif args.directory:
        enhance_all_files_in_directory(args.directory, args.output)
    else:
        print("Usage:")
        print("  python enhance_timeline_data.py --file path/to/file.json")
        print("  python enhance_timeline_data.py --directory path/to/batch_outputs")
        print("  python enhance_timeline_data.py --directory path/to/input --output path/to/output")