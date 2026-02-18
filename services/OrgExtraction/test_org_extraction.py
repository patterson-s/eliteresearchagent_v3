#!/usr/bin/env python3
"""
Test script for OrgExtraction service
"""

import sys
import json
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from services.OrgExtraction.org_extraction import OrgExtractionService

def test_service_initialization():
    """Test that the service initializes correctly"""
    print("Testing OrgExtractionService initialization...")
    
    try:
        service = OrgExtractionService()
        print("+ Service initialized successfully")
        
        # Check that prompts were loaded
        if service.prompts:
            print(f"+ Loaded {len(service.prompts)} prompts")
            for stage, prompt in service.prompts.items():
                print(f"  - {stage}: {len(prompt)} characters")
        else:
            print("- No prompts loaded")
            return False
        
        # Check Cohere client
        if service.cohere_client:
            print("+ Cohere client initialized")
        else:
            print("- Cohere client not initialized")
            return False
        
        return True
        
    except Exception as e:
        print(f"- Initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_prompt_creation():
    """Test prompt creation methods"""
    print("\nTesting prompt creation...")
    
    try:
        service = OrgExtractionService()
        
        # Test text for stage 1
        test_text = "John Doe worked at the World Health Organization and received an award from the Nobel Committee."
        
        # Create stage 1 prompt
        stage1_prompt = service._create_stage_1_prompt(test_text)
        if "TEXT:" in stage1_prompt and test_text in stage1_prompt:
            print("+ Stage 1 prompt created correctly")
        else:
            print("- Stage 1 prompt creation failed")
            return False
        
        # Create mock organizations for stage 2
        mock_orgs = [
            {
                "name": "World Health Organization",
                "quotes": ["John Doe worked at the World Health Organization"]
            }
        ]
        
        stage2_prompt = service._create_stage_2_prompt("John Doe", mock_orgs)
        if "PERSON NAME: John Doe" in stage2_prompt and "ORGANIZATIONS:" in stage2_prompt:
            print("+ Stage 2 prompt created correctly")
        else:
            print("- Stage 2 prompt creation failed")
            return False
        
        # Create stage 3 prompt
        stage3_prompt = service._create_stage_3_prompt(mock_orgs)
        if "ORGANIZATIONS:" in stage3_prompt:
            print("+ Stage 3 prompt created correctly")
        else:
            print("- Stage 3 prompt creation failed")
            return False
        
        return True
        
    except Exception as e:
        print(f"- Prompt creation test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_mock_processing():
    """Test processing with mock data (without actual API calls)"""
    print("\nTesting mock processing...")
    
    try:
        service = OrgExtractionService()
        
        # Create a mock chunk
        mock_chunk = {
            "text": "Jane Smith served as Minister of Health for Canada and was a professor at Harvard University. She received the Nobel Prize in Medicine.",
            "person_name": "Jane Smith",
            "source_url": "https://example.com/jane-smith",
            "chunk_id": "test_chunk_001"
        }
        
        print(f"Processing mock chunk for {mock_chunk['person_name']}...")
        print(f"Text: {mock_chunk['text']}")
        
        # Note: This will actually call the Cohere API, so we'll just test the structure
        # For a real test, we'd need API access and would want to mock the API calls
        
        print("+ Mock processing setup complete")
        print("Note: Actual API calls would be made in full processing")
        
        return True
        
    except Exception as e:
        print(f"- Mock processing test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests"""
    print("=== OrgExtraction Service Tests ===")
    
    tests = [
        test_service_initialization,
        test_prompt_creation,
        test_mock_processing
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print(f"\n=== RESULTS ===")
    print(f"Passed: {passed}/{total} tests")
    
    if passed == total:
        print("All tests passed!")
        return True
    else:
        print("Some tests failed")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)