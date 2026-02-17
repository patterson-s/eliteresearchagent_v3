#!/usr/bin/env python3
"""
Example Cohere API interaction script
Demonstrates how to connect to Cohere's API and send custom prompts
"""

import os
import json
from pathlib import Path
from dotenv import load_dotenv
import cohere

def load_config():
    """Load configuration from config.json"""
    config_path = Path(__file__).parent.parent / "config" / "config.json"
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def example_cohere_api():
    """Example of using Cohere API with custom prompts"""
    try:
        # Load environment variables
        env_path = Path(__file__).parent.parent / ".env"
        load_dotenv(env_path)
        
        # Get API key
        api_key = os.getenv("COHERE_API_KEY")
        if not api_key:
            raise ValueError("COHERE_API_KEY not found in .env file")
        
        print("=== Cohere API Example ===")
        print(f"Using Cohere ClientV2")
        print(f"Model: command-a-03-2025")
        print()
        
        # Initialize Cohere client V2
        co = cohere.ClientV2(api_key)
        
        while True:
            print("\nOptions:")
            print("1. Use predefined test prompt")
            print("2. Enter custom prompt")
            print("3. Exit")
            
            choice = input("\nEnter your choice (1-3): ").strip()
            
            if choice == "3":
                print("Exiting...")
                break
            elif choice == "2":
                # Custom prompt input
                print("\nEnter your custom prompt (press Enter twice to finish):")
                lines = []
                while True:
                    line = input()
                    if line == "":
                        break
                    lines.append(line)
                user_prompt = "\n".join(lines)
                
                if not user_prompt.strip():
                    print("No prompt entered. Please try again.")
                    continue
                    
            elif choice == "1":
                # Use predefined test prompt
                try:
                    config = load_config()
                    prompt_path = config["prompts"]["test_cohere_connection"]
                    full_path = Path(__file__).parent.parent / prompt_path
                    
                    with open(full_path, 'r', encoding='utf-8') as f:
                        user_prompt = f.read()
                    
                    print(f"\nUsing predefined prompt from: {prompt_path}")
                    print("Prompt content:")
                    print("-" * 50)
                    print(user_prompt)
                    print("-" * 50)
                    
                except Exception as e:
                    print(f"Could not load predefined prompt: {e}")
                    print("Falling back to simple test prompt...")
                    user_prompt = "Hello, what is your name?"
            else:
                print("Invalid choice. Please try again.")
                continue
            
            # Show what will be sent
            print(f"\n[DEBUG] Sending to Cohere API:")
            print(f"Model: command-a-03-2025")
            print(f"Max tokens: 100")
            print(f"Temperature: 0.1")
            print(f"Prompt length: {len(user_prompt)} characters")
            
            # Get confirmation
            confirm = input("\nSend this prompt? (y/n): ").strip().lower()
            if confirm != 'y':
                print("Cancelled.")
                continue
            
            # Send to Cohere API
            print("\n[SENDING] Connecting to Cohere API...")
            
            response = co.chat(
                model="command-a-03-2025",
                messages=[
                    {
                        "role": "user",
                        "content": user_prompt
                    }
                ],
                max_tokens=100,
                temperature=0.1
            )
            
            print("[SUCCESS] API call completed!")
            
            # Extract and display response
            if hasattr(response, 'message') and hasattr(response.message, 'content'):
                for content_item in response.message.content:
                    if hasattr(content_item, 'text'):
                        response_text = content_item.text
                        print("\n[RESPONSE]")
                        print("=" * 50)
                        print(response_text)
                        print("=" * 50)
                        break
            else:
                print(f"[RESPONSE] {str(response)}")
            
            # Show token usage if available
            if hasattr(response, 'meta') and hasattr(response.meta, 'billed_units'):
                print(f"\n[TOKEN USAGE] Input: {response.meta.billed_units.input_tokens} tokens, Output: {response.meta.billed_units.output_tokens} tokens")
            
    except Exception as e:
        print(f"\n[ERROR] Connection failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    example_cohere_api()