#!/usr/bin/env python3
"""
Wikipedia Career Event Extraction Tool

Uses Claude API to extract structured career events from biographical text
using a few-shot learning approach.
"""

import argparse
import json
import os
import re
from pathlib import Path
import anthropic
from dotenv import load_dotenv

def load_file_content(file_path):
    """Load content from a file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

def construct_prompt(raw_text, v1_content, v2_content):
    """Construct the few-shot prompt following the template structure."""
    
    prompt_template = """[USER_BEGIN] I developed an LLM-based tool that can extract career-events from unstructured text data. 

Read the contents of this website, then review the attached file. 

Evaluate the quality of the extractions. 

RAW INPUT:
{raw_input}

EXTRACTED OUTPUT:
{v1_output}

[USER_END]

[CHAT_BEGIN]
Analyse terminée. Voici mon évaluation systématique de vos extractions:

Problèmes structurels majeurs
1. Confusion temporelle critique
L'événement Sloan Fellowship (1994) est dupliqué avec des rôles incorrects:

"fellow of the Econometric Society"
"member of the National Academy of Sciences"
"fellow of the American Academy of Arts and Sciences"

Ces trois sont des positions distinctes NON liées à la Sloan Fellowship de 1994. Vos extractions mélangent l'award de 1994 avec des affiliations institutionnelles séparées.
2. Erreurs d'attribution organisation-rôle
Event University of Zurich (July 2026):

Roles listés: "co-founder of Abdul Latif Jameel Poverty Action Lab", "serves on academic advisory board of Plaksha University", "lecturer of literature at MIT"
Ces rôles n'ont AUCUN rapport avec UZH. Seul le rôle "co-direct the newly-established Lemann Center" est valide pour UZH.

3. Event_type incorrects

"That same year" award: confidence "high" mais aucune organisation/rôle/contexte
Royal Swedish Academy of Sciences: event_type "award" avec role "professor of Poverty Alleviation"? Non, c'est l'institution décernant le Nobel

4. Extractions fantômes ou décontextualisées

CEPR/VoxEU: confidence "low" - pourquoi extraire si low confidence?
Business Standard event: "MIT economist" comme rôle? C'est une description journalistique pas une position

5. Manque d'événements clés documentés
Absents de vos extractions:

Ford Foundation International Professor (position actuelle principale à MIT)
Guggenheim Fellowship
Gerald Loeb Award 2012
Infosys Prize jury service 2018
UN panel appointment 2013
Bernhard-Harms-Prize 2014
Trustee of British Museum
Co-director of J-PAL (vs juste co-founder)

Extractions correctes
Bonnes captures:

Nobel Prize 2019 (général)
University of Calcutta honorary degree January 2020
American Academy of Achievement Golden Plate September 2022
Lemann Center co-director role at UZH July 2026
Plaksha University advisory board
Alumni affiliations (Presidency, JNU, Harvard, etc.)

Recommandations
Priorité immédiate:

Séparer événements temporels uniques (awards, appointments) des affiliations continues
Validation stricte organisation-rôle: un rôle doit être logiquement lié à l'organisation listée
Ne pas fragmenter un seul événement en multiples entries avec des combinaisons incorrectes

Améliorations système:

Post-processing: détecter duplications temporelles (même time_marker, organizations différentes)
Validation sémantique: vérifier cohérence organisation-rôle
Filtrage confidence: éliminer automatiquement extractions "low" confidence
Event consolidation: fusionner fragments se référant au même événement

Voulez-vous que je génère un script de validation pour détecter ces patterns problématiques?
[CHAT_END]


[USER_BEGIN]
Please create a new json file that is correct.
[USER_END]

[CHAT_BEGIN]
{v2_output}
[CHAT_END]


[USER_BEGIN]
Perfect, now complete the same task for this new source. Think through and then output to a valid json file.

{new_raw_input}
[USER_END]
"""
    
    return prompt_template.replace('{raw_input}', raw_text) \
                   .replace('{v1_output}', v1_content) \
                   .replace('{v2_output}', v2_content) \
                   .replace('{new_raw_input}', raw_text)

def extract_json_from_response(response_text):
    """Extract JSON from the API response, handling markdown code fences."""
    # Remove markdown code fences if present
    json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
    if json_match:
        json_str = json_match.group(1)
    else:
        # Try to find JSON content directly
        json_str = response_text.strip()
    
    # Clean up any trailing text
    json_str = json_str.split('\n[CHAT_END]')[0].strip()
    json_str = json_str.split('\n[USER_END]')[0].strip()
    
    return json_str

def call_claude_api(prompt: str, api_key: str) -> str:
    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model="claude-sonnet-4-5-20250929",
        max_tokens=12000,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.content[0].text

def main():
    # Load environment variables from .env file in project root
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(env_path, override=True)  # override=True ensures .env file takes precedence
    
    parser = argparse.ArgumentParser(description='Extract career events from biographical text using Claude API')
    parser.add_argument('--input', type=Path, required=True, help='Input text file containing raw biography')
    parser.add_argument('--output', type=Path, required=True, help='Output JSON file for extracted career events')
    args = parser.parse_args()
    
    # Load few-shot examples
    base_dir = Path(__file__).parent
    raw_example = load_file_content(base_dir / 'Abhijit_Banerjee_raw.txt')
    v1_example = load_file_content(base_dir / 'Abhijit_Banerjee_v1.json')
    v2_example = load_file_content(base_dir / 'Abhijit_Banerjee_v2.json')
    
    # Load new input text
    new_raw_text = load_file_content(args.input)
    
    # Get API key from environment
    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")
    
    # Construct prompt
    prompt = construct_prompt(new_raw_text, v1_example, v2_example)
    
    print("Constructed prompt length:", len(prompt), "characters")
    print("Calling Claude API...")
    
    # Call API
    response_text = call_claude_api(prompt, api_key)
    
    print("API call completed. Processing response...")
    
    # Extract JSON
    json_str = extract_json_from_response(response_text)
    
    # Validate and parse JSON
    try:
        result = json.loads(json_str)
        
        # Save to output file
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print(f"Successfully saved extracted career events to: {args.output}")
        print(f"Extracted {len(result.get('career_events', []))} career events")
        
    except json.JSONDecodeError as e:
        print("Failed to parse JSON response:")
        print("Response text:")
        print(response_text[:1000] + "..." if len(response_text) > 1000 else response_text)
        raise Exception(f"JSON parse error: {str(e)}")

if __name__ == "__main__":
    main()