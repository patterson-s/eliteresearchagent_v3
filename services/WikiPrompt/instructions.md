# Prompt Execution System — Career Event Extraction

## Context

This system runs a few-shot prompt against the Claude API (Sonnet 4.5) to extract structured career events from raw biographical text.

All project files are located at:
`C:\Users\spatt\Desktop\eliteresearchagent_v3\services\WikiPrompt`

The few-shot example uses Abhijit Banerjee:
- `Abhijit_Banerjee_raw.txt` — raw Wikipedia text (input)
- `Abhijit_Banerjee_v1.json` — first extraction attempt (bad)
- `Abhijit_Banerjee_v2.json` — corrected extraction (good)
- `prompt_main.txt` — the prompt template with placeholders

---

## How the Prompt Works

The prompt is a simulated multi-turn conversation passed to the API. It demonstrates:

1. User provides raw text + bad extraction → asks for evaluation
2. Assistant critiques the bad extraction
3. User asks for a corrected JSON
4. Assistant returns the corrected JSON (`v2`)
5. User provides a new raw text → Assistant produces a new extraction

This is a few-shot approach: no explicit instructions, just a worked example.

---

## What to Build

A Python CLI script (`run_extraction.py`) that:

1. Loads the few-shot files from disk
2. Constructs the multi-turn `messages` array
3. Accepts a new raw text file as a CLI argument
4. Calls the Anthropic API
5. Writes the extracted JSON to a output file

---

## Script Interface

```bash
python run_extraction.py --input new_person_raw.txt --output new_person_v1.json
```

---

## Messages Array Structure

The API call uses `messages` in this order:

```
role: user    → evaluation request with Banerjee raw + v1
role: assistant → critique of v1
role: user    → "create a corrected json"
role: assistant → v2 json
role: user    → "do the same for this new source" + new raw text
```

The final assistant response is the extracted JSON for the new person.

---

## API Call

- Model: `claude-sonnet-4-5`
- Max tokens: A lot (extractions can be long)
- API key: environment variable `ANTHROPIC_API_KEY`

---

## Dependencies

```
anthropic
```

Install:
```bash
pip install anthropic
```

---

## Output

The script should:
- Print the raw response to stdout
- Save the JSON to the specified `--output` file
- If the response is wrapped in markdown code fences, strip them before saving