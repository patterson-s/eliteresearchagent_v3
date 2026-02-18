import json

# Load the chunks
with open('abhijit_banerjee_chunks.json', 'r', encoding='utf-8') as f:
    chunks = json.load(f)

# Filter for Wikipedia chunks
wikipedia_chunks = [chunk for chunk in chunks if 'wikipedia' in chunk['source_url'].lower()]
print(f'Found {len(wikipedia_chunks)} Wikipedia chunks')

# Sort by chunk_index to reconstruct the original order
wikipedia_chunks.sort(key=lambda x: x['chunk_index'])

# Combine all text
full_text = ''
for i, chunk in enumerate(wikipedia_chunks):
    full_text += chunk['text'] + '\n\n'

# Save to file
with open('abhijit_banerjee_wikipedia.txt', 'w', encoding='utf-8') as out_f:
    out_f.write(full_text)

print(f'Combined Wikipedia content saved to abhijit_banerjee_wikipedia.txt')
print(f'Total characters: {len(full_text)}')
print('First 500 characters:')
print(full_text[:500])