import os
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(env_path)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "eliteresearch"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD")
    )

def load_all_chunks_from_db() -> List[Dict[str, Any]]:
    conn = get_db_connection()
    
    try:
        query = """
            SELECT 
                c.id as chunk_id,
                c.text,
                c.chunk_index,
                sr.url as source_url,
                sr.title,
                sr.extraction_method,
                ps.person_name
            FROM sources.chunks c
            JOIN sources.search_results sr ON c.search_result_id = sr.id
            JOIN sources.persons_searched ps ON sr.person_search_id = ps.id
            ORDER BY ps.person_name, sr.url, c.chunk_index
        """
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()
        
        chunks = [dict(row) for row in rows]
        return chunks
    
    finally:
        conn.close()

def load_chunks_from_db(person_name: str) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    
    try:
        query = """
            SELECT 
                c.id as chunk_id,
                c.text,
                c.chunk_index,
                sr.url as source_url,
                sr.title,
                sr.extraction_method,
                ps.person_name
            FROM sources.chunks c
            JOIN sources.search_results sr ON c.search_result_id = sr.id
            JOIN sources.persons_searched ps ON sr.person_search_id = ps.id
            WHERE ps.person_name = %s
            ORDER BY sr.url, c.chunk_index
        """
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (person_name,))
            rows = cur.fetchall()
        
        chunks = [dict(row) for row in rows]
        return chunks
    
    finally:
        conn.close()

def load_chunks_from_file(file_path: Path, person_name: str) -> List[Dict[str, Any]]:
    with open(file_path, "r", encoding="utf-8") as f:
        all_chunks = json.load(f)
    
    person_chunks = [
        chunk for chunk in all_chunks
        if chunk.get("person_name") == person_name
    ]
    
    return person_chunks

def save_chunks_to_file(chunks: List[Dict[str, Any]], file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(chunks, f, indent=2, ensure_ascii=False)

def load_chunks_for_person(person_name: str, from_file: Optional[Path] = None) -> List[Dict[str, Any]]:
    if from_file and from_file.exists():
        return load_chunks_from_file(from_file, person_name)
    else:
        return load_chunks_from_db(person_name)

def get_all_people() -> List[str]:
    conn = get_db_connection()
    
    try:
        query = """
            SELECT DISTINCT person_name
            FROM sources.persons_searched
            ORDER BY person_name
        """
        
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        
        people = [row[0] for row in rows]
        return people
    
    finally:
        conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Load chunks for a person from database")
    parser.add_argument("--person", help="Person name (omit to load all people)")
    parser.add_argument("--list-people", action="store_true", help="List all people in database")
    parser.add_argument("--save", type=Path, help="Save chunks to JSON file")
    parser.add_argument("--all", action="store_true", help="Load chunks for all people")
    args = parser.parse_args()
    
    if args.list_people:
        people = get_all_people()
        print(f"\nFound {len(people)} people in database:")
        for i, person in enumerate(people[:20], 1):
            print(f"  {i}. {person}")
        if len(people) > 20:
            print(f"  ... and {len(people) - 20} more")
    
    elif args.all or (not args.person and args.save):
        print("Loading chunks for ALL people from database...")
        chunks = load_all_chunks_from_db()
        
        people_count = len(set(c['person_name'] for c in chunks))
        print(f"Found {len(chunks)} chunks for {people_count} people")
        
        if args.save:
            save_chunks_to_file(chunks, args.save)
            print(f"\nSaved to: {args.save.resolve()}")
    
    elif args.person:
        print(f"Loading chunks for {args.person} from database...")
        chunks = load_chunks_from_db(args.person)
        print(f"Found {len(chunks)} chunks")
        
        if chunks:
            print(f"First chunk ID: {chunks[0].get('chunk_id')}")
            print(f"Sample URL: {chunks[0].get('source_url', 'unknown')}")
        
        if args.save:
            save_chunks_to_file(chunks, args.save)
            print(f"\nSaved to: {args.save.resolve()}")
    
    else:
        print("Use --list-people to see available people")
        print("Use --person NAME to load chunks for one person")
        print("Use --all to load chunks for all people")