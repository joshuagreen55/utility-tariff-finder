"""Run Track B LLM absorption on Chunk 2 utilities only."""
import sys
sys.path.insert(0, "/app")

from sqlalchemy.orm import Session
from app.db.session import get_sync_engine
from scripts.chunk2_followup import CHUNK2_IDS, track_b_chunk2

if __name__ == "__main__":
    apply = "--apply" in sys.argv
    with Session(get_sync_engine()) as session:
        result = track_b_chunk2(session, apply=apply)
    print(result)
