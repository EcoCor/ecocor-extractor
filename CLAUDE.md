# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run tests (requires .venv — see README Local Setup)
python -m unittest test/test_extractor.py

# Run dev server
uvicorn extractor.main:app --host 0.0.0.0 --port 8000

# Run as CLI script
python extractor/main.py test/test.json

# Build and run Docker container
docker build -t ecocor-extractor .
docker run -p 8000:80 ecocor-extractor

# Test the running API
curl -X POST -H "Content-Type: application/json" 127.0.0.1:8000/extractor --data-binary @test/test.json
```

Always run `python -m unittest test/test_extractor.py` after making any code changes.

## Architecture

This is a single-file FastAPI microservice (`extractor/main.py`) that extracts animal/plant/fungi entity frequencies from narrative texts for the EcoCor ecological analysis project.

**Request flow:**

1. POST `/extractor` receives segments of text, a language (`de`/`en`), and an optional entity list URL
2. The entity list (default or fetched from URL) contains names with Wikidata IDs and categories (Animal/Plant/Fungi)
3. spaCy lemmatizes and POS-tags the text (parser and NER disabled for speed)
4. By default, each token's lemma is matched against the lemmas of entity names. If the flag --noun-only is set, only the nouns are checked.
5. Response contains per-segment and overall frequency counts for matched entities

**Key design decisions:**

- spaCy models are loaded once per language via a cached `Language` enum class attribute (`_nlp_cache`), not on every request
- Entity names can have multiple word forms (e.g., hyphenated compounds); however only single words are checked, so the compound words in the list can't actually be found.
- The `word_list/` directory contains pre-built JSON entity lists for DE and EN, sourced from GermaNet and Wikidata taxon names

**Input/output format** is documented in `test/test.json` (input) and the Pydantic models in `extractor/main.py`: `SegmentEntityListUrl` (request) with nested `Segment` and `UrlDescriptor`; `NameInfoMeta` (entity list) with nested `NameMetadata` and `NameInfo`; `NameInfoFrequencyMeta` (response) with nested `NameInfoFrequency`.

**Word lists** (`word_list/`): JSON files with `metadata` and `entity_list` arrays. See `word_list/list_descriptions.md` for sources and composition rationale. Ambiguous words (common German/English words that also name animals) are periodically removed.
