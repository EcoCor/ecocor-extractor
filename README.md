# ecocor-extractor

### Description

This repository hosts a script which extracts frequencies of words from a given word list in
text segments. As input a JSON object is passed over an API which saves the text segment
and their IDs and optionally a URL to the word list on which basis the freqeuncies are
extracted. It returns a JSON in which for each word the frequency per text segment is
saved.

### Formats

It is required that the keys in the Input and the WordList are as below:

- Input Format: `{"segments": [{"segment_id":"xyz", "text":"asd ..."}, ...], "language":"de",
"word_list":{"url":"http://..."}}`
- WordList Format: `[{"word":abc, "wikidata_ID":"Q12345","category":"plant"}]`
- Output Format: `[{"word":"xyz", "wikidata_ID":"Q12345","category":"plant",
"overall_frequency":1234, "segment_frequencies":{segment_id:1234,...}}]`

### Requirements

This scripts requires `spacy` and `FastAPI` to be installed. Additionally the spacy models
for English and German must be downloaded: `de_core_news_sm`, `en_core_web_sm`

### Test

The script was tested using [uvicorn](https://fastapi.tiangolo.com/lo/#installation).
Unittests are provided in `test/` and can be executed with `python -m unittest test/test_extractor.py`
The following curl command can be used to post a JSON file to the extractor service:
`curl -X POST -H "Content-Type: application/json" 127.0.0.1:8000/extractor --data-binary @test/test.json`. The items in the response should match `test/result.json`.

### Options

By default the script counts all occurences of tokens that match a given word list, as configured in [config.py](./config.py), no matter their position. You can tell spaCy to count only tokens in [NOUN position](https://universaldependencies.org/u/pos/index.html) by passing the command line option `--noun-only`
