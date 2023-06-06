#!/usr/bin/env/python

""" EcoCor Extractor This script extracts frequencies of names from a given name list in
text segments. As input a JSON object is passed over an API which saves the text segment
and their IDs and optionally a URL to the name list on which basis the freqeuncies are
extracted. It returns a JSON in which for each name the frequency per text segment is
saved.

Input Format: {"segments": [{"segment_id":"xyz", "text":"asd ..."}, ...], "language":"de",
"entity_list":{"url":"http://..."}}
EntityList Format: {"entity_list": [{"name":abc, "wikidata_id":"Q12345","category":"plant"},{...}],
"metadata":{"description":"abcd"}]
Output Format: [{"name":"xyz", "wikidata_id":["Q12345"],"category":"plant",
"overall_frequency":1234, "segment_frequencies":{segment_id:1234,...}}] 
-> only of names that appear at least once in the text

This scripts requires `spacy` and `FastAPI` to be installed. Additionally the spacy models
for English and German must be downloaded: `de_core_news_sm`, `en_core_web_sm` """

import sys
from collections import Counter
from datetime import date
from enum import Enum
from functools import cache
from typing import Optional

import requests
import spacy
from fastapi import FastAPI
from pydantic import BaseModel
from pydantic.networks import HttpUrl

app = FastAPI()
NOUN_POS = "NOUN"


class Language(str, Enum):
    EN = "en"
    DE = "de"

    @cache
    def get_spacy_model(self) -> spacy.Language:
        return {
            Language.DE: spacy.load("de_core_news_sm"),
            Language.EN: spacy.load("en_core_web_sm"),
        }[self]

    def get_entity_list(self) -> str:
        return {
            Language.DE: "https://raw.githubusercontent.com/dh-network/ecocor-extractor/main/word_list/german/animal_plant-de.json",
            Language.EN: "https://raw.githubusercontent.com/dh-network/ecocor-extractor/main/word_list/english/animal_plant-en.json",
        }[self]


class Segment(BaseModel):
    text: str
    segment_id: str


class NameInfo(BaseModel):
    name: str
    wikidata_id: str
    category: str
    additional_wikidata_ids: list[str] = []


class NameMetadata(BaseModel):
    name: str
    description: str
    date: date


class NameInfoFrequency(NameInfo):
    segment_frequencies: dict[str, int]
    overall_frequency: int


class NameInfoMeta(BaseModel):
    metadata: NameMetadata
    entity_list: list[NameInfo]


class NameInfoFrequencyMeta(BaseModel):
    metadata: NameMetadata
    entity_list: list[NameInfoFrequency]


class UrlDescriptor(BaseModel):
    url: HttpUrl


class SegmentEntityListUrl(BaseModel):
    segments: list[Segment]
    language: Language
    entity_list: Optional[UrlDescriptor]

    def get_entity_list(self) -> UrlDescriptor:
        if self.entity_list:
            return self.entity_list
        else:
            return UrlDescriptor(url=self.language.get_entity_list())


@app.get("/")
def root():
    return {"service": "ecocor-extractor", "version": "0.0.0"}


# TODO: handle exception nicer?
def read_entity_list(url: str) -> NameInfoMeta:
    print(url)
    response = requests.get(url)
    response.raise_for_status()
    name_info_meta = NameInfoMeta(**response.json())
    return name_info_meta


@app.post("/extractor")
def process_text(segments_entity_list: SegmentEntityListUrl) -> NameInfoFrequencyMeta:
    nlp = segments_entity_list.language.get_spacy_model()

    name_info_meta = read_entity_list(segments_entity_list.get_entity_list().url)
    name_to_name_info = {}

    for entry in name_info_meta.entity_list:
        if entry.name not in name_to_name_info:
            name_to_name_info[entry.name] = []
        name_to_name_info[entry.name].append(entry.dict())

    unique_names = set([entry.name for entry in name_info_meta.entity_list])

    # annotate
    name_to_segment_frq = {}

    for i, annotated_segment in enumerate(
        nlp.pipe(
            [segment.text for segment in segments_entity_list.segments],
            disable=["parser", "ner"],
        )
    ):
        lemmatized_text = [
            token.lemma_ for token in annotated_segment if token.pos_ == NOUN_POS
        ]

        # count and intersect
        vocabulary = set(lemmatized_text)
        counted = Counter(lemmatized_text)
        intersect = unique_names.intersection(vocabulary)

        # save frequencies
        for name in intersect:
            if name not in name_to_segment_frq:
                name_to_segment_frq[name] = {}
            name_to_segment_frq[name][
                segments_entity_list.segments[i].segment_id
            ] = counted[name]

    name_info_frequency = []
    for name, segment_frq in name_to_segment_frq.items():
        name_infos = name_to_name_info[name]
        overall_frequency = sum(segment_frq.values())
        for name_info in name_infos:
            name_info_frequency.append(
                NameInfoFrequency(
                    segment_frequencies=segment_frq,
                    overall_frequency=overall_frequency,
                    **name_info,
                )
            )
    result = NameInfoFrequencyMeta(
        entity_list=name_info_frequency, metadata=name_info_meta.metadata
    )

    return result


if __name__ == "__main__":
    args = sys.argv
    if len(args) != 2:
        print(f"usage: {args[0]} path/to/test/file")
        exit(-1)
    import json

    with open(args[1]) as json_in:
        segments = json.load(json_in)
    segments_entity_list = SegmentEntityListUrl(**segments)

    result = process_text(segments_entity_list)
    print(result)
