"""
Create a combined animals and plants word list from GermaNet and Wikidata.
"""

import polars as pl
from glob import glob
import json
from datetime import datetime

# Columns: word, category
germanet = pl.read_json("data/germanet_animal_plant.json")

lookup_df = pl.DataFrame(schema={"wikidata_id": pl.Utf8, "word": pl.Utf8})

# Dataframe to gather entries without category
df_no_category = pl.DataFrame()

# Read in all files
for file in glob("data/*.tsv"):
    # Read file
    file_df = pl.read_csv(file, separator="\t")

    # Files without category column: Add to lookup_df for joining with GermaNet
    if "category" not in file_df.columns:
        lookup_df = lookup_df.vstack(file_df)
        continue

    # Files with category column: Collect in df_no_category
    df_no_category = df_no_category.vstack(file_df.select(germanet.columns))


# Remove duplicates
lookup_df = lookup_df.unique(keep="first")

# Join germanet and lookup_df
joined_df = germanet.join(lookup_df, on="word", how="left")

# vstack df_no_category
joined_df = joined_df.vstack(
    # Create null wikidata_id column for vstack
    df_no_category.with_columns(wikidata_id=pl.lit(None).cast(pl.Utf8))
).filter(
    # Remove entries without category
    pl.col("category").is_not_null()
)


# Processing for JSON export
joined_for_json = (
    # Join words with multiple Wikidata IDs into one row for JSON export
    joined_df.groupby(["word", "category"])
    .agg(wikidata_ids=pl.col("wikidata_id").implode().explode())
    # Replace [null] arrays with []
    .with_columns(
        wikidata_ids=pl.when(pl.col("wikidata_ids").list[0].is_null())
        .then([])
        .otherwise(pl.col("wikidata_ids"))
    )
    .sort("word")
)

print("joined_for_json", joined_for_json)

word_list_dicts = joined_for_json.to_dicts()

# Create meta-JSON
meta_json = {
    "metadata": {
        "name": "Plant and Animal list (de)",
        "description": "German word list for plants and animals gathered from GermaNet and Wikidata",
        "date": datetime.now().strftime("%Y-%m-%d"),
    },
    "word_list": word_list_dicts,
}

json.dump(meta_json, open("combined_word_list.json", "w"))
print("Wrote combined_word_list.json")
