"""
Create a combined animals and plants word list from GermaNet and Wikidata.
"""

import polars as pl
from glob import glob
import json
from datetime import datetime


lookup_df = pl.DataFrame(schema={"wikidata_id": pl.Utf8, "word": pl.Utf8})

# Dataframe to gather entries with category but without wikidata_id
df_category = pl.DataFrame()

# Columns: word, category
germanet = pl.read_json("data/germanet_animal_plant.json")

# Add GermaNet entries to this
df_category = df_category.vstack(germanet.select(["word", "category"]))

# Read in all files
for file in glob("data/*.tsv"):
    # Read file
    file_df = pl.read_csv(file, separator="\t")

    # Files without category column: Add to lookup_df for joining with df_category
    if "category" not in file_df.columns:
        lookup_df = lookup_df.vstack(file_df)
        continue

    # Files with wikidata_id column: Add to lookup_df for joining with df_category
    if "wikidata_id" in file_df.columns:
        lookup_df = lookup_df.vstack(file_df.select(["wikidata_id", "word"]))
        continue

    # Files with category column: Collect in df_category
    df_category = df_category.vstack(file_df.select(germanet.columns))

# Remove duplicates
df_category = df_category.unique(keep="first")

# Remove duplicates from lookup_df
lookup_df = lookup_df.unique(keep="first")

# Add wikidata IDs: Join df_category entries with lookup_df
joined_df = df_category.join(lookup_df, on="word", how="left").filter(
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
