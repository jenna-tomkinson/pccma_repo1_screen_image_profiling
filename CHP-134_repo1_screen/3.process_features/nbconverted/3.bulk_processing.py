#!/usr/bin/env python
# coding: utf-8

# # Process bulk profiles

# ## Import libraries

# In[1]:


import pathlib
import pprint

import pandas as pd

from pycytominer import aggregate, annotate, normalize, feature_select
from pycytominer.cyto_utils import output


# ## Set paths and variables

# In[2]:


# Directory containing one merged profile parquet per plate
merged_dir = pathlib.Path("./data/merged_profiles")

# Directory containing per-plate QC annotation files from 2.single_cell_qc.ipynb
qc_dir = pathlib.Path("./data/qc_results")

# output path for bulk profiles
output_dir = pathlib.Path("./data/bulk_profiles")
output_dir.mkdir(parents=True, exist_ok=True)

# path for platemap directory
platemap_dir = pathlib.Path("../0.download_data/metadata")

# load in barcode platemap
barcode_platemap = pd.read_csv(platemap_dir / "barcode_platemap.csv")

# plate_id always uses underscores (e.g. "Assay_Plate_1_3"), but a few
# barcodes in this file use a space instead (e.g. "Assay Plate_1_3")
barcode_platemap["Plate Barcode"] = barcode_platemap["Plate Barcode"].str.replace(
    " ", "_", regex=False
)

# extract the plate names from the merged profile file names
plate_names = sorted(file.stem for file in merged_dir.glob("*.parquet"))

# operations to perform for feature selection
feature_select_ops = [
    "variance_threshold",
    "correlation_threshold",
    "blocklist",
    "drop_na_columns",
]

plate_names


# ## Set dictionary with plates to process

# In[3]:


# create plate info dictionary
plate_info_dictionary = {
    plate_id: {
        "profile_path": str(merged_dir / f"{plate_id}.parquet"),
        # QC annotations are produced per-plate by 2.single_cell_qc.ipynb;
        # not every plate has been QC'd yet, so this may be None
        "qc_path": (
            str(qc_dir / f"{plate_id}_qc_annotations.parquet")
            if (qc_dir / f"{plate_id}_qc_annotations.parquet").exists()
            else None
        ),
        # Find the platemap file based on barcode match
        "platemap_path": (
            str(
                platemap_dir
                / barcode_platemap.loc[
                    barcode_platemap["Plate Barcode"] == plate_id, "File Name"
                ].values[0]
            )
            if plate_id in barcode_platemap["Plate Barcode"].values
            else None
        ),
    }
    for plate_id in plate_names
}

# Display the dictionary to verify the entries
pprint.pprint(plate_info_dictionary, indent=4)


# ## Process data with pycytominer

# In[4]:


for plate_id, info in plate_info_dictionary.items():
    if info["qc_path"] is None:
        print(
            f"Skipping {plate_id}: no QC annotations yet "
            "(run 2.single_cell_qc.ipynb for this plate first)"
        )
        continue
    if info["platemap_path"] is None:
        print(f"Skipping {plate_id}: no platemap found in barcode_platemap.csv")
        continue

    # Output file paths for each file
    output_aggregated_file = str(output_dir / f"{plate_id}_bulk.parquet")
    output_annotated_file = str(output_dir / f"{plate_id}_bulk_annotated.parquet")
    output_normalized_file = str(output_dir / f"{plate_id}_bulk_normalized.parquet")
    output_feature_select_file = str(
        output_dir / f"{plate_id}_bulk_feature_selected.parquet"
    )

    # Already fully processed, so this plate can safely be skipped on a rerun
    if pathlib.Path(output_feature_select_file).exists():
        print(f"Skipping {plate_id}: already processed (found {output_feature_select_file})")
        continue

    print(f"Now performing pycytominer pipeline for {plate_id}")

    # Load single-cell profile, its QC annotations, and the platemap
    single_cell_df = pd.read_parquet(info["profile_path"])
    qc_df = pd.read_parquet(info["qc_path"])
    platemap_df = pd.read_csv(info["platemap_path"]).rename(
        columns={"Well Position": "Well"}
    )

    # Merge QC flags onto the profile and drop any cell flagged by at least
    # one QC condition (clustered/missegmented nuclei, background segmented
    # as a nucleus, whole-cell intensity outliers, etc.)
    cqc_cols = [col for col in qc_df.columns if col.startswith("Metadata_cqc_")]
    join_keys = ["Metadata_ImageNumber", "Metadata_Cells_Number_Object_Number"]

    single_cell_df = single_cell_df.merge(
        qc_df[join_keys + cqc_cols], on=join_keys, how="left", validate="one_to_one"
    )
    assert (
        single_cell_df[cqc_cols].isna().sum().sum() == 0
    ), f"{plate_id}: some cells have no matching QC annotation"

    is_poor_quality = single_cell_df[cqc_cols].any(axis=1)
    print(
        f"  Dropping {is_poor_quality.sum()} / {len(single_cell_df)} "
        "poor-quality segmentations"
    )
    single_cell_df = (
        single_cell_df.loc[~is_poor_quality]
        .drop(columns=cqc_cols)
        .reset_index(drop=True)
    )

    # Step 1: Aggregation
    aggregate(
        population_df=single_cell_df,
        operation="median",
        strata=["Image_Metadata_Plate", "Image_Metadata_Well"],
        output_file=output_aggregated_file,
        output_type="parquet",
    )

    # Step 2: Annotation
    annotated_df = annotate(
        profiles=output_aggregated_file,
        platemap=platemap_df,
        join_on=["Metadata_Well", "Image_Metadata_Well"],
    )

    # Step 3: Output annotated DataFrame
    output(
        df=annotated_df,
        output_filename=output_annotated_file,
        output_type="parquet",
    )

    # Step 4: Normalization (whole-plate)
    normalize(
        profiles=annotated_df,
        method="standardize",
        output_file=output_normalized_file,
        output_type="parquet",
        samples="all",
    )

    # Step 5: Feature selection
    feature_select(
        output_normalized_file,
        operation=feature_select_ops,
        na_cutoff=0,
        output_file=output_feature_select_file,
        output_type="parquet",
    )


# In[5]:


# Check output file
test_df = pd.read_parquet(output_feature_select_file)

print(test_df.shape)
test_df.head(2)

