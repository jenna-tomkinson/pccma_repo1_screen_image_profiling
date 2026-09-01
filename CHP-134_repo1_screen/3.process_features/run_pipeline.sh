#!/bin/bash
#
# Run the full feature-processing pipeline, in order:
#   1.merge_profiles.ipynb       -> one merged parquet per plate (via nbconverted script)
#   2.single_cell_qc.ipynb       -> one QC-annotations parquet per plate (via papermill,
#                                    which keeps the executed notebook per plate for review)
#   3.bulk_processing.ipynb      -> aggregated/annotated/normalized/feature-selected bulk profiles (via nbconverted script)
#   4.single_cell_processing.ipynb -> annotated/normalized/feature-selected single-cell profiles (via nbconverted script)

set -uo pipefail

# -----------------------------
# Initialize environment
# -----------------------------
conda init bash
conda activate pccma_repo1_preprocessing_env

failed_plates=()

# convert notebooks to scripts
jupyter nbconvert --to script --output-dir=nbconverted/ *.ipynb

# -----------------------------
# Step 1: merge row-batch profiles into one profile per plate
# -----------------------------
echo "======================================"
echo "Step 1: 1.merge_profiles.ipynb"
echo "======================================"
python nbconverted/1.merge_profiles.py

# -----------------------------
# Step 2: per-plate single-cell QC via papermill
# -----------------------------
echo "======================================"
echo "Step 2: 2.single_cell_qc.ipynb (papermill, per plate)"
echo "======================================"

qc_notebook_dir="./executed_notebooks/2.single_cell_qc"
mkdir -p "$qc_notebook_dir"

mapfile -t plate_ids < <(find ./data/merged_profiles -maxdepth 1 -name "*.parquet" -printf "%f\n" | sed 's/\.parquet$//' | sort)

echo "Number of plates found: ${#plate_ids[@]}"

for plate_id in "${plate_ids[@]}"; do
    qc_output="./data/qc_results/${plate_id}_qc_annotations.parquet"
    executed_notebook="${qc_notebook_dir}/${plate_id}.ipynb"

    if [ -f "$qc_output" ]; then
        echo "✅ ${plate_id} already QC'd (found ${qc_output})"
        continue
    fi

    echo ">>> Running QC for ${plate_id}"
    # render_diagnostics=False skips the per-condition CytoDataFrame image
    # previews, which pull crops off the bandicoot network mount and are only
    # useful for interactive review -- the QC annotations export at the
    # bottom of the notebook doesn't depend on them. Leaving this on for a
    # batch run across many plates is what causes each plate to take a very
    # long time (or effectively hang) here.
    papermill 2.single_cell_qc.ipynb "$executed_notebook" \
        -p plate_id "$plate_id" \
        -p render_diagnostics False
    exit_code=$?

    if [ "$exit_code" -ne 0 ]; then
        echo "QC FAILED for plate: ${plate_id} (exit code $exit_code)"
        failed_plates+=("qc:${plate_id}")
    else
        echo "QC done for plate: ${plate_id}"
    fi
done

# -----------------------------
# Step 3: bulk processing (skips plates without QC, and already-processed plates)
# -----------------------------
echo "======================================"
echo "Step 3: 3.bulk_processing.ipynb"
echo "======================================"
python nbconverted/3.bulk_processing.py
exit_code=$?
if [ "$exit_code" -ne 0 ]; then
    echo "3.bulk_processing.ipynb FAILED (exit code $exit_code)"
    failed_plates+=("bulk_processing")
fi

# -----------------------------
# Step 4: single-cell processing (skips plates without QC, and already-processed plates)
# -----------------------------
echo "======================================"
echo "Step 4: 4.single_cell_processing.ipynb"
echo "======================================"
python nbconverted/4.single_cell_processing.py
exit_code=$?
if [ "$exit_code" -ne 0 ]; then
    echo "4.single_cell_processing.ipynb FAILED (exit code $exit_code)"
    failed_plates+=("single_cell_processing")
fi

conda deactivate

echo "======================================"
echo "Pipeline finished."
if [ ${#failed_plates[@]} -gt 0 ]; then
    echo "Failures:"
    printf '  %s\n' "${failed_plates[@]}"
    exit 1
fi

echo "✅ All steps completed successfully."
