#!/bin/bash

# set -x

export PATH=""
export PYTHONPATH=""

reporting_folder=""

cd "$reporting_folder" || { echo "Failed to change directory to $reporting_folder"; exit 1; }

echo "Tiyambe processing zimenezi............"

reports_folder="final_pull"
processed_folder="processed_files"
today=$(date +%Y-%m-%d)
prev_reports_folder="final_pull_prev_$today"

if [ -d "$reports_folder" ]; then
    if [ -d "$prev_reports_folder" ]; then
        rm -r "$prev_reports_folder"
        echo "Deleted existing prev folder $prev_reports_folder"
    fi
    mv "$reports_folder" "$prev_reports_folder"
    echo "Renamed $reports_folder to $prev_reports_folder"
fi

# Create a new final_pull folder
mkdir "$reports_folder"
echo "Created $reports_folder"

PYTHON_SCRIPT=""
LOG_FILE=""
PYSPARK_SCRIPT=""

# Check if the Python script exists and log if it does not
{
if [ ! -f "$PYTHON_SCRIPT" ]; then
  echo "Python script not found: $PYTHON_SCRIPT"
  exit 1
fi
} >> "$LOG_FILE" 2>&1

echo "Processing insertion and reports pulling now.............."

# Run the Python script
python3 "$PYTHON_SCRIPT"

echo "Done processing insertion and reports pulling.............."

cd "$reporting_folder/$reports_folder" || { echo "Failed to change directory to $reporting_folder/$reports_folder"; exit 1; }

indicators=("REPORT_1" "REPORT_2" "REPORT_3" "REPORT_4")

for indicator in "${indicators[@]}"; do
  echo "Processing files with prefix: $indicator"  
  if ls ${indicator}* 1> /dev/null 2>&1; then
    cat ${indicator}* >> "${indicator}_amalgamated.csv"
    echo "Concatenated ${indicator}* into ${indicator}_amalgamated.csv"
  else
    echo "No files found for indicator $indicator"
  fi
done

cd "$reporting_folder/$processed_folder" || { echo "Failed to change directory to $reporting_folder/$processed_folder"; exit 1; }

ls -l  

if ls *amalgamated.csv 1> /dev/null 2>&1; then
    echo "Let's remove the available amalgamated files"
    rm *amalgamated.csv
    echo "Done removing!"
else
    echo "No amalgamated files found to remove."
fi

cp "$reporting_folder/$reports_folder/"*amalgamated.csv .

if [ $? -ne 0 ]; then
    echo "Error copying files"
else
    echo "Files copied successfully"
fi

cd "$reporting_folder/" || { echo "Failed to change directory to $reporting_folder/"; exit 1; }

python3 "$PYSPARK_SCRIPT"

echo "Done!! Tikunge comparisons basi.............."


