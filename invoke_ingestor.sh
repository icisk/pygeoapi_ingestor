# bin/bash
# Description: Script to invoke the ingestor process

# ingestor id
ingestor_process=ingestor-process

# parameters for the ingestor
data_dir="seasonal_forecast"
issue_date=$(date '+%Y%m')
living_lab="georgia"
zarr_out="s3://saferplaces.co/test/icisk/living_labs/test_ingestor/smhi/${issue_date}/${living_lab}_${data_dir}.zarr"

# initialize body data
data='{
  "inputs": {
    "data_dir": "'${data_dir}'",
    "issue_date": "'${issue_date}'",
    "living_lab": "'${living_lab}'",
    "zarr_out": "'${zarr_out}'"
  }
}'

echo "Ingestor process: 'http://localhost:5000/processes/${ingestor_process}/execution'"
echo "Data directory: $data_dir"
echo "Current date: $issue_date"
echo "Living lab: $living_lab"
echo "Zarr output: $zarr_out"
echo "Data: $data"

# curl command to invoke the ingestor
curl -X 'POST' \
  "http://localhost:5000/processes/${ingestor_process}/execution" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d "${data}"
