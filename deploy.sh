set -e

# Define the AWS CLI profile name
AWS_PROFILE="saml"

delete_venv_and_dist() {
  local venv_dir=".venv"
  local dist_dir="dist"
  if [ -d $venv_dir ]; then
    rm -rf $venv_dir
    echo "Deleted .venv directory."
  fi
  if [ -d $dist_dir ]; then
    rm -rf $dist_dir
    echo "Deleted dist directory."
  fi
}

# Function to delete the .venv directory if it exists
delete_venv() {
  local venv_dir=".venv"
  if [ -d $venv_dir ]; then
    rm -rf $venv_dir
    echo "Deleted .venv directory."
  fi
}

# Function to create the dist directory if it doesn't exist
create_dist_directory() {
  local dist_dir=$1
  mkdir -p $dist_dir
  echo "Created directory $dist_dir."
}

# Function to remove any existing zip file in the dist directory
remove_existing_zip() {
  local zip_path=$1
  rm -f $zip_path
  echo "Removed existing zip file $zip_path."
}

# Function to create a new zip file including the necessary directories and files
create_zip_file() {
  local zip_path=$1
  zip -r $zip_path lib
  echo "Created $zip_path successfully."
}

# Function to build the wheel file using poetry
build_wheel() {
  poetry build --format wheel
  echo "Wheel file created in the dist directory."
}

# Function to upload files to S3
upload_to_s3() {
  local file_path=$1
  local s3_key=$2
  local extra_args=${3:-}
  aws s3 cp "$file_path" "$s3_key" --profile $AWS_PROFILE $extra_args
  if [ $? -ne 0 ]; then
    echo "Failed to upload $file_path to $s3_key. Exiting."
    exit 1
  fi
  echo "Uploaded $file_path to $s3_key successfully."
}

# Function to determine the S3 bucket for glue projects based on AWS account ID
determine_glue_s3_bucket() {
  local account_id=$(aws sts get-caller-identity --query "Account" --output text --profile $AWS_PROFILE)
  case $account_id in
    143049391535)
      S3_BUCKET="s3://baxaws-dev-enterpriseanalytics-edh-jde-inbound/glue_scripts"
      ;;
    737965399985)
      S3_BUCKET="s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/glue_scripts"
      ;;
    203058073716)
      S3_BUCKET="s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/glue_scripts"
      ;;
    *)
      echo "Unknown account ID: $account_id. Exiting."
      exit 1
      ;;
  esac
}

# Function to determine the S3 bucket for ETL projects based on AWS account ID
determine_etllib_s3_bucket() {
  local account_id=$(aws sts get-caller-identity --query "Account" --output text --profile $AWS_PROFILE)
  case $account_id in
    143049391535)
      S3_BUCKET="s3://baxaws-dev-enterpriseanalytics-edh-jde-inbound/scripts/edh_pipelib"
      ;;
    737965399985)
      S3_BUCKET="s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/scripts/edh_pipelib"
      ;;
    203058073716)
      S3_BUCKET="s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/scripts/edh_pipelib"
      ;;
    *)
      echo "Unknown account ID: $account_id. Exiting."
      exit 1
      ;;
  esac
}

# Function to determine the S3 bucket for ITA jobs based on AWS account ID
determine_ita_jobs_s3_bucket() {
  local account_id=$(aws sts get-caller-identity --query "Account" --output text --profile $AWS_PROFILE)
  case $account_id in
    143049391535)
      S3_BUCKET="s3://baxaws-dev-enterpriseanalytics-edh-ita-inbound/scripts/edh_pipelib"
      ;;
    737965399985)
      S3_BUCKET="s3://baxaws-prd-enterpriseanalytics-edh-ita-inbound/scripts/edh_pipelib"
      ;;
    203058073716)
      S3_BUCKET="s3://baxaws-tst-enterpriseanalytics-edh-ita-inbound/scripts/edh_pipelib"
      ;;
    *)
      echo "Unknown account ID: $account_id. Exiting."
      exit 1
      ;;
  esac
}

# Function to upload the jobs folder to S3 while maintaining the structure
upload_jobs_to_s3() {
  local s3_key_base=$1
  local jobs_s3_key="${s3_key_base}/jobs/"
  aws s3 cp lib/jobs/ "$jobs_s3_key" --recursive --profile $AWS_PROFILE
  if [ $? -ne 0 ]; then
    echo "Failed to upload jobs folder to $jobs_s3_key. Exiting."
    exit 1
  fi
  echo "Uploaded jobs folder to $jobs_s3_key successfully."
}

# Function to upload the dist directory to S3 with versioning
upload_dist_to_s3() {
  local dist_dir=$1
  local s3_key_base=$2
  local s3_key="${s3_key_base}/dist/"

  echo "Uploading dist directory to S3 with versioning..."
  aws s3 cp "$dist_dir" "$s3_key" --recursive --profile $AWS_PROFILE
  if [ $? -ne 0 ]; then
    echo "Failed to upload dist directory to $s3_key. Exiting."
    exit 1
  fi
  echo "Uploaded dist directory to ${s3_key} successfully."
}

# Function to upload the configs directory to S3 with versioning
upload_configs_to_s3() {
  local s3_key_base=$1
  local s3_key="${s3_key_base}/configs/"

  echo "Uploading configs directory to S3 with versioning..."
  aws s3 cp "configs" "$s3_key" --recursive --profile $AWS_PROFILE
  if [ $? -ne 0 ]; then
    echo "Failed to upload configs directory to $s3_key. Exiting."
    exit 1
  fi
  echo "Uploaded configs directory to ${s3_key} successfully."
}

# Function to upload the jars directory to S3 with versioning
upload_jars_to_s3() {
  local s3_key_base=$1
  local s3_key="${s3_key_base}/jars/"

  echo "Uploading jars directory to S3 with versioning..."
  aws s3 cp "jars" "$s3_key" --recursive --profile $AWS_PROFILE
  if [ $? -ne 0 ]; then
    echo "Failed to upload jars directory to $s3_key. Exiting."
    exit 1
  fi
  echo "Uploaded jars directory to ${s3_key} successfully."
}

# Function to upload the lib directory to S3 with versioning
upload_lib_to_s3() {
  local s3_key_base=$1
  local s3_key="${s3_key_base}/lib/"

  echo "Uploading lib directory to S3 with versioning..."
  aws s3 cp "lib" "$s3_key" --recursive --profile $AWS_PROFILE
  if [ $? -ne 0 ]; then
    echo "Failed to upload lib directory to $s3_key. Exiting."
    exit 1
  fi
  echo "Uploaded lib directory to ${s3_key} successfully."
}

# Function to upload the specific directories for ITA jobs
upload_ita_jobs_to_s3() {
  local s3_key_base=$1

  # Upload only the wheel file from dist
  local wheel_file=$(ls dist/*.whl)
  if [ -z "$wheel_file" ]; then
    echo "Wheel file not found. Exiting."
    exit 1
  fi
  upload_to_s3 "$wheel_file" "${s3_key_base}/dist/"

  echo "$wheel_file"
  local spm_wheel_file=$(echo "$wheel_file" | sed 's/edh/spm/')
  
  cp "$wheel_file" "$spm_wheel_file"
  if [ $? -ne 0 ]; then
    echo "Failed to create a copy of $edh_wheel_file as $spm_wheel_file. Exiting."
    exit 1
  fi
  echo "Created a copy of the wheel file as $spm_wheel_file."

  # Upload the spm wheel file
  upload_to_s3 "$spm_wheel_file" "${s3_key_base}/dist/"
  echo "Uploaded $spm_wheel_file successfully."

  

  # Upload only the required lib files inside data_ingestion/gensight
  echo "Uploading lib/data_ingestion/gensight to S3..."
  aws s3 cp "lib/data_ingestion/gensight" "${s3_key_base}/lib/data_ingestion/gensight/" --recursive --profile $AWS_PROFILE
  if [ $? -ne 0 ]; then
    echo "Failed to upload lib/data_ingestion/gensight to ${s3_key_base}/lib/data_ingestion/gensight/. Exiting."
    exit 1
  fi
  echo "Uploaded lib/data_ingestion/gensight to ${s3_key_base}/lib/data_ingestion/gensight/ successfully."

  # Upload all YAML files in the config directory
  echo "Uploading all YAML files from lib/data_ingestion/gensight/config to S3..."
  aws s3 cp "lib/data_ingestion/gensight/config" "${s3_key_base}/lib/data_ingestion/gensight/config/" --recursive --exclude "*" --include "*.yaml" --profile $AWS_PROFILE
  if [ $? -ne 0 ]; then
    echo "Failed to upload YAML files to ${s3_key_base}/lib/data_ingestion/gensight/config/. Exiting."
    exit 1
  fi
  echo "Uploaded all YAML files to ${s3_key_base}/lib/data_ingestion/gensight/config/ successfully."


  # Upload only the required lib files inside data_ingestion/gensight
  echo "Uploading lib/data_ingestion/spm to S3..."
  aws s3 cp "lib/data_ingestion/spm" "${s3_key_base}/lib/data_ingestion/spm/" --recursive --profile $AWS_PROFILE
  if [ $? -ne 0 ]; then
    echo "Failed to upload lib/data_ingestion/gensight to ${s3_key_base}/lib/data_ingestion/spm/. Exiting."
    exit 1
  fi
  echo "Uploaded lib/data_ingestion/spm to ${s3_key_base}/lib/data_ingestion/spm/ successfully."

  # Upload all YAML files in the config directory
  echo "Uploading all YAML files from lib/data_ingestion/spm/config to S3..."
  aws s3 cp "lib/data_ingestion/spm/config" "${s3_key_base}/lib/data_ingestion/spm/config/" --recursive --exclude "*" --include "*.yaml" --profile $AWS_PROFILE
  if [ $? -ne 0 ]; then
    echo "Failed to upload YAML files to ${s3_key_base}/lib/data_ingestion/spm/config/. Exiting."
    exit 1
  fi
  echo "Uploaded all YAML files to ${s3_key_base}/lib/data_ingestion/spm/config/ successfully."


}

# Main function to orchestrate the deployment for ITA jobs with selective uploads
deploy_ita_jobs() {
  delete_venv_and_dist
  determine_ita_jobs_s3_bucket
  local branch_name=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "no-branch")
  local s3_key_base="$S3_BUCKET/head-snapshot/$branch_name"

  # Ensure that the dist directory exists
  create_dist_directory "dist"

  # Build the wheel file using poetry (no zip file needed)
  build_wheel

  # Upload specific directories/files to S3
  upload_ita_jobs_to_s3 "$s3_key_base"
}

# Main function to orchestrate the deployment for glue projects
deploy_glue() {
  determine_glue_s3_bucket

  local dist_dir="dist"
  local zip_name="edh_pipelib.zip"
  local zip_path="$dist_dir/$zip_name"
  local BIN_S3_KEY="$S3_BUCKET/bin/"
  local CONFIGS_S3_KEY="$S3_BUCKET/configs/"
  local JARS_S3_KEY="$S3_BUCKET/jars/"
  local JOBS_S3_KEY="$S3_BUCKET/jobs/"

  # Delete .venv directory
  delete_venv

  # Create dist directory
  create_dist_directory $dist_dir

  # Remove existing zip file
  remove_existing_zip $zip_path

  # Create a new zip file
  create_zip_file $zip_path

  # Build the wheel file
  build_wheel

  # Find the built wheel file
  local wheel_file=$(ls dist/*.whl)

  # Check if the wheel file exists
  if [ -z "$wheel_file" ]; then
    echo "Wheel file not found. Exiting."
    exit 1
  fi

  # Upload the wheel file to S3
  upload_to_s3 "$wheel_file" "$BIN_S3_KEY"

  # Upload the zip file to S3
  upload_to_s3 "$zip_path" "$BIN_S3_KEY"

  # Upload the configs directory to S3
  upload_configs_to_s3 "$S3_BUCKET"

  # Upload the jars directory to S3
  upload_jars_to_s3 "$S3_BUCKET"

  # Upload the jobs folder to S3
  upload_jobs_to_s3 "$S3_BUCKET"
}

# Main function to orchestrate the deployment for head-snapshot with versioning and branches
deploy_head_snapshot() {
  determine_etllib_s3_bucket
  local branch_name=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "no-branch")
  local s3_key_base="$S3_BUCKET/head-snapshot/$branch_name"

  # Ensure that the dist directory exists
  create_dist_directory "dist"

  # Remove any existing zip file
  remove_existing_zip "dist/edh_pipelib.zip"

  # Create a new zip file including necessary directories and files
  create_zip_file "dist/edh_pipelib.zip"

  # Build the wheel file using poetry
  build_wheel

  # Upload the dist directory to S3
  upload_dist_to_s3 "dist" "$s3_key_base"

  # Upload the configs directory to S3
  upload_configs_to_s3 "$s3_key_base"

  # Upload the jars directory to S3
  upload_jars_to_s3 "$s3_key_base"

  # Upload the jobs folder to S3
  upload_jobs_to_s3 "$s3_key_base"

  # Upload the lib directory to S3
  upload_lib_to_s3 "$s3_key_base"
}

# Main function to orchestrate the deployment for ETL projects
deploy_etllib() {
  determine_etllib_s3_bucket

  local dist_dir="dist"
  local zip_name="edh_pipelib.zip"
  local zip_path="$dist_dir/$zip_name"
  local BIN_S3_KEY="$S3_BUCKET/bin/"
  local CONFIGS_S3_KEY="$S3_BUCKET/configs/"
  local JARS_S3_KEY="$S3_BUCKET/jars/"
  local JOBS_S3_KEY="$S3_BUCKET/jobs/"

  # Delete .venv directory
  delete_venv

  # Create dist directory
  create_dist_directory $dist_dir

  # Remove existing zip file
  remove_existing_zip $zip_path

  # Create a new zip file
  create_zip_file $zip_path

  # Build the wheel file
  build_wheel

  # Find the built wheel file
  local wheel_file=$(ls dist/*.whl)

  # Check if the wheel file exists
  if [ -z "$wheel_file" ]; then
    echo "Wheel file not found. Exiting."
    exit 1
  fi

  # Upload the wheel file to S3
  upload_to_s3 "$wheel_file" "$BIN_S3_KEY"

  # Upload the zip file to S3
  upload_to_s3 "$zip_path" "$BIN_S3_KEY"

  # Upload the configs directory to S3
  upload_configs_to_s3 "$S3_BUCKET"

  # Upload the jars directory to S3
  upload_jars_to_s3 "$S3_BUCKET"

  # Upload the jobs folder to S3
  upload_jobs_to_s3 "$S3_BUCKET"
}

# Check for arguments to decide which deployment to run
if [ -z "$1" ]; then
  deploy_glue
  deploy_head_snapshot
  deploy_ita_jobs
elif [ "$1" == "glue" ]; then
  deploy_glue
elif [ "$1" == "etllib" ]; then
  deploy_etllib
elif [ "$1" == "head-snapshot" ]; then
  deploy_head_snapshot
elif [ "$1" == "ita_jobs" ]; then
  deploy_ita_jobs
else
  echo "Usage: $0 {glue|etllib|head-snapshot|ita_jobs}"
  exit 1
fi
