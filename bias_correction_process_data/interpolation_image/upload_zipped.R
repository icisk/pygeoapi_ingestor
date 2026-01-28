check_collections <- function(metadata, collections_endpoint) {
  ds_to_process <- c()
  for (date in names(metadata)) {
    formateddate <- gsub("-", "_", date)
    url <- glue("{collections_endpoint}/creaf_forecast_temp_{formateddate}")
    res <- GET(url)
    if (res$status_code != 200) {
      ds_to_process <- c(ds_to_process, date)
    }
  }
  return(ds_to_process)
}

dss = check_collections(metadata, collections_endpoint)
ds_to_process <- sort(dss[1])
date_stamp <- gsub("-", "_", ds_to_process)


library(zip)

target_folder <- "/app/outputs"
var_keys <- list(temp = "TEMPforecast", precip = "PLforecast")

all_files <- list.files(target_folder, full.names = TRUE, recursive = TRUE)

for (var_name in names(var_keys)) {
  search_string <- var_keys[[var_name]]

  matching_files <- all_files[grepl(search_string, all_files)]

  if (length(matching_files) > 0) {
    output_name <- sprintf("/app/zip/seasonal_forecast_%s_%s.zip", var_name, date_stamp)

    zip::zip(
      zipfile = output_name,
      files = matching_files,
      mode = "cherry-pick"
    )

    file.remove(matching_files)
  }
}
#######################################
library(paws.storage)

access_key <- Sys.getenv("FSSPEC_S3_KEY")
secret_key <- Sys.getenv("FSSPEC_S3_SECRET")
endpoint <- Sys.getenv("FSSPEC_S3_ENDPOINT_URL")
region <- "eu-de"
bucket_name <- Sys.getenv("DEFAULT_BUCKET")
zip_dir <- "/app/zip/"
options(paws.log_level = 3L)
svc <- s3(
  config = list(
    credentials = list(
      creds = list(
        access_key_id = access_key,
        secret_access_key = secret_key
      )
    ),
    endpoint = endpoint,
    region = region,
    s3_force_path_style = TRUE
  )
)

zip_files <- list.files(path = zip_dir, pattern = "\\.zip$", full.names = TRUE)

for (file_path in zip_files) {
  file_name <- basename(file_path)
  s3_key <- paste0("tif/LL_Spain/forecast/", file_name)

  file_size <- file.info(file_path)$size
  file_raw <- readBin(file_path, "raw", n = file_size)

  upload_successful <- FALSE
    tryCatch({
      svc$put_object(
        Body = file_raw,
        Bucket = bucket_name,
        Key = s3_key
      )
      upload_successful <- TRUE

    }, error = function(e) {
      # This extracts the actual response from OTC
      message("FAILED to upload: ", file_name)
      message("Error Code: ", e$code)
      message("Error Message: ", e$message)
      # Print the full list of error details
      print(e)
    })
    if (upload_successful) {
    file.remove(file_path)
    message("Successfully uploaded and removed: ", file_name)
  }
}
