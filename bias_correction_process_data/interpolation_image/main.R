library(jsonlite)
library(httr)
library(glue)

meta_url <- Sys.getenv("META_URL")
collections_endpoint <- Sys.getenv("COL_ENDPOINT")
ds_to_process <- Sys.getenv("DS_TO_PROCESS")
metadata <- fromJSON(meta_url)

check_collections <- function(metadata, collections_endpoint) {
  ds_to_process <- c()
  for (date in names(metadata)) {
    url <- glue("{collections_endpoint}/creaf_forecast_t2m_{date}")
    res <- GET(url)
    if (res$status_code != 200) {
      ds_to_process <- c(ds_to_process, date)
    }
  }
  return(ds_to_process)
}

download_file <- function(url) {
destfile <- basename(url)
  resp <- GET(url, write_disk(destfile, overwrite = TRUE))
  if (status_code(resp) != 200) {
    stop(paste("Failed to download:", url))
  }
  destfile
}

t2m_url <- metadata[[ds_to_process]]$t2m
tp_url <- metadata[[ds_to_process]]$tp

print("downloading")
print(ds_to_process)
t2m_file <- download_file(t2m_url)
tp_file  <- download_file(tp_url)

print("start functions loading")
source("1_CS_LoadOperationalFunctions_v2.R")

print("doing the thing")
source("2_CS_OperationalWorkFlow.R")

#Sys.sleep(Inf)