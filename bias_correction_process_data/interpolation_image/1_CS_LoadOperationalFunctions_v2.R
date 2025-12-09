# ============================================================================ #
#                 I-CISK project - Spanish Living Lab                          #
# ============================================================================ #
# Script name     :  1_CS_LOadOperationalFunctions_v2.R
# Description     : Functions required for the automatic generation of 
#                   Seasonal Forecast models in Climate Service 1. 
# Author          : Amanda Batlle-Morera (a.batlle@creaf.uab.cat)
# Organization    : CREAF
# Date created    : 25-03-2025
# Last modified   : 23-07-2025
# R version       : R-4.5.1
# ============================================================================= #


#### Install packages if required ______________________________________________________________________________####
#.libPaths("/usr/local/lib/R/site-library")
#requiredPackages <- c( "ncdf4", "lubridate", "dplyr", "tidyr", "gstat", "stars", "sf")
# Check and install only missing packages

#install.packages(setdiff(requiredPackages, rownames(installed.packages())), dependencies = TRUE)
# Load libraries
#lapply(requiredPackages, library, character.only = TRUE)

library(ncdf4)
library(lubridate)
library(dplyr)
library(tidyr)
library(gstat)
library(stars)
library(sf)


# Function: READING FORECAST FILES __________________________________________####
  # Description: 
  #   Function reads a NetCDF file and converts it to df object with monthly values. 
  # Inputs: 
  #         - file: path to the NetCDF file. 
  #         - variable: Accepts"tp_adj" accumulated for precipitation, or "t2m_avg_adj" for mean temperature. 
  # Output: df object with the content of the NetCDF file
  # NOTE: This function resolve converts daily accumulated precipitation since the beginning of the year to daily precipitation. And converts negative precipitation to 0 values. 


  #Required libraries:
  # library(ncdf4)
  # library(lubridate)
  # library(dplyr)
  # library(tidyr)

  read_NetCDF <- function(file, variable) {
    # Open file
    nc_ds <- nc_open(file)
    
    print(nc_ds)
    
    # Variable to write
    dname <- variable  # daily precipitation
    
    # Extract Point ID
    if (variable=="tp_adj") {
      dim_pointID<- as.vector(c(1:651)) # Generating my Point ID, It doesn't read this dimension
    }else if (variable=="t2m_avg_adj") {
      dim_pointID<- as.vector(c(1:215))  # Generating my Point ID, It doesn't read this dimension
    }
    
    n_pointID <- length(dim_pointID) 
    
    # Extract Time
    dim_time <- ncvar_get(nc_ds, "time")
    n_time <- length(dim_time)
    
    # Time conversion for Time units days since ....
    t_units <- ncatt_get(nc_ds, "time", "units")
    t_ustr <- strsplit(t_units$value, " ")
    t_dstr <- strsplit(unlist(t_ustr)[3], "-")
    date <- ymd(t_dstr) + ddays(dim_time)
    MMYYYY <- format(date, "%d-%m-%Y")
    
    
    # Extract Coordinates lat/Long
    lat <- ncvar_get(nc_ds, "lat", collapse_degen=FALSE)
    latitude<- as.vector(lat)
    
    lon <- ncvar_get(nc_ds, "lon", collapse_degen=FALSE)
    longitude <- as.vector(lon)
    
    # Extract variable   
    var_array <- ncvar_get(nc_ds,dname)
    dlname <- ncatt_get(nc_ds,dname,"long_name")
    dunits <- ncatt_get(nc_ds,dname,"units")
    fillvalue <- ncatt_get(nc_ds,dname,"_FillValue")
    
    # PointID with Coordinates 
    pointID_df <- data.frame(cbind(dim_pointID, latitude, longitude)) 
    
    #get a single slice or layer per day
    daylist <- c(1:n_time) # Month selection for the prediction month and the following 7 months. In that case is a February prediction.
    memberlist <- c(1:51)

    var_df_members <- data.frame() # Create the base dataframe to store data
    for (member in memberlist)  {
      # df for daily data extraction
      var_df<-pointID_df
      var_df$member <- member
      ColumnNames<- c("Station", "Latitude", "Longitude", "member")
      for (day in daylist) {
        # Variable dimensions [pointID, time, member]
        var_slice_day <- var_array[,day, member]
        var_slice_day
        
        var_slice_day_vec <- as.vector(var_slice_day)
        length(var_slice_day_vec)
        #Building data frame
        var_df <- cbind(var_df,var_slice_day_vec)
        
        date <- MMYYYY[day] #Convert month to 2 digits
        
        # Add date to dataframe header. 
        ColumnNames<- c(ColumnNames, date)
        # Change column names using colnames()
        colnames(var_df) <- ColumnNames
      }
      
      var_df_members <- rbind(var_df_members, var_df)
    }
    # Close NetCDF file
    nc_close(nc_ds)
    
    if (debugmode) {
      # Export the subset to a CSV file
      write.csv(var_df_members, paste0("FileProcessing_debugmode/", variable,"_dfmembers_", period, ".csv"), row.names = FALSE)
    }
    
    # Change dataframe structure. 
    var_df_members_long<- var_df_members %>%
      pivot_longer(cols = 5:ncol(var_df_members),      # Select columns to pivot
                   names_to = "DATE", # New column name for pivoted column names
                   values_to = variable   # New column for the values
                   )

  if (variable=="tp_adj") {
    # Convert cumulative precipitation to daily values
    var_df_members_long<- var_df_members_long %>%
      group_by(Station, member)%>%
      arrange(date) %>%  # ensure order by time
      mutate(daily_precip = tp_adj - lag(tp_adj, default = 0)) %>%
      mutate( # Convert negative values to 0
        was_negative = daily_precip < 0,
        daily_precip = ifelse(daily_precip < 0, 0, daily_precip)) %>%
      ungroup()
        
        if (debugmode) {
          # Export the subset to a CSV file
          write.csv(var_df_members_long, paste0("FileProcessing_debugmode/Precipitation_DataConversion_dfmembers_", period, ".csv"), row.names = FALSE)
        }
        
    
    var_df_members_month <- var_df_members_long %>%
      select (-c("tp_adj", "was_negative")) %>%
      mutate(DATE = as.Date(DATE, format = "%d-%m-%Y")) %>%  # Convert DATE to Date type 
      mutate(MMYYYY = format(DATE, "%m-%Y"))%>%  # Extract the month from DATE
      group_by(Station, Latitude, Longitude, member, MMYYYY) %>% # Group by station, Lat, Long, member and month
      summarize(monthly_pr = sum(daily_precip, na.rm = TRUE), .groups="drop") # Summarize to calculate the monthly sum of precipitation

  } else if (variable=="t2m_avg_adj") {
    var_df_members_month <- var_df_members_long %>%
      mutate(DATE = as.Date(DATE, format = "%d-%m-%Y")) %>%  # Convert DATE to Date type 
      mutate(MMYYYY = format(DATE, "%m-%Y"))%>%  # Extract the month from DATE
      group_by(Station, Latitude, Longitude, member, MMYYYY) %>% # Group by station, Lat, Long, member and month
      summarize(monthly_tas = mean(t2m_avg_adj, na.rm = TRUE), .groups="drop") # Summarize to calculate the monthly mean of temperature
  } 
    
    return(var_df_members_month)
    
  }
  
# Function: RegMultR  __________________________________________####
  # Description: 
  #   Function performs a Multiple Linear Regression stepwise with lm function. 
  #   Followed by a residual spatial interpolation using IWD (exponent 3 for precipitation, and 1.75 for temperature).
  #   Residual spatial interpolation is subtracted to the model lm result. 
  #   Process is executed for each month of the forecast. 
  # Inputs: 
  #         - dependent_var_df : dataframe containg the monthly perecentiles of the variable for each station.
  #         - variable: Available options: "pr" accumulated for precipitation, "tas" for mean temperature. 
  #         - dependent_var: percentile to generate model from. Available option: "pc_05", "pc_10", "pc_25", "pc_50", "pc_75", "pc_90", "pc_95" 
  #         - indepevar_path: Path to the folder containg: 
  #                   - ALT.tif: Altitude raster. [required for "pr" and "tas"] 
  #                   - 1mCOSASPECT.tif: Terrain orientation raster. [required for "pr" and "tas"] 
  #                   - DIST_ATL_QU.tif: Quadratic Distance to the Atlantic raster. [required for "pr" and "tas"] 
  #                   - DIST-MED_QU.tif: Quadratic Distance to the Mediterranean raster.[required for "pr" and "tas"] 
  #                   - TWI.tif: Topographic Wetness Index raster. [required for "tas"] 
  #                   - RAD_m.tif: Radiation rasters for each month "m". [required for "tas"] 
  #                   - Guadalquivir_ROI.shp: Polygon of the Region Of Interest (ROI). [required for "pr" and "tas"] 
  #         - start-month: Inicialisation forecast month in format "MM-YYYY" [required for "tas"] 
  #         - OutputDirectory : Path to the directory where to store outputs.
  #         - debugmode: Debug mode activated TRUE, or deactivated FALSE.
  # Output: Variable continuous model for each month of the seasonal forecast and each of the member percentiles [format TIF]
 
  # Required libraries RegMultR
  #  library (stars) # Raster data
  #  library(gstat) # interpolation models
  #  library(sf) # Vector data
  #  library(dplyr) # table management
  
  # Set environmental variable for consistent CRS behavior.
  Sys.setenv(GTIFF_SRS_SOURCE = "EPSG")
  
RegMultR <- function(dependent_var_df, variable,  dependent_var, indepvar_path, start_month_year, OutputDirectory, debugmode) {
  # Define Region of Interest
  borders <- st_read(paste0(indepvar_path, "/Spanish_LL_ROI.shp"), crs = 25830)
  
  # Extract forecast month and year
  parts <- strsplit(start_month_year, "-")[[1]]
  start_month <- as.numeric(parts[2])
  start_year <- as.numeric(parts[1])

  # Independent variables
  if (variable=="pr") {
    # Load Independent variables with stars package
      dem_raster <- read_stars(paste0(indepvar_path, "/ALT.tif"))
      cosaps_raster <- read_stars(paste0(indepvar_path, "/1mCOS_ASPECT.tif"))
      distatl_raster <- read_stars(paste0(indepvar_path, "/DIST_ATL_QU.tif"))
      distmed_raster <- read_stars(paste0(indepvar_path, "/DIST_MED_QU.tif"))

    # Stacking rasters:Combine rasters into a multilayer SpatRaster
    iv_raster_stack <- c(dem_raster, cosaps_raster, distatl_raster, distmed_raster)
  } else if (variable=="tas") {
    # Load Independent variables with stars package
    dem_raster <- read_stars(paste0(indepvar_path, "/ALT.tif"))
    cosaps_raster <- read_stars(paste0(indepvar_path, "/1mCOS_ASPECT.tif"))
    distatl_raster <- read_stars(paste0(indepvar_path, "/DIST_ATL_QU.tif"))
    distmed_raster <- read_stars(paste0(indepvar_path, "/DIST_MED_QU.tif"))
    TWI_raster <- read_stars(paste0(indepvar_path, "/TWI.tif"))
    # Monthly Radiation
    # Generate month list
    start_month <- as.numeric(start_month) # Ensure start_month is numeric
    months <- (start_month:(start_month + 5)) %% 12 # Generate the next 5 months, wrapping around after 12
    months[months == 0] <- 12  # Replace 0 with 12
    months <- sprintf("%02d", months) # Format months as two-digit characters

    # Read and stack all radiation rasters in one line
    rad_stack <- do.call(c, lapply(months, function(m) {
      read_stars(paste0(indepvar_path, "/RAD_", m, ".tif"))
    }))

    # Combine with other rasters
    iv_raster_stack <- c(dem_raster, cosaps_raster, distatl_raster, distmed_raster,TWI_raster, rad_stack)
  }

  #Convert to dependent variable dataframe in a sf object
  dv_sf <- st_as_sf(dependent_var_df, coords = c('Longitude', 'Latitude'), crs = 4326)
  dv_sf <- st_transform(dv_sf, crs = 25830 )

  # Extract values for all layers at once
  iv_dv_data <- st_extract(iv_raster_stack, dv_sf, bilinear = TRUE)
  # Replace "termination.tiff" in all column names
  colnames(iv_dv_data) <- gsub(".tif", "", colnames(iv_dv_data))

  # Combine with original sf object
  iv_dv_data  <- cbind(dv_sf, iv_dv_data )
  iv_dv_data <- iv_dv_data %>%
    select(-geometry.1)%>%
    filter(if_all(where(is.numeric), ~ !is.nan(.))) #Remove rows with any any NaN values
  #Get forecast period
      # Generate a sequence of 6 months
      start_date <- as.Date(paste0(start_year, "-", start_month, "-01"))
      forecast_period <- seq.Date(start_date, by = "month", length.out = 6)

      # Format back to "MM_YYYY"
      forecast_period <- format(forecast_period, "%m-%Y")


  # RegMultR:

  # Create an empty dataframe to store results
  RegMultR_results <- data.frame()

  for (month in forecast_period) {
    # Filter months data
    forecast_month <- iv_dv_data %>%
      filter(MMYYYY == month)

    # MULTIPLE LINEAR REGRESSION:
    if (variable=="pr") {
      # Used a multiple regression analysis using the backward stepwise method for choosing the independent variables included in the model.
      formula <- as.formula(paste(dependent_var, "~ ALT + X1mCOS_ASPECT + DIST_ATL_QU + DIST_MED_QU"))
    } else if (variable=="tas") {
      # Extract the month part
      m <- as.character(substr(month, 1, 2))  # Get first two characters (month)

      # Used a multiple regression analysis using the backward stepwise method for choosing the independent variables included in the model.
      formula <- as.formula(paste0(dependent_var, " ~ ALT + X1mCOS_ASPECT + DIST_ATL_QU + DIST_MED_QU + TWI + RAD_", m))
    }

    model <- lm(formula, data=forecast_month)
    # Stepwise regression (both directions)
    stepwise_model <- step(model, direction = "both")
    # Extract residual
    # Generate dynamic column names
    pred_col_name <- paste0("Model_", dependent_var)
    resd_col_name <- paste0("Model_resd_", dependent_var)

    # Store predicted values
    forecast_month[[pred_col_name]] <- predict(stepwise_model, newdata = forecast_month)

    if (variable=="pr") {
      forecast_month[[pred_col_name]] <- ifelse(forecast_month[[pred_col_name]] < 0, 0, forecast_month[[pred_col_name]]) # Convert negative values to 0. Do not tolerate negative precipitation values.
    }

    # Store residuals
    forecast_month[[resd_col_name]] <- forecast_month[[pred_col_name]] - forecast_month[[dependent_var]]

    # Append results to iv_dv_data_results
    RegMultR_results <- rbind(RegMultR_results, forecast_month)
    
    if (debugmode) {
      # Write a report in txt:
      file_path <- paste0("FileProcessing_debugmode/LMstepwise_", variable , "_model_summary_",month,"_", dependent_var, ".txt")  # Prepare a file path for your text file
      sink(file_path)  # Open the connection to the text file using sink()
      cat("Regression Model Summary:\n\n") # Write model summary to file
      cat(capture.output(summary(model)), sep = "\n")
      cat("Stepwise Regression Model Summary:\n\n") # Write model summary to file
      cat(capture.output(summary(stepwise_model)), sep = "\n")
      sink() # Close the connection
    }

    # Extract regression coefficients from the model
    coefficients <- coef(stepwise_model)

    if (variable=="pr") {
      #GENERATING PRECIPITATION MODEL
      # Assign coefficients to variables
      intercept <- as.numeric(coefficients["(Intercept)"])
      coef_ALT <- as.numeric(coefficients["ALT"])
      coef_X1mCOS_ASPECT <- as.numeric(coefficients["X1mCOS_ASPECT"])
      coef_DIST_ATL <- as.numeric(coefficients["DIST_ATL_QU"])
      coef_DIST_MED <- as.numeric(coefficients["DIST_MED_QU"])

      # Check if coefficients are NA (variable not includede i the stepwise model)
      if (is.na(coef_ALT)) { coef_ALT <- 0 }
      if (is.na(coef_X1mCOS_ASPECT)) { coef_X1mCOS_ASPECT <- 0}
      if (is.na(coef_DIST_ATL)) { coef_DIST_ATL <- 0}
      if (is.na(coef_DIST_MED)) { coef_DIST_MED <- 0}
      # Apply the formula
      predicted_raster <- intercept +
        (coef_ALT * dem_raster ) +
        (coef_X1mCOS_ASPECT * cosaps_raster ) +
        (coef_DIST_ATL * distatl_raster) +
        (coef_DIST_MED * distmed_raster)

      names(predicted_raster) <- "regression"

      predicted_raster <- predicted_raster[borders] # Clip Raster to the borders
      predicted_raster[predicted_raster < 0] <- 0 # Trim all negative precipitation values to 0. Do not tolerate negative precipitation values.
      # Save the raster
      #write_stars(predicted_raster, paste0(OutputDirectory ,"/PLforecast_", period, "_", month,"_", dependent_var, ".tif"))
    } else if (variable=="tas") {
      # GENERATING TEMPERATURE MODEL
      # Assign coefficients to variables
      intercept <- as.numeric(coefficients["(Intercept)"])
      coef_ALT <- as.numeric(coefficients["ALT"])
      coef_X1mCOS_ASPECT <- as.numeric(coefficients["X1mCOS_ASPECT"])
      coef_DIST_ATL <- as.numeric(coefficients["DIST_ATL_QU"])
      coef_DIST_MED <- as.numeric(coefficients["DIST_MED_QU"])
      coef_TWI <- as.numeric(coefficients["TWI"])
      coef_RAD <- as.numeric(coefficients[paste0("RAD_", m)])

      # Check if coefficients are NA (variable not includede i the stepwise model)
      if (is.na(coef_ALT)) { coef_ALT <- 0 }
      if (is.na(coef_X1mCOS_ASPECT)) { coef_X1mCOS_ASPECT <- 0}
      if (is.na(coef_DIST_ATL)) { coef_DIST_ATL <- 0}
      if (is.na(coef_DIST_MED)) { coef_DIST_MED <- 0}
      if (is.na(coef_TWI)) { coef_TWI <- 0}
      if (is.na(coef_RAD)) { coef_RAD <- 0}
      # Apply the formula
      #RADIATION raster
      rad_name <- paste0("RAD_", m, ".tif") # Define the RADIATION raster name dynamically
      rad_raster <- rad_stack[rad_name] # Extract the corresponding raster from the stack

      predicted_raster <- intercept +
        (coef_ALT * dem_raster ) +
        (coef_X1mCOS_ASPECT * cosaps_raster ) +
        (coef_DIST_ATL * distatl_raster) +
        (coef_DIST_MED * distmed_raster) +
        (coef_TWI * TWI_raster) +
        (coef_RAD * rad_raster)

      names(predicted_raster) <- "regression"

      predicted_raster <- predicted_raster[borders] # Clip Raster to the borders
      # Save the raster
      # write_stars(predicted_raster, paste0(OutputDirectory ,"/TEMPforecast_", period, "_", month,"_", dependent_var, ".tif"))
    }

    # RESIDUALS INTERPOLATION: inverse distance weighted interpolation
    gstat_formula <- as.formula(paste(resd_col_name, "~ 1")) # Dynamically create the formula for gstat
    # Create a gstat object (is needed to interpolate)
      # Set IDW exponent
      if (variable=="pr") {exp <- 3}
      if (variable=="tas") {exp <- 1.75}
    g <- gstat(formula= gstat_formula, data =forecast_month, set=list(idp=exp)) # ~ 1 mean there is no independent variables
    residual_interpol <- predict(g, predicted_raster)
    residual_interpol <- residual_interpol['var1.pred',,]
    names(residual_interpol) = 'resd'

    # APPLY RESIDUAL INTERPOLATION TO THE MODEL ####
    residual_interpol_resampled <- st_warp(residual_interpol, predicted_raster) # Resample residual_interpol to match the extent and resolution of predicted_raster
    RegMult_R <- predicted_raster + residual_interpol_resampled # Apply residual interpolation to the lm model

    # EXPORT MODEL
    if (variable=="pr") {
      # Check for negative values and trim them to 0 if any are found
      RegMult_R[RegMult_R < 0] <- 0 # Trim all negative precipitation values to 0. Do not tolerate negative precipitation values.
      # Save the raster
      write_stars(predicted_raster, paste0(OutputDirectory ,"/PLforecast_", period, "_", month,"_", dependent_var, "_corrected.tif"))
    } else if (variable=="tas") {
      # Save the raster
      write_stars(predicted_raster, paste0(OutputDirectory ,"/TEMPforecast_", period, "_", month,"_", dependent_var, "_corrected.tif"))
    }

  }
  
  if (debugmode) {
    # Export the subset to a CSV file
    write.csv(RegMultR_results, paste0("FileProcessing_debugmode/RegMultR_", variable,"_results_", period, "_", dependent_var,".csv"), row.names = FALSE)
  }
  
  return(RegMultR_results)
}

# Function: Member Uncertainty  __________________________________________####
  # Description: 
  #   Function performs a IDW (exponent 3 for precipitation, and 1.75 for temperature) of the SD between all members.
  #   Followed by a reclassification using percentile 25 and 75 to determine low , mid and high divergence between members: 
  #           - LOW divergence: Values 1 for pixels < percentile 25 
  #           - MID divergence: Value 2 for pixels between percentile 25 and 75. 
  #           - HIGH divergence: Value 3 for pixels > percentile 75. 
  # Inputs: 
  #         - dependent_var_df : data frame containing the monthly standard deviation of the variable for each station.
  #         - variable: Available options: "pr" accumulated for precipitation, "tas" for mean temperature. 
  #         - indepevar_path: Path to the folder containg: 
  #                   - ALT.tif: Altitude raster. As reference grid.
  #                   - Guadalquivir_ROI.shp: Polygon of the Region Of Interest (ROI).
  #         - start-month: Inicialisation forecast month in format "MM-YYYY"
  #         - OutputDirectory : Path to the directory where to store outputs. 
  # Output: Member uncertainty for each month of the seasonal forecast [format TIF]

  # Required libraries: 
  #  library(gstat) # interpolation models
  #  library(stars) # read raster
  #  library(sf)
             
MemberUncert <- function (dependent_var_df, variable, indepvar_path, start_month_year, OutputDirectory,debugmode ) {
  # Convert Data sumaries into point data
  SD <- dependent_var_df %>%
    st_as_sf(coords = c('Longitude', 'Latitude'), crs = 4326) %>%
    st_transform(crs = 25830) %>%
    select(geometry, MMYYYY, all_of(variable))
  
  # Use DEM as a reference grid and area of interest to interpolate
  dem <- read_stars(paste0(indepvar_path, "/ALT.tif"))
  borders <- st_read (paste0(indepvar_path, "/Spanish_LL_ROI.shp"),crs = 25830)
  dem = dem[borders]
  
  # Extract forecast month and year
    parts <- strsplit(start_month_year, "-")[[1]]
    start_month <- as.numeric(parts[2])
    start_year <- as.numeric(parts[1])
    
    # Generate a sequence of 6 months
    start_date <- as.Date(paste0(start_year, "-", start_month, "-01"))
    forecast_period <- seq.Date(start_date, by = "month", length.out = 6)
    
    # Format back to "MM_YYYY"
    forecast_period <- format(forecast_period, "%m-%Y")
  
  # Set IDW exponent
  if (variable=="sd_monthly_pr") {exp <- 3.5}
  if (variable=="sd_monthly_tas") {exp <- 1.75}
  
  for (month in forecast_period) {
    # Subset the dataframe by MMYYYY
    SD_subset <- SD %>%
      filter(MMYYYY == month)
    
    #INTERPOLATION: inverse distance weighted interpolation
    # Create a gstat object (is needed to interpolate)
    formula <- as.formula(paste(variable , "~1"))
    g <- gstat(formula= formula, data = SD_subset, set=list(idp=exp)) # ~ 1 mean there is no independent variables
    z <- predict(g, dem)
    z = z['var1.pred',,]
    
    if (debugmode) {write_stars(z, paste0("FileProcessing_debugmode/PLforecast_", period, "_",month, "_memberSDinterpol.tif"))}
    
    # Reclassify results to high, mid, low sd value and export
    if (variable=="sd_monthly_pr") {
      names(z) = 'sd_precipitation'
      # Compute the 25th and 75th percentiles
      z_p25 <- quantile(z[[1]], probs = 0.25, na.rm = TRUE)
      z_p75 <- quantile(z[[1]], probs = 0.75, na.rm = TRUE)
      # Reclassify raster using cut() inside st_apply()
      r_classified <- st_apply(z, MARGIN = c("x", "y"), FUN = function(x) {
        ifelse(x < z_p25, 1, ifelse(x > z_p75, 3, 2))
      })
      # Export the stars object 'r_classified' to a TIFF file
      write_stars(r_classified, paste0(OutputDirectory, "/PLforecast_", period, "_",month, "_memberUNCERTAINTY.tif"))

    } else if (variable=="sd_monthly_tas"){
      names(z) = 'sd_temperature'
      # Compute the 25th and 75th percentiles
      z_p25 <- quantile(z[[1]], probs = 0.25, na.rm = TRUE)
      z_p75 <- quantile(z[[1]], probs = 0.75, na.rm = TRUE)
      # Reclassify raster using cut() inside st_apply()
      r_classified <- st_apply(z, MARGIN = c("x", "y"), FUN = function(x) {
        ifelse(x < z_p25, 1, ifelse(x > z_p75, 3, 2))
      })
      # Export the stars object 'r_classified' to a TIFF file
      write_stars(r_classified, paste0(OutputDirectory, "/TEMPforecast_", period, "_",month, "_memberUNCERTAINTY.tif"))
    }
  }
}
  
  
# Function: TaskReview  __________________________________________####
  # Description: 
  #   Function performs a check in the output directory to confirm that all the output files from this protocol exist. 
  # Inputs: 
  #         - start-month: Initialization forecast month in format "MM-YYYY" [required for "tas"] 
  #         - OutputDirectory : Path to the directory where to store outputs.
  # Output: Confirmation message in console. 

TaskReview <- function (start_month_year, OutputDirectory ) {

  # Define components for filenames
    # Extract forecast month and year
    parts <- strsplit(start_month_year, "-")[[1]]
    start_month <- as.numeric(parts[2])
    start_year <- as.numeric(parts[1])
    
    # Generate a sequence of 6 months
    start_date <- as.Date(paste0(start_year, "-", start_month, "-01"))
    forecast_period <- seq.Date(start_date, by = "month", length.out = 6)
    
    # Format back to "MM_YYYY"
    forecast_period <- format(forecast_period, "%m-%Y")
    
    #Other files name elements: 
    pc <- c("pc_05_corrected", "pc_10_corrected", "pc_25_corrected", "pc_50_corrected", "pc_75_corrected", "pc_90_corrected", "pc_95_corrected", "memberUNCERTAINTY")
    variables <- c("PL", "TEMP")
    
    # Generate list if expected file names using expand.grid()
    file_names <- apply(expand.grid(variables, forecast_period, pc), 1, function(x) {
      paste0(x[1], "forecast_", period, "_", x[2], "_", x[3], ".tif")
    })
  
  # Create full paths
  file_paths <- file.path(OutputDirectory, file_names)
  
  # Check existence
  files_exist <- file.exists(file_paths)
  # Output results
  if (all(files_exist)) {
    message("Task completed successfully!")
  } else {
    missing_files <- file_names[!files_exist]
    message("Missing files: ", paste(missing_files, collapse = ", "))
  }

}

