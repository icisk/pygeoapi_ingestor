# ============================================================================ #
#                 I-CISK project - Spanish Living Lab                          #
# ============================================================================ #
# Script name     : 2_CS_OperationWorkFlow.R
# Description     : Operation code to obtain Climate SErvice 1 Seasonal Forecast
#                   This code includes a workflow of 3 functions:
#                     - Function ReadNetCDF: Reads the NetCDF file of precipitation or temperature and converts it into a dataframe
#                     - Function RegMult_R: Generated a variable model in the Spanish Living Lab. Method is a multiple linear regression with residual interpolation (Ninyerola, M., Pons, X., Roure, J.M., 2000. A METHODOLOGICAL APPROACH OF CLIMATOLOGICAL MODELLING OF AIR TEMPERATURE AND PRECIPITATION THROUGH GIS TECHNIQUES. INTERNATIONAL JOURNAL OF CLIMATOLOGY Int. J. Climatol 20, 1823–1841).
#                     - Function uncertainty: Generates a map displaying the spation distribution of forecast uncertainty. Method is a IDW of the SD of all members. 
# Author          : Amanda Batlle-Morera (a.batlle@creaf.uab.cat)
# Organization    : CREAF
# Date created    : 20-03-2025
# Last modified   : 03-07-2025
# R version       : R-4.5.1
# ============================================================================= #
debugmode <- FALSE  # or FALSE when running normally
if (debugmode) {
  if (!dir.exists("FileProcessing_debugmode")) {dir.create("FileProcessing_debugmode")} # Create File processing  directory for debuging if does not exist
}
print('reset params')
#### Parameters to be reset by each user: ______________________________________________________________________________####
 
    # set working directory
    setwd("/app/")
    
    # Modelling inputs
    # Directory for Bias_corrected forecast input
    forecast_input <- "./"
    
    #Directory to store results
    results_dir <- "outputs"
    if (!dir.exists(results_dir)) {dir.create(results_dir)} # Create results directory if does not exist
    
    # Directory for auxiliary file (ROI; and independent variables)
    auxiliary_dir <- "auxiliary_files"
    

print('reading precip FC')
#### READING PRECIPITATION FORECAST variable pr_________________________________________________________________####

# Create variable with precipitation forecast file starting with "ECMWF_51__tp-bias_corrected_"
filelist <- list.files(path=forecast_input, pattern = "^ECMWF_51__tp-bias_corrected_", full.names=TRUE)
    
    if (length(filelist) != 1) {stop("Expected exactly one file, but found ", length(filelist))}
    file <- filelist[1]    

  # Forecats inicialisation month: 
  forecast_inicialisation <- sub(".*__(\\d{4}-\\d{2})\\.nc", "\\1", file)
  # Forescast period: 
    start_date <- ymd(paste0(forecast_inicialisation, "-01")) # Convert to Date (assume first day of the month)
    end_date <- start_date %m+% months(5) # Get end date (5 months after start = 6 months total)
    period <- paste0(format(start_date, "%Y%m"), "_", format(end_date, "%Y%m"))  # Format as "YYYYMM"
  
  var_df_monthly <- read_NetCDF(file, "tp_adj")

if (debugmode) {
  write.csv(var_df_monthly, file=paste0("FileProcessing_debugmode/pr_acc_monthly_members.csv"), row.names = FALSE)
}

#### Statistics across member by station and month _____________________________________________________________####

var_df_stats <- var_df_monthly %>%
  group_by(Station, Latitude, Longitude, MMYYYY) %>%
  summarize(
    mean_monthly_pr = mean(monthly_pr, na.rm = TRUE),
    sd_monthly_pr = sd(monthly_pr, na.rm = TRUE),
    pc_05 = quantile(monthly_pr, 0.05, na.rm = TRUE),
    pc_10 = quantile(monthly_pr, 0.10, na.rm = TRUE),
    pc_25 = quantile(monthly_pr, 0.25, na.rm = TRUE),
    pc_50 = quantile(monthly_pr, 0.50, na.rm = TRUE),
    pc_75 = quantile(monthly_pr, 0.75, na.rm = TRUE),
    pc_90 = quantile(monthly_pr, 0.90, na.rm = TRUE),
    pc_95 = quantile(monthly_pr, 0.95, na.rm = TRUE)
  ) %>%
  ungroup() 

if (debugmode) { 
  write.csv(var_df_stats, file=paste0("FileProcessing_debugmode/pr_acc_monthly_memberSTATS.csv"), row.names = FALSE)

  # Exporting CVS my month to run RegMult in MiraMon.
  # list of dates to export
  forecast_dates <- unique(var_df_stats$MMYYYY)  # Get unique MMYYYY values
  # Loop through each unique MMYYYY value
  for (date_value in forecast_dates) {
    # Subset the dataframe by MMYYYY
    subset_df <- var_df_stats %>%
      filter(MMYYYY == date_value)
    # get Month adn Year values
    split <- strsplit(date_value, split = "-")
    M <- split[[1]][1]
    Y <- split[[1]][2]
    
    # Export the subset to a CSV file
    write.csv(subset_df, paste0("FileProcessing_debugmode/pr_acc_monthly_memberSTATS_", period, "_", M, "_", Y ,".csv"), row.names = FALSE)
    
    # Print a message (optional) to confirm the export
    message(paste("Exported data for", date_value))
  }
}

print('multreg')
#### RegMultR for the percentiles in the forescast _____________________________________________________________####
pc <- c("pc_05", "pc_10", "pc_25", "pc_50", "pc_75", "pc_90", "pc_95")

for (p in pc) {
  RegMultR_outputs <- RegMultR(dependent_var_df = var_df_stats, 
                       variable = "pr", 
                       dependent_var = p, 
                       indepvar_path = auxiliary_dir, 
                       start_month_year = forecast_inicialisation, 
                       OutputDirectory = results_dir,
                       debugmode)
}


#### Member Spatial Uncertainty  _______________________________________________________________________________####
MemberUncert(dependent_var_df=var_df_stats, 
             variable="sd_monthly_pr", 
             indepvar_path=auxiliary_dir, 
             start_month_year=forecast_inicialisation, 
             OutputDirectory=results_dir,
             debugmode)

# ______________________________________________________________________________________________________________####
# ______________________________________________________________________________________________________________####
print('reading precip FC')
#### READING TEMPERATURE FORECAST variable tas__________________________________________________________________####

# Create variable with temperature forecast file starting with "ECMWF_51__tp-bias_corrected_"
filelist <- list.files(path=forecast_input, pattern = "^ECMWF_51__t2m-bias_corrected_", full.names=TRUE)
  if (length(filelist) != 1) {stop("Expected exactly one file, but found ", length(filelist))}
  file <- filelist[1]    
  
  # Forecats inicialisation month: 
  forecast_inicialisation <- sub(".*__(\\d{4}-\\d{2})\\.nc", "\\1", file)
  # Forescast period: 
  start_date <- ymd(paste0(forecast_inicialisation, "-01")) # Convert to Date (assume first day of the month)
  end_date <- start_date %m+% months(5) # Get end date (5 months after start = 6 months total)
  period <- paste0(format(start_date, "%Y%m"), "_", format(end_date, "%Y%m"))  # Format as "YYYYMM"
  
  var_df_monthly <- read_NetCDF(file, "t2m_avg_adj")

if (debugmode) {
  # Save as CSV
  write.csv(var_df_monthly, file=paste0("FileProcessing_debugmode/tas_mean_monthly_members.csv"), row.names = FALSE)
}

#### Statistics across member by station and month ____________________________________________________________ ####

var_df_stats <- var_df_monthly %>%
  group_by(Station, Latitude, Longitude, MMYYYY) %>%
  summarize(
    mean_monthly_tas = mean(monthly_tas, na.rm = TRUE),
    sd_monthly_tas = sd(monthly_tas, na.rm = TRUE),
    pc_05 = quantile(monthly_tas, 0.05, na.rm = TRUE),
    pc_10 = quantile(monthly_tas, 0.10, na.rm = TRUE),
    pc_25 = quantile(monthly_tas, 0.25, na.rm = TRUE),
    pc_50 = quantile(monthly_tas, 0.50, na.rm = TRUE),
    pc_75 = quantile(monthly_tas, 0.75, na.rm = TRUE),
    pc_90 = quantile(monthly_tas, 0.90, na.rm = TRUE),
    pc_95 = quantile(monthly_tas, 0.95, na.rm = TRUE)
  ) %>%
  ungroup() 

if (debugmode) {
  write.csv(var_df_stats, file=paste0("FileProcessing_debugmode/tas_mean_monthly_memberSTATS.csv"), row.names = FALSE)
  
  # Exporting CVS my month.
  # list of dates to export
  forecast_dates <- unique(var_df_stats$MMYYYY)  # Get unique MMYYYY values
  # Loop through each unique MMYYYY value
  for (date_value in forecast_dates) {
    # #For debugging
    # date_value <- forecast_dates[1]
    # Subset the dataframe by MMYYYY
    subset_df <- var_df_stats %>%
      filter(MMYYYY == date_value)
    # get Month adn Year values
    split <- strsplit(date_value, split = "-")
    M <- split[[1]][1]
    Y <- split[[1]][2]
    
    # Export the subset to a CSV file
    write.csv(subset_df, paste0("FileProcessing_debugmode/tas_mean_monthly_memberSTATS_", period, "_", M, "_", Y ,".csv"), row.names = FALSE)
    
    # Print a message (optional) to confirm the export
    message(paste("Exported data for", date_value))
  }
  
}

  

#### RegMultR for the percentiles in the forescast _____________________________________________________________####
  # Percentile to generate model: 
  pc <- c("pc_05", "pc_10", "pc_25", "pc_50", "pc_75", "pc_90", "pc_95")

for (p in pc) {
  RegMultR_outputs <- RegMultR(dependent_var_df=var_df_stats, 
                       variable="tas", 
                       dependent_var=p, 
                       indepvar_path=auxiliary_dir, 
                       start_month_year=forecast_inicialisation, 
                       OutputDirectory = results_dir,
                       debugmode )
 
}

print('MEMBERS')
#### Member Spatial Uncertainty  _______________________________________________________________________________####
MemberUncert(dependent_var_df=var_df_stats, 
             variable="sd_monthly_tas", 
             indepvar_path=auxiliary_dir, 
             start_month_year=forecast_inicialisation, 
             OutputDirectory=results_dir,
             debugmode)
# ______________________________________________________________________________________________________________####
#### TASK REVIEW _______________________________________________________________________________________________####
if (debugmode) {
  TaskReview (start_month_year= forecast_inicialisation,
              OutputDirectory = results_dir )
}


