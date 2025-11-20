# ------------------------------------------------------------------------------
# master_script.R
#
# Author: Filipe Ribeiro Ferreira
#
# This is the master script which slices the Orbis M&A dataset with the merger
# authority decision into roughly evenly sized data subsets and, for each batch, 
# executes the scripts which ultimately produce the cumulative abnormal returns.
#
# ------------------------------------------------------------------------------

run_all_scripts <- function() {
  
  # This function imports the Orbis M&A dataset, slices it into roughly evenly
  # sized batches and produces the abnormal returns for each one by running
  # the necessary scripts
  
  # setting the work directory
  setwd(Sys.getenv("working_dir"))
  
  # load libraries
  library(dplyr)
  library(lubridate)
  
  # load Orbis M&A dataset
  orbis_df <- readRDS("data/raw_data/orbis/with_merger_authority_decision/m&a_data_all_batches.RDS") %>%
    filter(!(is.na(Acquiror.ISIN.number) & is.na(Target.ISIN.number)))
  orbis_df <- orbis_df %>% mutate( Announced.date = as.Date(Announced.date, 
                                                            format = "%d/%m/%Y"),
                                   Rumour.date = as.Date(Rumour.date, 
                                                            format = "%d/%m/%Y"),
                                   )
  
  # we will need this to later loop through all the deals
  # nrows <<- orbis_df %>% distinct( Deal.Number ) %>% nrow()
  
  # also import these two datasets
  sp500_data <<- readRDS("data/raw_data/sp500_data.rds") %>% 
    filter( year(date)>1990 ) %>% mutate(row_num = row_number())
  
  competitors_txt_def <<- readRDS(file = "data/raw_data/hoberg_philips.rds" )
  
  # create variables to store the row number which will slice the original
  # orbis M&A dataset 
  n_min <<- 1
  n_max <<- 5000
  
  # we also import here the Hoberg-Philips dataset (which contains the rivals)
  # so we avoid loading it for every batch
  # competitors_txt_def <- read.table("data/raw_data/hoberg_philips.txt", header = TRUE, sep = "\t", stringsAsFactors = FALSE)
  
  # removing NAs in the competitors score variable and zero scores
  # competitors_txt_def <- competitors_txt_def %>%  filter(score!=0 & !is.na(score))
  
  # save as RDS file
  # saveRDS(competitors_txt_def, file = "data/raw_data/hoberg_philips.rds" )
  
  # competitors_txt_def <- readRDS("data/raw_data/hoberg_philips.rds" ) %>%  filter(score!=0 & !is.na(score))
  
  # and set counter to keep track of batch number
  k <<- 1
  
  while (n_max < nrow(orbis_df)) {
    subset_orbis_df <- orbis_df[n_min:n_max,]
    
    # save batch locally
    filepath1 <<- paste0("data/raw_data/orbis/with_merger_authority_decision/batch/",k,"/", k/k ,"_raw_slice.rds")
    saveRDS(subset_orbis_df, filepath1)
    
    # run each script in the correct order
    source("final_scripts/orbis_code/Estimate ARs/1_extract_cusips.R")
    source("final_scripts/orbis_code/Estimate ARs/2_append_acq_gvkey.R")
    source("final_scripts/orbis_code/Estimate ARs/3_append_tgt_gvkey.R")
    source("final_scripts/orbis_code/Estimate ARs/4_get_and_append_acq_rivals_id_data.R")
    source("final_scripts/orbis_code/Estimate ARs/5_get_and_append_tgt_rivals_id_data.R")
    source("final_scripts/orbis_code/Estimate ARs/6_merge_acq_and_tgt_data.R")
    source("final_scripts/orbis_code/Estimate ARs/7_get_stock_data.R")
    source("final_scripts/orbis_code/Estimate ARs/8_get_sp500_data.R")
    source("final_scripts/orbis_code/Estimate ARs/9_clean_stock_data.R")
    source("final_scripts/orbis_code/Estimate ARs/10_run_OLS_regressions.R")
    source("final_scripts/orbis_code/Estimate ARs/new_get_ar.R")
    
    # update counter variables
    n_min <<- n_min + 5000
    n_max <<- n_max + 5000
    k <<- k + 1
    
  }
  
  n_max <<- nrow(orbis_df)
  subset_orbis_df <- orbis_df[n_min:n_max,]
  
  # save batch locally
  filepath1 <<- paste0("data/raw_data/orbis/with_merger_authority_decision/batch/",k,"/", k/k ,"_raw_slice.rds")
  saveRDS(subset_orbis_df, filepath1)
  
  source("final_scripts/orbis_code/Estimate ARs/1_extract_cusips.R")
  source("final_scripts/orbis_code/Estimate ARs/2_append_acq_gvkey.R")
  source("final_scripts/orbis_code/Estimate ARs/3_append_tgt_gvkey.R")
  source("final_scripts/orbis_code/Estimate ARs/4_get_and_append_acq_rivals_id_data.R")
  source("final_scripts/orbis_code/Estimate ARs/5_get_and_append_tgt_rivals_id_data.R")
  source("final_scripts/orbis_code/Estimate ARs/6_merge_acq_and_tgt_data.R")
  source("final_scripts/orbis_code/Estimate ARs/7_get_stock_data.R")
  source("final_scripts/orbis_code/Estimate ARs/8_get_sp500_data.R")
  source("final_scripts/orbis_code/Estimate ARs/9_clean_stock_data.R")
  source("final_scripts/orbis_code/Estimate ARs/10_run_OLS_regressions.R")
  source("final_scripts/orbis_code/Estimate ARs/new_get_ar.R")

}

# For privacy purposes, the working directory is stored under an environment variable
# (this applies to all subsequent scripts)
# To properly run it upon calling the function, open the windows powershell and run:
# Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "working_dir=your_working_dir"
# Then restart R Studio and it is good to go
# Finally, create a 'data' folder inside that working directory, and store the M&A data set there
# so that the data importing in the code can run smoothly

# we nest the function call inside system.time to record the amount of time taken
# to run all the script
execution_time <- system.time( call_to_run_all_scripts <- run_all_scripts() 
                               )

