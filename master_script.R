########################################################################################

# This is the master script which executes all the scripts necessary to collect the data
# and produce the cumulative abnormal returns as part of the RA project for Professors
# Jamie Coen and Patrick Coen.

########################################################################################

run_all_scripts <- function(working_dir) {
  
  library(dplyr)
  
  # set the working directory
  setwd(Sys.getenv("working_dir"))
  
  # to avoid congesting the R memory, we will divide the data into 3
  # 1: years [1989, 1999]
  # 2: [2000, 2009]
  # 3: [2010, 2023]
  
  # import M&A dataset from LSEG
  all_lseg_mergers_df <- read.csv("data/raw_data/LSEG_mergers_data/all_lseg_mergers_data.csv")
  all_lseg_mergers_df$Date.Announced <- as.Date(all_lseg_mergers_df$Date.Announced, format = "%m/%d/%Y")
  
  # subset them on the appropriate years
  lseg_mergers_df_89_99 <- all_lseg_mergers_df %>% filter(Date.Announced < as.Date("2000-01-01"))
  lseg_mergers_df_00_09 <- all_lseg_mergers_df %>% filter(Date.Announced >= as.Date("2000-01-01") & Date.Announced < as.Date("2010-01-01") )
  lseg_mergers_df_10_23 <- all_lseg_mergers_df %>% filter(Date.Announced >= as.Date("2010-01-01"))
  
  # and save each as an RDS file
  saveRDS(lseg_mergers_df_89_99, "data/raw_data/LSEG_mergers_data/1989_99_lseg_mergers_data.rds")
  saveRDS(lseg_mergers_df_00_09, "data/raw_data/LSEG_mergers_data/00_09_lseg_mergers_data.rds")
  saveRDS(lseg_mergers_df_10_23, "data/raw_data/LSEG_mergers_data/10_23_lseg_mergers_data.rds")
  
  list_lseg_mergers_data <- list(lseg_mergers_df_89_99, lseg_mergers_df_00_09, lseg_mergers_df_10_23)
  
  for (i in 1:length(list_lseg_mergers_data)) {
    k <<- i
    # store globally the mergers (subsetted) dataset so that
    # the first script can access it
    mergers_data <<- list_lseg_mergers_data[[i]]
    
    # run each script in the correct order
    source("final_scripts/1_import_and_clean_MnA_data.R")
    source("final_scripts/2_get_and_append_acq_permcos_and_permnos.R")
    source("final_scripts/3_get_and_append_tgt_permcos_and_permnos.R")
    source("final_scripts/4_get_and_append_acq_rivals_id_data.R")
    source("final_scripts/5_get_and_append_tgt_rivals_id_data.R")
    source("final_scripts/6_get_stck_data_of_acq_and_rivals.R")
    source("final_scripts/7_get_stck_data_for_tgt_and_rivals.R")
    source("final_scripts/8_get_sp500_data.R")
    source("final_scripts/9_run_OLS.R")
    #source("final_scripts/9_table_of_firms_distribution.R")
    #source("final_scripts/10_analyzing_the_data.R")
    
  }
  
  print("All scripts have been executed.")
}

# we nest the function call inside system.time to record the amount of time taken
# to run all the script
execution_time <- system.time( call_to_run_all_scripts <- run_all_scripts(Sys.getenv("working_dir") 
                                                                          ) 
                               )

