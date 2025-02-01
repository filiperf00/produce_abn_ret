####################################################################################################################################

# This script is the 8th out of a series of 9 scripts.
#
# This script simply imports the S&P500 stock data (necessary to then estimate
# the abnormal returns) and then locally saves it

####################################################################################################################################

get_sp500_data <- function(working_dir) {
  
  # this function establishes the API connection to the WRDS database and
  # proceeds to import as well as locally store the S&P500 data
  
  # set working directory
  setwd(working_dir)
  
  # start by loading the relevant libraries
  #For data manipulation
  library(dplyr)
  #Necessary libraries to download WRDS data
  library(tidyverse)
  library(scales)
  library(RSQLite)
  #Library to connect with WRDS database
  library(RPostgres)
  library(DBI)
  
  
  # connect to WRDS database 
  wrds <- dbConnect(
    Postgres(),
    host = "wrds-pgdata.wharton.upenn.edu",
    dbname = "wrds",
    port = 9737,
    sslmode = "allow",
    user = Sys.getenv("WRDS_USER"),
    password = Sys.getenv("WRDS_PASS")
  )
  
  # getting the S&P500 data to then estimate the abnormal returns
  sp500_data <- tbl(wrds, sql("SELECT Caldt, vwretd, spindx FROM crspq.dsp500")) %>%
    rename(date=caldt) %>% as.data.frame()
  
  # locally save it
  saveRDS(sp500_data, "data/raw_data/sp500_data.rds")
  
  # disconnect from database
  dbDisconnect(wrds)
  
}

# NB: for privacy purposes the WRDS user and pass should be stored as environment variables
# To store your own credentials (otherwise code will not run), open Windows Powershell and run:  
# Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "WRDS_USER=your_username"
# Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "WRDS_PASS=your_pass"
# Then restart R Studio and you are good to go

sp500_data <- get_sp500_data(Sys.getenv("working_dir"))
