# ------------------------------------------------------------------------------
# 7_get_sp500_data.R
#
# Author: Filipe Ribeiro Ferreira
#
# This script is the 7th out of a series of 9 scripts.
#
# This script imports the S&P500 stock data from CRSP and then locally saves it
# ------------------------------------------------------------------------------
get_sp500_data <- function(wrds_user, wrds_pass) {
  
  # this function imports the S&P500 stock data from CRSP and then saves it
  # locally
  
  # connect to WRDS database 
  wrds <- dbConnect(
    Postgres(),
    host = "wrds-pgdata.wharton.upenn.edu",
    dbname = "wrds",
    port = 9737,
    sslmode = "require",
    user = Sys.getenv("WRDS_USER"),
    password = Sys.getenv("WRDS_PASS"),
    options  = "-c tcp_keepalives_idle=60 -c tcp_keepalives_interval=30 -c tcp_keepalives_count=10 -c statement_timeout=0 -c idle_in_transaction_session_timeout=0"
  )
  
  # getting the S&P500 data to then estimate the abnormal returns
  sp500_data <- tbl(wrds, sql("SELECT Caldt, vwretd, spindx FROM crspq.dsp500")) %>%
    rename(date=caldt) %>% as.data.frame()
  
  # locally save it
  saveRDS(sp500_data, "data/raw_data/sp500_data.rds")
  
  # disconnect from database
  dbDisconnect(wrds)
  
}

get_sp500_data(Sys.getenv("WRDS_USER"), 
               Sys.getenv("WRDS_PASS")
               )
