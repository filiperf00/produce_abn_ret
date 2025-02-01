####################################################################################################################################

# This script is the 7th out of a series of 9 scripts.
#
# It imports the dataset containing the targets and its rivals identifying info
# that is, name, CUSIP, stock ticker, permanent company number (permco) and permanent issue number (permno), among others
# and obtains their stock data from CRSP, which is then saved locally as data frame
#
# The permanent numbers will be the actual variables used to scoop such data as they are both permanent and unique
# The former is a unique company level identifier, while the latter is a unique share class identifier (within that firm)
#
# We need both since one company may have multiple share classes listed at the same time, thus
# having recorded multiple stock prices in the same date in CRSP. To uniquely pin down the firm
# and the stock we need both identifiers.

####################################################################################################################################


get_stck_data_of_tgt_and_rivals <- function(working_dir, wrds_user, wrds_pass) {
  
  # The function takes as arguments the working directory (so it can read the M&A dataset)
  # as well as the WRDS username and password to establish the API connection. 
  # It then imports the dataset, extarcts the (permco, permno) of every target and respective rival
  # and retrieves their stock data
  
  # setting the work directory
  setwd(working_dir)
  
  # start by loading the relevant libraries
  #For data manipulation
  library(dplyr)
  #Library to connect with WRDS database
  library(RPostgres)
  library(DBI)
  #Necessary libraries to download WRDS data
  library(tidyverse)
  library(scales)
  library(RSQLite)
  
  # As per usual, we first connect to the WRDS database
  wrds <- dbConnect(
    Postgres(),
    host = "wrds-pgdata.wharton.upenn.edu",
    dbname = "wrds",
    port = 9737,
    sslmode = "allow",
    user = Sys.getenv("WRDS_USER"),
    password = Sys.getenv("WRDS_PASS")
  )
  
  # importing the dataset with the identifying info of all the firms
  # i.e. targets, targets and rivals
  # but because this is the final dataset with identifying (which we thus want to keep)
  # we must import it for each batch
  # recall the counter (i.e. k) has been set globally in the master script
  if (k == 1) {
    all_reg_id_info <- readRDS("data/processed_data/id_data/1989_99_acq_and_tgt_id_data.rds" )
  } else if (k==2) {
    all_reg_id_info <- readRDS("data/processed_data/id_data/00_09_acq_and_tgt_id_data.rds" )
  } else {
    all_reg_id_info <- readRDS("data/processed_data/id_data/10_23_acq_and_tgt_id_data.rds" )
  }
  
  # Now we can get the stock data for all targets in the M&A dataset and their rivals.
  # As mentioned above, we will do so using their permanent numbers
  
  # pulling the permanent numbers of the targets and rivals
  tgt_permcos <- all_reg_id_info %>% filter(!is.na(tgt_permco)) %>% distinct(tgt_permco) %>% pull()
  tgt_rivals_permcos <- all_reg_id_info %>% filter(!is.na(tgt_rival_permco)) %>% distinct(tgt_rival_permco) %>% pull()
  
  # combine the target and rivals' permcos into one vector and convert it into string
  # the need for the latter step will become clear in just a few lines
  merged_tgt_and_rivals_permcos <- unique( c(tgt_permcos, tgt_rivals_permcos) )
  merged_tgt_and_rivals_permcos_string <- paste( merged_tgt_and_rivals_permcos[!is.na(merged_tgt_and_rivals_permcos)] , collapse = ",")
  
  # now for the permnos
  tgt_permnos <- all_reg_id_info %>% filter(!is.na(tgt_permno)) %>% distinct(tgt_permno) %>% pull()
  tgt_rivals_permnos <- all_reg_id_info %>% filter(!is.na(tgt_rival_permno)) %>% distinct(tgt_rival_permno) %>% pull()
  
  # combine the target and rivals' permcos into one vector and convert it into string
  merged_tgt_and_rivals_permnos <- unique( c(tgt_permnos, tgt_rivals_permnos) )
  merged_tgt_and_rivals_permnos_string <- paste( merged_tgt_and_rivals_permnos[!is.na(merged_tgt_and_rivals_permnos)] , collapse = ",")
  
  # query the stock data for all acquirers and rivals
  # we pick date, permanent company number and issue number (permco and permno, respectively), the CUSIP (cusip), stock price (prc) and returns (ret) and
  # the header standard industrial classification code (hsiccd). The last one is a code ranging from 100 to 9999 that groups companies with similar
  # products or services. As per CRSP's documentation, "The first two digits refer to a major group. The first three digits refer to an industry group. 
  # All four digits indicate an industry."
  #
  # In addition, we select the number of shares outstanding (shrout),
  # the cumulative factor price adjustment (cfacpr),
  # and the cumulative factor to adjust shares outstanding (cfacshr)
  # The first is chosen to compute each firm's market capitalization (i.e. share price * # of outstanding shares)
  # the last two are chosen to correct such market cap to events such as M&As and spin-offs, among others
  # Usually cfacpr and cfacshr will cancel out such that the market cap simply equals share price times # of outstanding shares,
  # but not always
  #
  # notice the query must be a string - this is precisely why we needed to convert the vectors above into a character ones
  
  query <- sprintf("
  SELECT date, permco, permno, cusip, prc, ret, hsiccd, shrout, cfacpr, cfacshr
  FROM crspq.dsf
  WHERE permco IN (%s)
    AND permno IN (%s)
    AND date > '1970-01-01'
  ", merged_tgt_and_rivals_permcos_string,
                   merged_tgt_and_rivals_permnos_string)
  
  # execute the query
  res <- dbSendQuery(wrds, query)
  
  # we now save the stock data locally (following same procedure as before - i.e. one for each batch)
  if (k == 1) {
    
    # initialize an empty data frame to store stock data
    tgt_stock_data <- write.csv(data.frame(), "data/raw_data/stock_data/1989_99_tgt_stock_data.csv")
    
    # given the large size of the query, we shall access it in chunks
    # and store it incrementally
    # defining the chunk size
    chunk_size <- 10000  
    
    # variable to flag whether col names are to be appended (only for 1st chunk)
    first_chunk <- TRUE
    
    # fetch and store the chunks until all rows have been covered
    while(!dbHasCompleted(res)) {
      # fetching the chunk
      chunk <- dbFetch(res, n = chunk_size) 
      # storing it in the data frame
      # acq_stock_data <- bind_rows(acq_stock_data, chunk)
      write.table(chunk, file = "data/raw_data/stock_data/1989_99_tgt_stock_data.csv", append = TRUE, sep = ",", col.names = first_chunk, row.names = FALSE)
      
      # after first chunk set column names to false
      first_chunk <- FALSE
      
    }
    
    # clear the result
    dbClearResult(res)
    
    # before proceeding, some data transforming steps are required
    # i) remove observations where price is zero; 
    #
    # ii) take the absolute value of stock price variable ( since when price is unavailable 
    # CRSP uses the bid/ask average and records it as a negative value to distinguish between the 2 )
    # 
    # iii) we need to adjust the stock price to any events such as stock splits, spin-offs, etc.
    # CRSP records these events under the variable Cumulative Factor Price adjustment (cfacpr)
    # thus we simply need to divide the stock price variable (prc) by cfacpr 
    #
    # Applying all these steps
    tgt_stock_data_df <- read.csv("data/raw_data/stock_data/1989_99_tgt_stock_data.csv", skip=1)
    tgt_stock_data <- tgt_stock_data_df %>% filter( prc != 0 ) %>% 
      mutate(abs_prc = abs(prc), adj_prc = ifelse( cfacpr>0, abs_prc/cfacpr, abs_prc ) ) %>%
      select(-prc)
    
    # save the stock data locally as an RDS file
    saveRDS(tgt_stock_data, file = "data/raw_data/stock_data/1989_99_tgt_stock_data.rds" )
    
    # and delete CSV
    file.remove("data/raw_data/stock_data/1989_99_tgt_stock_data.csv")
    
  } else if (k==2) {
    
    # initialize an empty data frame to store stock data
    tgt_stock_data <- write.csv(data.frame(), "data/raw_data/stock_data/00_09_tgt_stock_data.csv")
    
    # given the large size of the query, we shall access it in chunks
    # and store it incrementally
    # defining the chunk size
    chunk_size <- 10000  
    
    # variable to flag whether col names are to be appended (only for 1st chunk)
    first_chunk <- TRUE
    
    # fetch and store the chunks until all rows have been covered
    while(!dbHasCompleted(res)) {
      # fetching the chunk
      chunk <- dbFetch(res, n = chunk_size) 
      # storing it in the data frame
      # acq_stock_data <- bind_rows(acq_stock_data, chunk)
      write.table(chunk, file = "data/raw_data/stock_data/00_09_tgt_stock_data.csv", append = TRUE, sep = ",", col.names = first_chunk, row.names = FALSE)
      
      # after first chunk set column names to false
      first_chunk <- FALSE
      
    }
    
    # clear the result
    dbClearResult(res)
    
    # before proceeding, some data transforming steps are required
    # i) remove observations where price is zero; 
    #
    # ii) take the absolute value of stock price variable ( since when price is unavailable 
    # CRSP uses the bid/ask average and records it as a negative value to distinguish between the 2 )
    # 
    # iii) we need to adjust the stock price to any events such as stock splits, spin-offs, etc.
    # CRSP records these events under the variable Cumulative Factor Price adjustment (cfacpr)
    # thus we simply need to divide the stock price variable (prc) by cfacpr 
    #
    # Applying all these steps
    tgt_stock_data_df <- read.csv("data/raw_data/stock_data/00_09_tgt_stock_data.csv", skip=1)
    tgt_stock_data <- tgt_stock_data_df %>% filter( prc != 0 ) %>% 
      mutate(abs_prc = abs(prc), adj_prc = ifelse( cfacpr>0, abs_prc/cfacpr, abs_prc ) ) %>%
      select(-prc)
    
    # save the stock data locally as an RDS file
    saveRDS(tgt_stock_data, file = "data/raw_data/stock_data/00_09_tgt_stock_data.rds" )
    
    # and delete CSV
    file.remove("data/raw_data/stock_data/00_09_tgt_stock_data.csv")
    
    
  } else {
    
    # initialize an empty data frame to store stock data
    tgt_stock_data <- write.csv(data.frame(), "data/raw_data/stock_data/10_23_tgt_stock_data.csv")
    
    # given the large size of the query, we shall access it in chunks
    # and store it incrementally
    # defining the chunk size
    chunk_size <- 10000  
    
    # variable to flag whether col names are to be appended (only for 1st chunk)
    first_chunk <- TRUE
    
    # fetch and store the chunks until all rows have been covered
    while(!dbHasCompleted(res)) {
      # fetching the chunk
      chunk <- dbFetch(res, n = chunk_size) 
      # storing it in the data frame
      # acq_stock_data <- bind_rows(acq_stock_data, chunk)
      write.table(chunk, file = "data/raw_data/stock_data/10_23_tgt_stock_data.csv", append = TRUE, sep = ",", col.names = first_chunk, row.names = FALSE)
      
      # after first chunk set column names to false
      first_chunk <- FALSE
      
    }
    
    # clear the result
    dbClearResult(res)
    
    # before proceeding, some data transforming steps are required
    # i) remove observations where price is zero; 
    #
    # ii) take the absolute value of stock price variable ( since when price is unavailable 
    # CRSP uses the bid/ask average and records it as a negative value to distinguish between the 2 )
    # 
    # iii) we need to adjust the stock price to any events such as stock splits, spin-offs, etc.
    # CRSP records these events under the variable Cumulative Factor Price adjustment (cfacpr)
    # thus we simply need to divide the stock price variable (prc) by cfacpr 
    #
    # Applying all these steps
    tgt_stock_data_df <- read.csv("data/raw_data/stock_data/10_23_tgt_stock_data.csv", skip=1)
    tgt_stock_data <- tgt_stock_data_df %>% filter( prc != 0 ) %>% 
      mutate(abs_prc = abs(prc), adj_prc = ifelse( cfacpr>0, abs_prc/cfacpr, abs_prc ) ) %>%
      select(-prc)
    
    # save the stock data locally as an RDS file
    saveRDS(tgt_stock_data, file = "data/raw_data/stock_data/10_23_tgt_stock_data.rds" )
    
    # and delete CSV
    file.remove("data/raw_data/stock_data/10_23_tgt_stock_data.csv")
  }
  
  
  # disconnect from database
  dbDisconnect(wrds)
  
}

# NB: for privacy purposes the WRDS user and pass should be stored as environment variables
# To store your own credentials (otherwise code will not run), open Windows Powershell and run:  
# Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "WRDS_USER=your_username"
# Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "WRDS_PASS=your_pass"
# Then restart R Studio and you are good to go

stck_data_of_tgt_and_rivals <- get_stck_data_of_tgt_and_rivals(Sys.getenv("working_dir"),
                                                                 Sys.getenv("WRDS_USER"), 
                                                                 Sys.getenv("WRDS_PASS")
                                                                 )
