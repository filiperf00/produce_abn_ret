####################################################################################################################################

# This script is the 6th out of a series of 9 scripts.
#
# It imports the dataset containing the acquirers and its rivals identifying info
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


get_stck_data_of_acq_and_rivals <- function(working_dir, wrds_user, wrds_pass) {
  
  # The function takes as arguments the working directory (so it can read the M&A dataset)
  # as well as the WRDS username and password to establish the API connection. 
  # It then imports the dataset, extarcts the (permco, permno) of every acquiror and respective rival
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
  
  # before actually proceeding to getting the stock data we merge all the acquirors and targets (plus all their rivals)
  # identifying information into one single dataset - the reason for this will be clear momentarily
  
  # importing the dataset with the acquirers plus its rivals identifying info
  all_acq_and_rivals_reg_id_info <- readRDS("data/processed_data/id_data/acq_and_rivals_id_data.rds")
  # we will also import that of the targets plus its rivals
  all_tgt_and_rivals_reg_id_info <- readRDS("data/processed_data/id_data/tgt_and_rivals_id_data.rds")
  
  # merging the two under a single data frame
  all_reg_id_info_aux1 <- merge(all_acq_and_rivals_reg_id_info, all_tgt_and_rivals_reg_id_info %>% select(-date_announced), by="merger_id") %>%
    mutate(acq_rival_name = str_to_title(acq_rival_name), tgt_rival_name = str_to_title(tgt_rival_name)) 
  
  # before proceeding we need to ensure a target firm does not belong to the list of the acquiror's competitors
  # and vice-versa - it is precisely for this reason that we applied the aforementioned merge. We needed both merging parties' GVKEYs. 
  #
  # But notice that in cases of NA GVKEYs R automatically filters them out when using filter()
  # thus we must specify the rule applies only to non-NA values of GVKEYs
  #
  # first removing cases where acquiror belongs to the list of the target's rivals
  all_reg_id_info_aux2 <- all_reg_id_info_aux1 %>% group_by(merger_id, acq_rival_gvkey) %>% filter( ifelse( is.na(acq_rival_gvkey) | is.na(tgt_gvkey), 
                                                                                                            TRUE, acq_rival_gvkey != tgt_gvkey ) ) %>% ungroup()
  
  # and keep track of any merger ids we might have dropped (i.e. cases where the only rival of the acquiror is the target itself)
  missing_merger_ids <- setdiff( all_reg_id_info_aux1$merger_id, all_reg_id_info_aux2$merger_id )
  
  # grab the missing data while assigning to NA values to competitor information of rivals as these were dropped
  missing_data <- all_reg_id_info_aux1 %>% filter(merger_id %in% missing_merger_ids) %>% group_by(merger_id) %>% slice(1) %>%
    mutate(acq_rival_gvkey = NA,
           acq_rival_name = "NA",
           acq_rival_cusip = "NA",
           acq_rival_ncusip = "NA",
           acq_rival_ticker = "NA",
           acq_rival_start_date = as.Date(NA),
           acq_rival_end_date = as.Date(NA),
           acq_year_hp = NA
           )
  
  # add back the data to the dataset
  all_reg_id_info_aux3 <- bind_rows(all_reg_id_info_aux2, missing_data)
  
  # now removing cases where target belongs to list of acquiror's rivals
  all_reg_id_info_aux4 <- all_reg_id_info_aux3 %>% group_by(merger_id, tgt_rival_gvkey) %>% filter( ifelse( is.na(tgt_rival_gvkey) | is.na(acq_gvkey), TRUE, tgt_rival_gvkey != acq_gvkey ) ) %>% ungroup()
  
  # and keep track of any merger ids we might have dropped (i.e. cases where the only rival of the acquiror is the target itself)
  missing_merger_ids <- setdiff( all_reg_id_info_aux3$merger_id, all_reg_id_info_aux4$merger_id )
  
  # grab the missing data while assigning to NA values to competitor information of rivals as these were dropped
  missing_data <- all_reg_id_info_aux3 %>% filter(merger_id %in% missing_merger_ids) %>% group_by(merger_id) %>% slice(1) %>%
    mutate(tgt_rival_gvkey = NA,
           tgt_rival_name = "NA",
           tgt_rival_cusip = "NA",
           tgt_rival_ncusip = "NA",
           tgt_rival_ticker = "NA",
           tgt_rival_start_date = as.Date(NA),
           tgt_rival_end_date = as.Date(NA),
           tgt_year_hp = NA
    )
  
  # add back the data to the dataset
  all_reg_id_info <- bind_rows(all_reg_id_info_aux4, missing_data)
  
  # save as RDS file
  # but because this is the final dataset with identifying (which we thus want to keep)
  # we must save it for each batch
  # recall the counter (i.e. k) has been set globally in the master script
  if (k == 1) {
    saveRDS(all_reg_id_info, file = "data/processed_data/id_data/1989_99_acq_and_tgt_id_data.rds" )
  } else if (k==2) {
    saveRDS(all_reg_id_info, file = "data/processed_data/id_data/00_09_acq_and_tgt_id_data.rds" )
  } else {
    saveRDS(all_reg_id_info, file = "data/processed_data/id_data/10_23_acq_and_tgt_id_data.rds" )
  }
  
  # The final step is actually getting the stock data for all firms in the M&A dataset and their rivals.
  # In this script we only do it for the acquirors - the targets are left for the next one
  # As mentioned above, we will do so using their permanent numbers
  
  # pulling the permanent numbers of the acquirors and rivals
  acq_permcos <- all_reg_id_info %>% filter(!is.na(acq_permco)) %>% distinct(acq_permco) %>% pull()
  acq_rivals_permcos <- all_reg_id_info %>% filter(!is.na(acq_rival_permco)) %>% distinct(acq_rival_permco) %>% pull()
  
  # combine the acquirer and rivals' permcos into one vector and convert it into string
  # the need for the latter step will become clear in just a few lines
  merged_acq_and_rivals_permcos <- unique( c(acq_permcos, acq_rivals_permcos) )
  merged_acq_and_rivals_permcos_string <- paste( merged_acq_and_rivals_permcos[!is.na(merged_acq_and_rivals_permcos)] , collapse = ",")
  
  # now for the permnos
  acq_permnos <- all_reg_id_info %>% filter(!is.na(acq_permno)) %>% distinct(acq_permno) %>% pull()
  acq_rivals_permnos <- all_reg_id_info %>% filter(!is.na(acq_rival_permno)) %>% distinct(acq_rival_permno) %>% pull()
  
  # combine the acquirer and rivals' permcos into one vector and convert it into string
  merged_acq_and_rivals_permnos <- unique( c(acq_permnos, acq_rivals_permnos) )
  merged_acq_and_rivals_permnos_string <- paste( merged_acq_and_rivals_permnos[!is.na(merged_acq_and_rivals_permnos)] , collapse = ",")
  
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
  # the last two are chosen to correct such market cap to events such as M&As and spin-offs, among other
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
  ", merged_acq_and_rivals_permcos_string,
     merged_acq_and_rivals_permnos_string)
  
  # execute the query
  res <- dbSendQuery(wrds, query)
  
  # we now save the stock data locally (following same procedure as before - i.e. one for each batch)
  if (k == 1) {
    
    # initialize an empty data frame to store stock data
    acq_stock_data <- write.csv(data.frame(), "data/raw_data/stock_data/1989_99_acq_stock_data.csv")
    
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
      write.table(chunk, file = "data/raw_data/stock_data/1989_99_acq_stock_data.csv", append = TRUE, sep = ",", col.names = first_chunk, row.names = FALSE)
      
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
    acq_stock_data_df <- read.csv("data/raw_data/stock_data/1989_99_acq_stock_data.csv", skip=1)
    acq_stock_data <- acq_stock_data_df %>% filter( prc != 0 ) %>% 
      mutate(abs_prc = abs(prc), adj_prc = ifelse( cfacpr>0, abs_prc/cfacpr, abs_prc ) ) %>%
      select(-prc)
    
    # save the stock data locally as an RDS file
    saveRDS(acq_stock_data, file = "data/raw_data/stock_data/1989_99_acq_stock_data.rds" )
    
    # and delete CSV
    file.remove("data/raw_data/stock_data/1989_99_acq_stock_data.csv")
    
  } else if (k==2) {
    
    # initialize an empty data frame to store stock data
    acq_stock_data <- write.csv(data.frame(), "data/raw_data/stock_data/00_09_acq_stock_data.csv")
    
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
      write.table(chunk, file = "data/raw_data/stock_data/00_09_acq_stock_data.csv", append = TRUE, sep = ",", col.names = first_chunk, row.names = FALSE)
      
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
    acq_stock_data_df <- read.csv("data/raw_data/stock_data/00_09_acq_stock_data.csv", skip=1)
    acq_stock_data <- acq_stock_data_df %>% filter( prc != 0 ) %>% 
      mutate(abs_prc = abs(prc), adj_prc = ifelse( cfacpr>0, abs_prc/cfacpr, abs_prc ) ) %>%
      select(-prc)
    
    # save the stock data locally as an RDS file
    saveRDS(acq_stock_data, file = "data/raw_data/stock_data/00_09_acq_stock_data.rds" )
    
    # and delete CSV
    file.remove("data/raw_data/stock_data/00_09_acq_stock_data.csv")
    
    
  } else {
    
    # initialize an empty data frame to store stock data
    acq_stock_data <- write.csv(data.frame(), "data/raw_data/stock_data/10_23_acq_stock_data.csv")
    
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
      write.table(chunk, file = "data/raw_data/stock_data/10_23_acq_stock_data.csv", append = TRUE, sep = ",", col.names = first_chunk, row.names = FALSE)
      
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
    acq_stock_data_df <- read.csv("data/raw_data/stock_data/10_23_acq_stock_data.csv", skip=1)
    acq_stock_data <- acq_stock_data_df %>% filter( prc != 0 ) %>% 
      mutate(abs_prc = abs(prc), adj_prc = ifelse( cfacpr>0, abs_prc/cfacpr, abs_prc ) ) %>%
      select(-prc)
    
    # save the stock data locally as an RDS file
    saveRDS(acq_stock_data, file = "data/raw_data/stock_data/10_23_acq_stock_data.rds" )
    
    # and delete CSV
    file.remove("data/raw_data/stock_data/10_23_acq_stock_data.csv")
  }
  
  # disconnect from database
  dbDisconnect(wrds)
  
}

# NB: for privacy purposes the WRDS user and pass should be stored as environment variables
# To store your own credentials (otherwise code will not run), open Windows Powershell and run:  
# Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "WRDS_USER=your_username"
# Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "WRDS_PASS=your_pass"
# Then restart R Studio and you are good to go

stck_data_of_acq_and_rivals <- get_stck_data_of_acq_and_rivals(Sys.getenv("working_dir"),
                                                                 Sys.getenv("WRDS_USER"), 
                                                                 Sys.getenv("WRDS_PASS")
                                                                 )
