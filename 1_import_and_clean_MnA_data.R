############################################

# This script imports the M&A dataset (obtained from LSEG workspace)
# and applies some basic data transformation steps
# before locally saving the updated M&A dataset.
#
# It is the 1st script out of a series of 9. In order of execution, the subsequent ones start by
# matching such M&A dataset with the Center for Research in Security Prices (CRSP) to obtain its
# identifying variables. That is, the variables that will be used to later retrieve the stock data,
# namely stock prices and returns. 
#
# We then find the top-5 rivals for each merging party (i.e. acquiror and target). Only then do we retrieve 
# the stock data for all these firms (i.e. merging parties and its respective rivals) to then estimate the
# abnormal and cumulative abnormal returns 
#
# The function has the flexibility to exclude financial firms from the M&A sample or restrict to
# firms listed in a pool stock exchanges specified by the user

############################################

import_and_clean_MnA_data <- function(working_dir, exclude_fin_firms, list_of_stock_exchanges) {
  
  # The function imports the M&A dataset from LSEG and applies some basic data transformation steps
  # To do so it takes as arguments exclude_fin_firms and list_of_stock_exchanges which 
  # allow the user, respectively, to filter the M&A dataset by excluding (or not) financial firms 
  # and to decide the stock exchanges firms must be listed in 
  
  # setting the work directory
  setwd(Sys.getenv("working_dir"))
  
  # start by loading the relevant libraries
  #For data manipulation
  library(dplyr)
  #To read excel files into R session
  library(readxl)
  
  # notice the dataset has already been imported from the master script
  # with name "mergers_data"
  # we thus just need to clean it
  
  # convert "." to "_" in the column names 
  colnames(mergers_data) <- tolower(gsub("\\.", "_", colnames(mergers_data) ) )
  
  # and convert the appropriate date columns
  # mergers_df$date_announced <- as.Date(mergers_df$date_announced, format="%m/%d/%Y")
  
  # these lines remove the financial firms in the dataset if the letter "y"
  # is found in the argument exclude_fin_firms. Otherwise, nothing changes
  if (grepl("y", exclude_fin_firms, ignore.case=T)) {
    mergers_data <- mergers_data %>% filter(!(acquiror_macro_industry=="Financials"|target_macro_industry=="Financials"))
  }
  
  # filtering for firms in LSEG workspace listed in NYSE, NASDAQ, AMEX and ARCA
  # since CRSP only includes firms listed in these stock exchanges
  # but first convert the wording of stock exchanges to title case so it matches
  # correctly in LSEG
  # list_of_stock_exchanges <- tools::toTitleCase(list_of_stock_exchanges)
  # mergers_df <- mergers_df %>% filter( acquiror_primary_stock_exchange %in% list_of_stock_exchanges & 
  #                                      target_primary_stock_exchange %in% list_of_stock_exchanges ) 
  
  # remove "." in ticker column used to separate the first four characters from the 5th
  # since CRSP does not use the dot
  mergers_df <- mergers_data %>% mutate(acquiror_primary_ticker_symbol=gsub("\\.", "", acquiror_primary_ticker_symbol))
  mergers_df <- mergers_df %>% mutate(target_primary_ticker_symbol=gsub("\\.", "", target_primary_ticker_symbol))
  
  # convert empty entries in the column indicating the regulatory authority that handled the merge to NA
  mergers_df <- mergers_df %>% mutate( regulatory_agencies_required_to_approve_deal = 
                                         ifelse( nchar(regulatory_agencies_required_to_approve_deal) == 0, NA, regulatory_agencies_required_to_approve_deal ) )
  
  # removing restructuring deals (i.e. those where the acquirer and target have same company name)
  mergers_df <- mergers_df %>% filter(! (target_full_name == acquiror_full_name) ) 
  
  # finally assign an id to each merger (this will be necessary to later merge acquiror and target stock data into one data frame)
  mergers_df <- mergers_df %>% arrange(desc(date_announced)) %>%  mutate(merger_id=1:nrow(mergers_df))
  
  # and remove initial dataset to clear memory space
  rm(mergers_data)
  
  # saving locally the sample deals dataset after transforming the data
  # csv_file_path <- "data/processed_data/id_data/mergers_lseg_data_cleaned.csv"
  
  # if file has already been created then simply append the data to it
  #if (file.exists(csv_file_path)) {
    #write.table(mergers_df, file = "data/processed_data/id_data/mergers_lseg_data_cleaned.csv", append = TRUE, sep = ",", col.names = TRUE, row.names = FALSE)
    
  #} else{
    # otherwise create the CSV file and then append the data
    # write.csv(mergers_df,  "data/processed_data/id_data/mergers_lseg_data_cleaned.csv")
    
    # write.table(mergers_df[-1, ], file = "data/processed_data/id_data/mergers_lseg_data_cleaned.csv", append = TRUE, sep = ",", col.names = FALSE, row.names = FALSE)
  #}
  
  # save dataset as RDS file
  saveRDS(mergers_df, "data/processed_data/id_data/lseg_mergers_data_cleaned.rds")
  
}

# For privacy purposes, the working directory is stored under an environment variable
# (this applies to all subsequent scripts)
# To properly run it upon calling the function, open the windows powershell and run:
# Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "working_dir=your_working_dir"
# Then restart R Studio and it is good to go
# Finally, create a 'data' folder inside that working directory, and store the M&A data set there
# so that the data importing in the code can run smoothly

# the last two arguments indicate, respectively, whether the user wishes to exclude financial firms or not (if "yes", 
# financial firms are excluded) and the list of stock exchanges in which every firm must be quoted in at least one
cleaned_MnA_data <- import_and_clean_MnA_data(Sys.getenv("working_dir"),
                                              "no",
                                              c("Nasdaq", "New York Stock Exchange", 
                                              "NYSE Amex", "NYSE Arca")
                                              )


