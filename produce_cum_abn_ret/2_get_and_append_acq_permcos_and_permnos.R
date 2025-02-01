####################################################################################################################################

# This script is the 2nd out of a series of 9 scripts.
#
# It takes the previously imported (by script #1) M&A dataset (obtained from the LSEG workspace)
# and appends to it the permanent company number (permco) and permanent issue number (permno)
# of the acquirors (that of the targets is tackled in the next script).
#
# The former is a CRSP unique company level identifier, while the latter is a unique share class identifier (within that firm).
# We need both since one company may have multiple share classes listed at the same time, thus
# having recorded multiple stock prices in the same date in CRSP. To uniquely pin down the firm
# and the stock we need both identifiers.
#
# These are permanent and unique identifying variables from CRSP (Center for Research in Security and Prices)
# that will later be used to scoop the stock data for the firms in the M&A dataset 
# (plus its rivals but we shall get there in the following scripts).
# Importantly, these two identifiers will be obtained using the stock ticker or, if it fails, the CUSIP
#
# In addition, we also pick the start and end date of a firm in CRSP as well as its 
# header standard industrial classification code (named hsiccd in CRSP).
# The former is necessary to uniquely assign a (permco, permno) combination to each firm when 
# multiple are retrieved. The latter keeps track of the firms' industry
#
# This updated M&A dataset is then stored locally as a data frame

####################################################################################################################################

get_and_append_acq_permcos_and_permnos <- function(working_dir, wrds_user, wrds_pass) {
  
  # The function takes as arguments the working directory (so it can read the M&A dataset)
  # as well as the WRDS username and password to establish the API connection.
  #
  # It then matches the acquirors in the M&A dataset with CRSP (to get the 2 permanent identifiers
  # permanent company number (permco) and permanent issue number (permno) ) using the ticker, or
  # if that fails, the CUSIP
  #
  # In addition, we also pick the start and end date of a firm in CRSP as well as its 
  # header standard industrial classification code (named hsiccd in CRSP)
  
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
  
  # importing M&A dataset
  mergers_df <- readRDS("data/processed_data/id_data/lseg_mergers_data_cleaned.rds")
  
  # vectorize the necessary information from M&A dataset to filter CRSP database
  # all at once
  # (use '$' instead of %>% select() since the former returns a vector while the latter a data frame) 
  acq_names <- mergers_df$acquiror_full_name
  acq_cusip <- mergers_df$acquiror_6_digit_cusip
  acq_prim_tck <- mergers_df$acquiror_primary_ticker_symbol
  
  # we now have the identifiers to filter CRSP, so we proceed to import HERE COMMENT HERE
  # the necessary CRSP tables
  
  # Connecting to WRDS database
  wrds <- dbConnect(
    Postgres(),
    host = "wrds-pgdata.wharton.upenn.edu",
    dbname = "wrds",
    port = 9737,
    sslmode = "allow",
    user = Sys.getenv("WRDS_USER"),
    password = Sys.getenv("WRDS_PASS")
  )
  
  # connecting the table with the identifying information - stock_id. This table is necessary
  # as it contains the stock ticker (which the table with stock prices/returns does not) and it will allow
  # us to retrieve the name, ticker and CUSIP of the competitors
  stock_id <- tbl(wrds, sql("SELECT * FROM crsp.dsenames"))
  
  # convert its cusip and ncusip from 8 to 6 digit to be consistent with M&A dataset
  # since CUSIP will also be used as a matching variable between M&A dataset and CRSP
  stock_id <- stock_id %>%
    mutate(cusip = substr(cusip, 1, 6), ncusip = substr(ncusip, 1, 6))
  
  ### acquirors: Retrieving permanent number with stock ticker 
  
  # we shall retrieve stock data from CRSP using the permanent company number (permco) 
  # together with the permanent issue number (permno). The former is a permanent and unique company level identifier,
  # while the latter is a permanent and unique security level identifier, thus ensuring we pick one and only one
  # security per firm. Otherwise, if one firm has multiple share classes we might find multiple stock prices 
  # for the same date.
  #
  # This will give us the permco and permno of firms in M&A dataset (or, to be precise, of the firms whose permco-permno was matched)
  # Doing this first step for acquirors where we also retrieve the start and ending date (if it exists, otherwise its just NA)
  # of firm in CRSP database 
  #
  # Finally we save the header standard industrial classification code (hsiccd), a code ranging from 100 to 9999 identifying the 
  # industry to which the firm belongs to. This is particularly important for the rivals as for the merging parties we already
  # have that data from LSEG workspace
  
  acq_permcos_and_id_w_tck <- stock_id %>% select(namedt, nameendt, permco, permno, hsiccd, ticker) %>% filter(ticker %in% acq_prim_tck) 
  
  # but before proceeding it is important to add these permanent numbers to our original M&A dataset
  # so as to keep track of which permanent number belongs to which firm
  # first we grab the acquiror information from M&A dataset
  # and we pick the merger id column so that, when we merge all the acquiror with targets' id data
  # we don't mix acquirors and target from different mergers
  
  mergers_data_with_acq_selected_cols <- mergers_df %>% select(merger_id, date_announced, acquiror_full_name, acquiror_primary_ticker_symbol, 
                                                               acquiror_macro_industry, acquiror_6_digit_cusip, acquiror_market_value_4_weeks_prior_to_announcement,
                                                               regulatory_agencies_required_to_approve_deal,
                                                               m_a_type, premium_paid___4_weeks_prior_to_announcement,
                                                               deal_value, date_effective_or_unconditional, 
                                                               acquiror_public_status) 
  
  # once we have both dfs, we can merge the 2
  # but we don't want to lose any firm M&A data, i.e. we want to always keep all of its rows even if
  # some permanent numbers were not found, so we pass all.x=T. 
  # Finally, we rename the newly added columns so we then know which ones belong to the acquiror and (later) to the target
  
  acq_reg_id_info_w_tck_aux1 <- merge(mergers_data_with_acq_selected_cols, acq_permcos_and_id_w_tck, by.x="acquiror_primary_ticker_symbol", by.y="ticker", all.x=T) %>%
    arrange(merger_id) %>% rename(acq_permco_w_tck = permco, acq_permno_w_tck = permno, 
                                  acq_start_date_in_crsp_w_tck = namedt, acq_end_date_in_crsp_w_tck = nameendt,
                                  acq_sic_code_w_tck = hsiccd)
  
  # in addition, it is possible that in some cases the same ticker matches more than one firm and, therefore, multiple
  # permanent numbers - this may happen as different firms quoted in different stock exchanges may share the same ticker or,
  # if a company uses a stock ticker that previously belonged to a now de-listed, or even bankrupt firm
  # and, as mentioned previously, the same firm (permco) might have more than one share class (permno)
  # we need to deal with both scenarios in order to guarantee that, in the end, we get precisely one permco and permno
  # per firm 
  # First, we only want the permanent numbers whose start and ending date encompass the respective firm's merger
  # announcement date
  # and to avoid losing merger ids we only apply the filter where the permco was actually found
  
  acq_reg_id_info_w_tck_aux2 <- acq_reg_id_info_w_tck_aux1 %>% group_by(merger_id, acq_permco_w_tck) %>%
    filter( ifelse( is.na(acq_permco_w_tck), TRUE, date_announced >= acq_start_date_in_crsp_w_tck & date_announced <= acq_end_date_in_crsp_w_tck )  ) %>%
    ungroup() 
  
  # then we merge these back to the original dataframe (i.e. with all the M&A identifying info plus the permco and permno)
  # so that, for each acquiror firm, it only has permanent numbers whose start and ending date encompass the respective firm's merger
  # announcement date
  
  acq_reg_id_info_w_tck_aux3 <- merge(acq_reg_id_info_w_tck_aux1,
                                          acq_reg_id_info_w_tck_aux2 %>% select(merger_id, acq_permco_w_tck, acq_permno_w_tck), 
                                          by=c("merger_id", "acq_permco_w_tck", "acq_permno_w_tck"))
  
  # even after applying such filter, we may not get a unique (permco, permno) per firm
  # to address such issue, we pick, for each firm, the combination that has the most number of data points
  # first we pick for each permco, the permno with the highest amount of days in in CRSP 
  
  acq_reg_id_info_w_tck_aux4 <- acq_reg_id_info_w_tck_aux3 %>% group_by(merger_id, acq_permco_w_tck, acq_permno_w_tck) %>% 
    mutate(acq_start_date_in_crsp_w_tck = min(acq_start_date_in_crsp_w_tck)) %>% # keep only the oldest starting date
    slice_max(order_by = acq_end_date_in_crsp_w_tck, n=1, with_ties = T) %>% # filter the most recent ending date so we only keep track
    # of the starting and ending date of the stock in CRSP
    mutate(days_in_crsp = acq_end_date_in_crsp_w_tck - acq_start_date_in_crsp_w_tck) %>% # with such dates compute the number of days the stock has in CRSP
    slice_max(order_by = days_in_crsp, n=1, with_ties = F) %>% ungroup() # pick the permno with highest amount of date
  
  # the previous step gave us, for each firm, the number of days each (permco, permno) combination has
  # so now we only need to pick the combination with the highest amount of days
  # in case of ties, we randomly pick one (permco, permno) combination
  
  acq_reg_id_info_w_tck_aux5 <- acq_reg_id_info_w_tck_aux4 %>% group_by(merger_id) %>%
    slice_max(order_by = days_in_crsp, n=1, with_ties = F) %>% ungroup()
  
  # in the cases where the (permco, permno) was found to begin with,
  # acq_reg_id_info_w_tck_aux5 will have exactly one (permco, permno) per firm
  # otherwise, if the (permco, permno) was not found that merger id will be dropped
  # to avoid so, we merge it back to the original M&A dataset
  
  acq_reg_id_info_w_tck_aux6 <- merge(mergers_data_with_acq_selected_cols, 
                                          acq_reg_id_info_w_tck_aux5 %>% select(merger_id, acq_permco_w_tck, acq_permno_w_tck,
                                                                                    acq_start_date_in_crsp_w_tck, acq_end_date_in_crsp_w_tck, acq_sic_code_w_tck), 
                                          by="merger_id", all.x=T)
  
  # we now have every merger id from the original M&A dataset but we want to keep the the (permco, permno) we lost when their 
  # start to ending date did not encompass the merger announcement date
  # for not only there is a slim chance the OLS regression is successfully ran even for those dates but we also
  # need it to obtain its GVKEY and, consequently, its competitors
  # in those cases we just keep their most recent (permco, permno) as recorded in CRSP
  
  acq_reg_id_info_w_tck_aux7 <- acq_reg_id_info_w_tck_aux1 %>% group_by(merger_id) %>%
    slice_max(order_by = acq_start_date_in_crsp_w_tck, n=1, with_ties = F) %>%
    arrange(merger_id)
  
  # here we add them to our dataset where appropriate (i.e. we lost the permanent number due date range not including merger date)
  # we will apply the same steps to start and end dates of a firm in CRSP and to their sic code (hsiccd)
  # coalesce function will pick the first non-NA value from the elements in the vector (going from left to right)
  
  acq_reg_id_info_w_tck <- acq_reg_id_info_w_tck_aux6 %>% arrange(merger_id) %>% 
    mutate(
      acq_permco_w_tck = coalesce(acq_permco_w_tck, acq_reg_id_info_w_tck_aux7$acq_permco_w_tck), # first for permcos
      
      acq_permno_w_tck = coalesce(acq_permno_w_tck, acq_reg_id_info_w_tck_aux7$acq_permno_w_tck), # then permnos
      
      acq_start_date_in_crsp_w_tck = coalesce(acq_start_date_in_crsp_w_tck, acq_reg_id_info_w_tck_aux7$acq_start_date_in_crsp_w_tck), # start date
      
      acq_end_date_in_crsp_w_tck = coalesce(acq_end_date_in_crsp_w_tck, acq_reg_id_info_w_tck_aux7$acq_end_date_in_crsp_w_tck), # end date
      
      acq_sic_code_w_tck = coalesce(acq_sic_code_w_tck, acq_reg_id_info_w_tck_aux7$acq_sic_code_w_tck) # and finally for the SIC code
      
    )  
  
  ### REPEATING THE SAME PROCESS BUT WITH THE CUSIP
  
  # retrieving the permanent number missed by the ticker with the 6-digit CUSIP
  
  acq_permcos_and_id_w_cusip <- stock_id %>% select(namedt, nameendt, permco, permno, hsiccd, cusip) %>% filter(cusip %in% acq_cusip) 
  
  mergers_data_with_acq_selected_cols <- mergers_df %>% select(merger_id, date_announced, acquiror_6_digit_cusip) 
  
  # merging the firms' identifying info back to our M&A dataset (with only the necessary column to retrieve the firms
  # as the relevant identifying info has already been collected with the ticker)
  
  acq_reg_id_info_w_cusip_aux1 <- merge(mergers_data_with_acq_selected_cols, acq_permcos_and_id_w_cusip, by.x="acquiror_6_digit_cusip", by.y="cusip", all.x=T) %>%
    arrange(merger_id) %>% rename(acq_permco_w_cusip=permco, acq_permno_w_cusip=permno, acq_start_date_in_crsp_w_cusip=namedt, acq_end_date_in_crsp_w_cusip=nameendt,
                                  acq_sic_code_w_cusip = hsiccd)
  
  # keeping only the merger ids whose (permco, permno) was either not found or date range in CRSP
  # includes the merger announcement date
  
  acq_reg_id_info_w_cusip_aux2 <- acq_reg_id_info_w_cusip_aux1 %>% group_by(merger_id, acq_permco_w_cusip) %>%
    filter( ifelse( is.na(acq_permco_w_cusip), TRUE, date_announced > acq_start_date_in_crsp_w_cusip & date_announced < acq_end_date_in_crsp_w_cusip )  ) %>%
    ungroup() 
  
  # keeping only the (permco, permno) that satisfy the above filtering
  
  acq_reg_id_info_w_cusip_aux3 <- merge(acq_reg_id_info_w_cusip_aux1,
                                            acq_reg_id_info_w_cusip_aux2 %>% select(merger_id, acq_permco_w_cusip, acq_permno_w_cusip), 
                                            by=c("merger_id", "acq_permco_w_cusip", "acq_permno_w_cusip"))
  
  # if duplicates still persist pick, for each merger, the (permco, permno) with the most data points
  
  acq_reg_id_info_w_cusip_aux4 <- acq_reg_id_info_w_cusip_aux3 %>% group_by(merger_id, acq_permco_w_cusip, acq_permno_w_cusip) %>% 
    mutate(acq_start_date_in_crsp_w_cusip = min(acq_start_date_in_crsp_w_cusip)) %>%
    slice_max(order_by = acq_end_date_in_crsp_w_cusip, n=1, with_ties = T) %>%
    mutate(days_in_crsp = acq_end_date_in_crsp_w_cusip - acq_start_date_in_crsp_w_cusip) %>%
    slice_max(order_by = days_in_crsp, n=1, with_ties = F) %>% ungroup()
  
  acq_reg_id_info_w_cusip_aux5 <- acq_reg_id_info_w_cusip_aux4 %>% group_by(merger_id) %>%
    slice_max(order_by = days_in_crsp, n=1, with_ties = F) %>% ungroup()
  
  # one final merger to ensure we dont lose any merger ids
  
  acq_reg_id_info_w_cusip_aux6 <- merge(mergers_data_with_acq_selected_cols, 
                                            acq_reg_id_info_w_cusip_aux5 %>% select(merger_id, acq_permco_w_cusip, acq_permno_w_cusip,
                                                                                        acq_start_date_in_crsp_w_cusip, acq_end_date_in_crsp_w_cusip, acq_sic_code_w_cusip), 
                                            by="merger_id", all.x=T)
  
  # and finally keeping the (permco, permno) that we lost for their date range in CRSP did not
  # encompass the merger announcement date
  
  acq_reg_id_info_w_cusip_aux7 <- acq_reg_id_info_w_cusip_aux1 %>% group_by(merger_id) %>%
    slice_max(order_by = acq_start_date_in_crsp_w_cusip, n=1, with_ties = F) %>%
    arrange(merger_id)
  
  # here we add them to our dataset where appropriate (i.e. we lost the permanent number due date range not including merger date)
  # we will apply the same steps to start and end dates of a firm in CRSP and to their sic code (hsiccd)
  # coalesce function will pick the first non-NA value from the elements in the vector (going from left to right)
  
  acq_reg_id_info_w_cusip <- acq_reg_id_info_w_cusip_aux6 %>% arrange(merger_id) %>% 
    mutate(
      acq_permco_w_cusip = coalesce(acq_permco_w_cusip, acq_reg_id_info_w_cusip_aux7$acq_permco_w_cusip), # first for permcos
      
      acq_permno_w_cusip = coalesce(acq_permno_w_cusip, acq_reg_id_info_w_cusip_aux7$acq_permno_w_cusip), # then permnos
      
      acq_start_date_in_crsp_w_cusip = coalesce(acq_start_date_in_crsp_w_cusip, acq_reg_id_info_w_cusip_aux7$acq_start_date_in_crsp_w_cusip), # start date
      
      acq_end_date_in_crsp_w_cusip = coalesce(acq_end_date_in_crsp_w_cusip, acq_reg_id_info_w_cusip_aux7$acq_end_date_in_crsp_w_cusip), # end date
      
      acq_sic_code_w_cusip = coalesce(acq_sic_code_w_cusip, acq_reg_id_info_w_cusip_aux7$acq_sic_code_w_cusip) # and finally for the SIC code
      
    )   
  
  # finally, we plug in the (permco, permno) found with CUSIP whenever the ticker failed to do so
  # and similarly for start and end dates as well as the SIC code
  
  acq_reg_id_info <- acq_reg_id_info_w_tck %>% mutate( 
    acq_permco = coalesce(acq_permco_w_tck, acq_reg_id_info_w_cusip$acq_permco_w_cusip),
    
    acq_permno = coalesce(acq_permno_w_tck, acq_reg_id_info_w_cusip$acq_permno_w_cusip),
    
    acq_start_date_in_crsp = coalesce(acq_start_date_in_crsp_w_tck, acq_reg_id_info_w_cusip$acq_start_date_in_crsp_w_cusip),
    
    acq_end_date_in_crsp = coalesce(acq_end_date_in_crsp_w_tck, acq_reg_id_info_w_cusip$acq_end_date_in_crsp_w_cusip),
    
    acq_sic_code = coalesce(acq_sic_code_w_tck, acq_reg_id_info_w_cusip$acq_sic_code_w_cusip)
  )
  
  # before we save the acquiror identifying information locally, let us remove superfluous columns
  
  acq_reg_id_info <- acq_reg_id_info %>% select( - c( acq_permco_w_tck, acq_permno_w_tck, acq_start_date_in_crsp_w_tck, acq_end_date_in_crsp_w_tck,
                                                      acq_sic_code_w_tck ) )
  
  # saving locally the sample deals dataset after transforming the data
  # csv_file_path <- "data/processed_data/id_data/acq_id_data_with_permcos_and_permnos.csv"
  
  # if file has already been created then simply append the data to it
  # if (file.exists(csv_file_path)) {
    # write.table(acq_reg_id_info, file = "data/processed_data/id_data/acq_id_data_with_permcos_and_permnos.csv", append = TRUE, sep = ",", col.names = TRUE, row.names = FALSE)
    
  # } else{
    # otherwise create the CSV file and then append the data
    # write.csv(acq_reg_id_info,  "data/processed_data/id_data/acq_id_data_with_permcos_and_permnos.csv")
    
    # write.table(acq_reg_id_info, file = "data/processed_data/id_data/acq_id_data_with_permcos_and_permnos.csv", append = TRUE, sep = ",", col.names = FALSE, row.names = FALSE)
  #}
  
  # save file locally
  saveRDS(acq_reg_id_info, "data/processed_data/id_data/acq_id_data_with_permcos_and_permnos.rds")
  
  #Disconnect from database
  
  dbDisconnect(wrds)
  
}

# NB: for privacy purposes the WRDS user and pass should be stored as environment variables
# To store your own credentials (otherwise code will not run), open Windows Powershell and run:  
# Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "WRDS_USER=your_username"
# Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "WRDS_PASS=your_pass"
# Then restart R Studio and you are good to go

acq_id_info_with_permcos_and_permnos <- get_and_append_acq_permcos_and_permnos(Sys.getenv("working_dir"),
                                                                               Sys.getenv("WRDS_USER"), 
                                                                               Sys.getenv("WRDS_PASS") 
                                                                               ) 
