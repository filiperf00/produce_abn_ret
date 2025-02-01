####################################################################################################################################

# This script is the 9th out of a series of 9 scripts.
#
# This script takes the stock data of both the acquirors and targets plus their rivals
# and estimates the abnormal and cumulative abnormal returns (CAR) for every firm  
# before adding it to a dataset containing the identifying information (i.e name, ticker, CUSIP, industry, permanent numbers, etc.)
# of the acquirers, targets and all rivals
#
# As per usual, this final data frame is then locally saved

####################################################################################################################################

# num_of_rivals_per_firm <- 5
# event_window_days <- 15
# estimation_window_days <- 200

get_final_dataset_with_car <- function(working_dir, num_of_rivals_per_firm, event_window_days, 
                        estimation_window_days) {
  
  # The function takes the stock data of acquirers, targets and rivals, 
  # imports the S&P500 stock data from CRSP and
  # produces the abnormal and cumulative abnormal returns (CAR) for the event period
  # for every firm type (i.e. acquiror, target and all its rivals)
  #
  # The event period is the total number of days surrounding the merger announcement date
  # in which the merger decision can still have a non-negligible effect on the stock price
  #
  # But event_window_days does not capture the total amount of days in the event period
  # Instead, it denotes the amount of days surrounding the event period on each side. As we
  # define it to be symmetric (i.e. same number of days before and after the merger date),
  # we simply need one value - that is, the number of days either before or after the merger date
  #
  # Only estimation_window_days contains the actual total amount of days in the estimation window
  # 
  # Its output is a table containing each firm's identifying information
  # namely its name, cusip, ticker, industry, rival score, etc. and, importantly,
  # its abnormal and cumulative abnormal returns as well as their standard deviation 
  
  # setting work directory
  setwd(working_dir)
  
  # start by loading the relevant libraries
  #For data manipulation
  library(dplyr)
  # to get the NYSE calendar (i.e. its trading and non-trading days)
  library(bizdays)
  # to work with dates
  library(lubridate)
  
  # loading the data with all the identifying information for each firm
  # (where again we need to do so for each batch)
  if (k == 1) {
    
    all_reg_id_info <- readRDS("data/processed_data/id_data/1989_99_acq_and_tgt_id_data.rds")
    
    # convert mkt cap column to numeric
    all_reg_id_info$acquiror_market_value_4_weeks_prior_to_announcement <- 
      as.numeric(all_reg_id_info$acquiror_market_value_4_weeks_prior_to_announcement)
    
    all_reg_id_info$target_market_value_4_weeks_prior_to_announcement <- 
      as.numeric(all_reg_id_info$target_market_value_4_weeks_prior_to_announcement)
    
    # the stock data of acquirers and its rivals
    acq_stock_data <- readRDS("data/raw_data/stock_data/1989_99_acq_stock_data.rds" ) 
    
    # as well as targets and its rivals
    tgt_stock_data <- readRDS("data/raw_data/stock_data/1989_99_tgt_stock_data.rds" ) 
    
  } else if (k==2) {
    
    all_reg_id_info <- readRDS("data/processed_data/id_data/00_09_acq_and_tgt_id_data.rds")
    
    # convert mkt cap column to numeric
    all_reg_id_info$acquiror_market_value_4_weeks_prior_to_announcement <- 
      as.numeric(all_reg_id_info$acquiror_market_value_4_weeks_prior_to_announcement)
    
    all_reg_id_info$target_market_value_4_weeks_prior_to_announcement <- 
      as.numeric(all_reg_id_info$target_market_value_4_weeks_prior_to_announcement)
    
    # the stock data of acquirers and its rivals
    acq_stock_data <- readRDS("data/raw_data/stock_data/00_09_acq_stock_data.rds" ) 
    
    # as well as targets and its rivals
    tgt_stock_data <- readRDS("data/raw_data/stock_data/00_09_tgt_stock_data.rds" ) 
    
  } else {
    
    all_reg_id_info <- readRDS("data/processed_data/id_data/10_23_acq_and_tgt_id_data.rds")
    
    # convert mkt cap column to numeric
    all_reg_id_info$acquiror_market_value_4_weeks_prior_to_announcement <- 
      as.numeric(all_reg_id_info$acquiror_market_value_4_weeks_prior_to_announcement)
    
    all_reg_id_info$target_market_value_4_weeks_prior_to_announcement <- 
      as.numeric(all_reg_id_info$target_market_value_4_weeks_prior_to_announcement)
    
    # the stock data of acquirers and its rivals
    acq_stock_data <- readRDS("data/raw_data/stock_data/10_23_acq_stock_data.rds" ) 
    
    # as well as targets and its rivals
    tgt_stock_data <- readRDS("data/raw_data/stock_data/10_23_tgt_stock_data.rds" ) 
    
  }
  
  # and the hoberg-philips dataset with the competitors' score
  competitors_txt_def <- readRDS(file = "data/raw_data/hoberg_philips.rds" )
  
  # now the S&P500 stock data
  sp500_data <- readRDS("data/raw_data/sp500_data.rds")
  
  # data mapping SIC code to industry name (from U.S. Securities and Exchange Commission (SEC) )
  data_to_map_sic_to_industry <- read.csv("data/raw_data/data_to_map_sic_code_to_industry_name.csv")
  
  colnames(data_to_map_sic_to_industry) <- c("sic_code", "office", "industry_name")
  
  # hereby we create an empty dataframe to later store each firm's relevant identifying information
  # and their cumulative abnormal returns in a neat output
  final_table <- data.frame("Merger_id" = numeric(), "Announcement date"=as.Date(character()), 
                            "Trading day relative to anc't" = numeric(),
                            "Firm"=character(),
                            "Firm Type" = character(),
                            "LSEG Market value 4 weeks prior to announcement (USD, Millions)" = numeric(),
                            "CRSP Market value 4 weeks prior to announcement (USD, Millions)" = numeric(),
                            "Industry"=character(), 
                            "Firm Status"=character(),
                            "Ticker"=character(),
                            "CUSIP"=character(),
                            "Permco"=numeric(),
                            "Permno"=numeric(),
                            "GVKEY"=numeric(),
                            "SIC Code" = numeric(),
                            "Deal Value" = numeric(),
                            "Deal Status" = character(),
                            "Regulatory agencies required to approve deal" = character(),
                            "Relative AR" = numeric(),
                            "Relative CAR"=numeric(),
                            "SD (R-CAR)" = numeric(),
                            "Absolute AR" = numeric(),
                            "Absolute CAR"=numeric(),
                            "SD (A-CAR)" = numeric(),
                            "Competitors' score" = numeric(),
                            "Year of score" = numeric(),
                            "Beggining date in CRSP" = as.Date(character()),
                            "Ending date in CRSP" = as.Date(character()),
                            check.names = FALSE) 
  
  # creating the function to add data to the table which will be called inside for loop
  # it takes all the relevant identifying information as arguments as well as
  # the cumulative abnormal returns
  #
  # In particular, it adds the merger id, merger announcement date, the trading day relative to announcement
  # (since event window has 15 days this variable ranges from -15 to 15, with zero being the merger announcement date),
  # firm name, its market value 4 weeks prior to announcement, stock ticker, CUSIP, permanent company number (permco),
  # permanent issue number (permno), GVKEY, industry (sic code), firm type (i.e. acquiror, target or rival),
  # abnormal returns (using stock returns variable), cumulative abnormal returns and its standard deviation,
  # abnormal returns (using stock price, so these are in absolute terms), cumulative abnormal returns and its standard deviation (this time with the stock price),
  # and finally the start and ending date for which the firm is recorded in CRSP (the latter has NA value if it still exists in CRSP)
  
  add_data_to_table <- function(merger_id, announcement_date, time_rel_to_anct, firm_name, firm_type,
                                mkt_val_lseg, mkt_val_crsp,
                                industry, firm_status,
                                tic, cusip, permco, permno, gvkey, sic_code, 
                                deal_value, deal_status, merger_auth,
                                abn_ret_rel, abn_ret_rel_csum, abn_ret_rel_sd, 
                                abn_ret_abs, abn_ret_abs_csum, abn_ret_abs_sd,
                                competitors_score, year_hp, 
                                start_date_crsp, end_date_crsp) {
    
    new_row <- data.frame("Merger id"= merger_id, "Announcement date" = as.Date(announcement_date, format="%m-%d-%Y"), 
                          "Trading day relative to anc't" = time_rel_to_anct, 
                          "Firm" = firm_name, "Firm Type" = firm_type,
                          "LSEG Market value 4 weeks prior to announcement (USD, Millions)"  = round(mkt_val_lseg, 3),
                          "CRSP Market value 4 weeks prior to announcement (USD, Millions)"  = round(mkt_val_crsp, 3),
                          "Industry" = industry, "Firm Status" = firm_status,
                          "Ticker"=tic, "CUSIP"=cusip,
                          "Permco" = permco, "Permno" = permno, "GVKEY" = gvkey,
                          "SIC Code" = sic_code, "Deal Value" = deal_value, 
                          "Deal Status" = deal_status,
                          "Regulatory agencies required to approve deal" = merger_auth, 
                          "Relative AR" = round(abn_ret_rel, 2),
                          "Relative CAR" = round(abn_ret_rel_csum, 2),
                          "SD (R-CAR)" = round(abn_ret_rel_sd, 2),
                          "Absolute AR" = round(abn_ret_abs, 2),
                          "Absolute CAR" = round(abn_ret_abs_csum, 2),
                          "SD (A-CAR)" = round(abn_ret_abs_sd, 2),
                          "Competitors' score" = round(competitors_score, 3),
                          "Year of score" = year_hp,
                          "Beggining date in CRSP" = start_date_crsp,
                          "Ending date in CRSP" = end_date_crsp,
                          check.names = FALSE
    ) # create row with the data
    
    final_table <<- rbind(final_table, new_row) # add it to the table
  }
  
  # hereby we define the function which will compute the cumulative abnormal returns 
  get_car <- function(merger_anct_date, stock_ind_data) {
    
    ### This function takes the stock data for the individual firm and S&P500
    # and computes the AR and CAR using both the stock price and stock returns
    # This means the former will be in absolute terms and the latter in relative terms
    
    # We start by filtering both individual and market stock data over estimation plus event window.
    # Since the stock data in CRSP already excludes non-trading days, we can obtain the stock data
    # for the estimation plus event period simply by finding the row number corresponding to the
    # merger announcement date and then slicing the stock data by the estimation and event window
    #
    # In particular, we will go back the number of event window days plus estimation window days
    # (as defined by estimation_window_days and event_window_days) and go forward the number
    # of event window days
    #
    # As previously explained, the event_window_days contains the number of days on each side of the 
    # event period has (i.e. its not the total number of days in the event window). Since it's symmetric,
    # that is, has same number of days before and after the merger date, we only need one value
    #
    # Only estimation_period_days contains the actual total amount of days in the estimation window
    #
    # The only caveat is if the merger announcement itself falls on a non-trading day as it will not be
    # found in CRSP. In those cases, we pick the next trading day according to the NYSE calendar
    # We deal with such cases in the following if-else statement
    
    # if merger announcement week day falls on non-trading day, 
    if( nrow(stock_ind_data %>% slice( which(date == merger_anct_date ) ) ) == 0 ) {
      
      # we want to pick the next trading day according to the NYSE calendar
      # in the same year as the merger announcement. For that end, shall use the bizdays package
      # first grab the year of merger so we can pick the appropriate (NYSE) calendar year
      merger_anct_year <- year(merger_anct_date)
      
      # then import calendars in bizdays package (again, for the respective merger year)
      load_rmetrics_calendars(merger_anct_year)
      
      # finally move to the next trading day as per the NYSE calendar
      # but create an auxiliary variable to store the new merger announcement date
      # since we don't want to overwrite the original one. Instead, we want to output the latter
      # in the final dataset
      merger_anct_date_aux <- adjust.next(as.Date(merger_anct_date), 'Rmetrics/NYSE')
      
      # slicing the stock data for individual firm and S&P500 by estimation and event window
      stock_ind_data <- stock_ind_data %>% slice( (which(date == merger_anct_date_aux) - event_window_days - estimation_window_days):
                                                    (which(date == merger_anct_date_aux) + event_window_days) ) 
      
      stock_mkt_data <- sp500_data %>% slice( (which(date == merger_anct_date_aux) - event_window_days - estimation_window_days):
                                                               (which(date == merger_anct_date_aux) + event_window_days) )
      
      # before proceeding we will compute the market cap of the firm 4 weeks prior to merger announcement as per CRSP
      # we do it here since the market cap is computed with reference to the merger announcement date (which can be
      # either merger announcement date itself or the next trading day in case the former falls on a non-trading day)
      # Note that for the merging parties we already have this data from LSEG so the purpose here is to obtain 
      # it for the rivals
      stock_data_at_merger_anct_date <- stock_ind_data %>% filter( date == merger_anct_date_aux ) # stock data at merger announcement date
      
      # get market cap - notice the number of oustanding shares (shrout) are recorded in thousands
      # thus the market cap is also in thousands
      # but notice we need to correct the market cap to events such as M&As and spin-offs, among others
      #
      # in particular, following WRDS/CRSP documentation, it must be computed according to the following formula
      # market cap = ( abs(stock_price)/cfacpr ) * (shrout*cfacshr)
      # where absolute value of stock price is applied since when it is missing, CRSP uses the bid/ask average
      # but stores it as a negative value
      #
      # cfacpr and cfacshr are the cumulative factor price adjustment and cumulative factor to adjust shares outstanding, 
      # respectively. They are precisely the variables which adjust the market cap to the aforementioned events
      #
      # Usually cfacpr and cfacshr will cancel out such that the market cap simply equals share price times # of outstanding shares,
      # but not always

      mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds <- stock_data_at_merger_anct_date %>% mutate( 
        mkt_val_4_weeks_bef_merger_anct_date_w_crsp = ( abs_prc/cfacpr ) * (shrout*cfacshr) ) %>% # compute the corrected market cap
        pull(mkt_val_4_weeks_bef_merger_anct_date_w_crsp)
      
      # convert to NA if no market cap was computed 
      # NB: we store it globally instead of returning from the function call as we want to access it
      # even if the function raises an error (i.e. OLS regression was not computed due to lack of data)
      mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds <<- ifelse( length(mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds) == 0, NA, 
                                                             mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds )
      
      # in addition, we get the firm's standard industrial classification code (SIC) which 
      # identifies the industry to which the firm belongs to
      # again, we already have this data for the merging parties so it applies mostly to rivals
      # 
      # Furthermore, for the SIC code might change throughout the company's lifetime, we shall pick the one
      # at the time closest to the merger announcement date
      sic_code_w_stock_data_tbl <- stock_data_at_merger_anct_date %>% pull(hsiccd) # grab the SIC code
      
      # convert to NA if no SIC was found (store it globally for the same reason as the aforementioned one)
      sic_code_w_stock_data_tbl <<- ifelse( length(sic_code_w_stock_data_tbl) == 0, NA, sic_code_w_stock_data_tbl )
      
      
    } else{
      # otherwise we slice the stock data directly
      stock_ind_data <- stock_ind_data %>% slice( (which(date == merger_anct_date) - event_window_days - estimation_window_days):
                                                    (which(date == merger_anct_date) + event_window_days) ) 
      
      stock_mkt_data <- sp500_data %>% slice( (which(date == merger_anct_date) - event_window_days - estimation_window_days):
                                                               (which(date == merger_anct_date) + event_window_days) )
      
      # before proceeding we will compute the market cap of the firm 4 weeks prior to merger announcement as per CRSP
      # this time we do so using the merger announcement date itself (as opposed to its next trading day)
      # Note that for the merging parties we already have this data from LSEG so the purpose here is to obtain 
      # it for the rivals
      stock_data_at_merger_anct_date <- stock_ind_data %>% filter( date == merger_anct_date ) # stock data at merger announcement date
      
      # get market cap - notice the number of oustanding shares (shrout) are recorded in thousands
      # thus the market cap is also in thousands
      # but notice we need to correct the market cap to events such as M&As and spin-offs, among others
      #
      # in particular, following WRDS/CRSP documentation, it must be computed according to the following formula
      # market cap = ( abs(stock_price)/cfacpr ) * (shrout*cfacshr)
      # where absolute value of stock price is applied since when it is missing, CRSP uses the bid/ask average
      # but stores it as a negative value
      #
      # cfacpr and cfacshr are the cumulative factor price adjustment and cumulative factor to adjust shares outstanding, 
      # respectively. They are precisely the variables which adjust the market cap to the aforementioned events
      #
      # Usually cfacpr and cfacshr will cancel out such that the market cap simply equals share price times # of outstanding shares,
      # but not always
      
      mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds <- stock_data_at_merger_anct_date %>% mutate( 
        mkt_val_4_weeks_bef_merger_anct_date_w_crsp = ( abs_prc/cfacpr ) * (shrout*cfacshr) ) %>% # compute the corrected market cap
        pull(mkt_val_4_weeks_bef_merger_anct_date_w_crsp)
      
      # convert to NA if no market cap was computed
      mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds <<- ifelse( length(mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds) == 0, NA, 
                                                             mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds )
      
      # in addition, we get the firm's standard industrial classification code (SIC) which 
      # identifies the industry to which the firm belongs to
      # again, we already have this data for the merging parties so it applies mostly to rivals
      # 
      # Furthermore, for the SIC code might change throughout the company's lifetime, we shall pick the one
      # at the time closest to the merger announcement date
      sic_code_w_stock_data_tbl <- stock_data_at_merger_anct_date %>% pull(hsiccd) # grab the SIC code
      
      # convert to NA if no SIC was found
      sic_code_w_stock_data_tbl <<- ifelse( length(sic_code_w_stock_data_tbl) == 0, NA, sic_code_w_stock_data_tbl )
      
      
    }

    # merge the stock data of individual firm ans S&P500 into one single data frame
    reg_data <- stock_ind_data %>% inner_join(stock_mkt_data, by=c("date"))
    
    # now we can compute the abnormal and cumulative abnormal returns
    # starting with AR in absolute terms (i.e. using the stock price)
    
    # run OLS regression on estimation window
    # but first we need the two dates spanning it
    end_estimation <- stock_ind_data[ estimation_window_days , "date" ]
    start_estimation <- stock_ind_data[ 1 , "date" ]
    
    # Notice we use adjusted price (adj_prc) which takes into account changes in the stock price
    # caused by events such as stock splits, mergers or liquidations. These are recorded in CRSP under the
    # variable Cumulative Factor Price adjustment (cfacpr). Thus, to use the correct stock pricing
    # one needs to divide the stock price (prc) by cfacpr  -these are stored in the adjusted price (adj_prc) variable
    # which retrieves the stock data for all firms in the M&A dataset
    #
    # spindx is the level of the S&P500 composite index without including dividends
    # regress the two for the estimation window to obtain the market-beta
    mkt_reg_abs <- lm(adj_prc ~ spindx, reg_data, subset = (date >= start_estimation & date <= end_estimation))
    
    # estimate market-predicted price for event period
    # but first we need the two dates spanning it
    event_end <- stock_ind_data[ nrow(stock_ind_data) , "date" ]
    event_start <- stock_ind_data[ nrow(stock_ind_data) - event_window_days*2, "date" ]
    
    # run regression
    market_stock_price <- reg_data %>% select(date, spindx) %>% filter(date >= event_start & date <= event_end) %>% select(spindx)
    # obtain the counterfactual - i.e. price implied by the market-beta
    estimated_price <- predict(mkt_reg_abs, newdata = market_stock_price)
    
    # get the actual price observed
    actual_price <- reg_data %>% select(date, adj_prc) %>% filter(date >= event_start & date <= event_end) %>% pull(adj_prc)
    
    # Compute cumulative abnormal returns in absolute terms by taking the difference of the 2
    abnormal_returns_abs <- actual_price - estimated_price
    abnormal_returns_abs_csum <- cumsum(abnormal_returns_abs)
    #abnormal_returns_abs_avg <- sum(abnormal_returns_abs)/event_window_days
    abnormal_returns_abs_sd <- sd(abnormal_returns_abs_csum)
    
    #Computing t-test
    #t_test_abs <- t.test(abnormal_returns_abs_csum, mu=0)
    
    # Analogous code for abnormal returns using stock returns 
    # ret is the stock returns for a firm in the M&A dataset which includes dividends
    # notice it has already been corrected by CRSP for any events such as stock splits and spin-offs
    # so no further transformation is required here
    #
    # vwretd is the value-weighted returns of the S&P500 with dividends
    #
    # run regression on estimation window
    mkt_reg_ret <- lm(ret ~ vwretd, reg_data, subset = (date >= start_estimation & date <= end_estimation) )
    
    # predict the returns implied by the market-beta
    market_stock_ret <- reg_data %>% select(date, vwretd) %>% filter(date >= event_start & date <= event_end) %>% select(vwretd)
    estimated_ret <- predict(mkt_reg_ret, newdata = market_stock_ret)
    
    # get the actual observed returns
    actual_ret <- reg_data %>% select(date, ret) %>% filter(date >= event_start & date <= event_end) %>% pull(ret)
    
    # take the difference between the 2
    abnormal_returns_rel <- actual_ret - estimated_ret
    abnormal_returns_rel_csum <- cumsum(abnormal_returns_rel)
    #abnormal_returns_rel_avg <- cummean(abnormal_returns_rel)
    abnormal_returns_rel_sd <- sd(abnormal_returns_rel_csum)
    
    #t_test_rel <- t.test(abnormal_returns_rel_csum, mu=0)
    
    # storing the trading day relative to merger announcement day
    time_rel_to_anct <- c(-event_window_days:event_window_days)
    
    # store all the abnormal returns in a list
    return(list(abnormal_returns_abs = abnormal_returns_abs,
                abnormal_returns_abs_csum = abnormal_returns_abs_csum,
                abnormal_returns_abs_sd = abnormal_returns_abs_sd,
                abnormal_returns_rel = abnormal_returns_rel,
                abnormal_returns_rel_csum = abnormal_returns_rel_csum,
                abnormal_returns_rel_sd = abnormal_returns_rel_sd,
                time_rel_to_anct = time_rel_to_anct
                ) )
    
  }
  
  # now we are ready to loop through each merger and compute the abnormal returns
  # defining the number of mergers
  num_of_mergers <- all_reg_id_info %>% distinct(merger_id) %>% summarise(c=n()) %>% pull(c)
  
  # and incrementally store the data in a CSV file (one for each batch)
  if (k == 1) {
    output_final_table <- write.csv(final_table ,"data/processed_data/car/1989_99_car_data.csv", row.names = F)
  } else if (k==2) {
    output_final_table <- write.csv(final_table ,"data/processed_data/car/00_09_car_data.csv", row.names = F)
  } else {
    output_final_table <- write.csv(final_table ,"data/processed_data/car/10_23_car_data.csv", row.names = F)
  }
  
  # running the loop for each merger
  for (i in 1:num_of_mergers) {
    
    # filter the dataframe with the identifying information for the merger in the loop
    merger_data <- all_reg_id_info %>% filter( merger_id == i )
    
    # get the merger announcement date
    merger_anct_date <- merger_data %>% distinct(date_announced) %>% pull()
    
    # retrieve its identifying info that will be appended to the final output table
    merger_id <- merger_data %>% distinct(merger_id) %>% pull()
    acq_name <- merger_data %>% distinct(acquiror_full_name) %>% pull()
    acq_mkt_cap_4_weeks_bef_merger_anct_date_w_lseg_in_mil <- merger_data %>% distinct(acquiror_market_value_4_weeks_prior_to_announcement) %>% pull() # market cap obtained from LSEG workspace (in usd, millions)
    acq_sic <- merger_data %>% distinct(acq_sic_code) %>% pull() # SIC stands for standard industrial classification code (is a 3 to 4-digit code 
    # identifying the firm's industry)
    acq_industry <- merger_data %>% distinct(acquiror_macro_industry) %>% pull()
    acq_permco <- merger_data %>% distinct(acq_permco) %>% pull()
    acq_permno <- merger_data %>% distinct(acq_permno) %>% pull()
    acq_tck <- merger_data %>% distinct(acquiror_primary_ticker_symbol) %>% pull()
    acq_cusip <- merger_data %>% distinct(acquiror_6_digit_cusip) %>% pull()
    acq_start_date_in_crsp <- merger_data %>% distinct(acq_start_date_in_crsp) %>% pull() # date the acquiror's stock data began in CRSP
    acq_end_date_in_crsp <- merger_data %>% distinct(acq_end_date_in_crsp) %>% pull() # date the acquiror's stock data ended in CRSP
    acq_public_status <- merger_data %>% distinct(acquiror_public_status) %>% pull()
    reg_agency <- merger_data %>% distinct(regulatory_agencies_required_to_approve_deal) %>% pull() # regulatory agency responsible for the merger decision
    deal_val <- merger_data %>% distinct(deal_value) %>% pull()
    deal_status <- merger_data %>% distinct(m_a_type) %>% pull()
    
    # lastly we want the competitor score shared by the acquiror with the target, if it exists
    # grab their GVKEYs
    acq_gvkey <- merger_data %>% distinct(acq_gvkey) %>% pull()  
    tgt_gvkey <- merger_data %>% distinct(tgt_gvkey) %>% pull()
    
    # if the gvkey of acquiror and target was found and they are rivals as defined by the Hoberg-Philips dataset,
    # meaning they share a competitor score, then retrieve both their competitor score and the year that score refers to.
    # Otherwise, just assign an NA to their rival score and year
    # getting the score
    acq_tgt_rival_score <- ifelse( length(acq_gvkey) > 0 & length(tgt_gvkey) > 0, 
                                   competitors_txt_def %>% filter( (gvkey1==acq_gvkey & gvkey2==tgt_gvkey) | 
                                                                     (gvkey1==tgt_gvkey & gvkey2==acq_gvkey) ) %>% 
                                     slice_max(order_by = year, n=1, with_ties = FALSE) %>% pull(score), 
                                   NA ) 
    
    # getting the year that score refers to
    acq_tgt_rival_year_hp <- ifelse( length(acq_gvkey) > 0 & length(tgt_gvkey) > 0, 
                                     competitors_txt_def %>% filter( (gvkey1==acq_gvkey & gvkey2==tgt_gvkey) | 
                                                                       (gvkey1==tgt_gvkey & gvkey2==acq_gvkey) ) %>% 
                                       slice_max(order_by = year, n=1, with_ties = FALSE) %>% pull(year), 
                                     NA ) 
    
    ### ESTIMATING ABNORMAL AND CUMULATIVE ABNORMAL RETURNS
    
    # if permanent company number (permco) was found in CRSP then attempt to run OLS regression
    # notice that if the permco exists so does the permno, so we only need one here
    if (!is.na(acq_permco)) {
      
      # if no error is raised upon running the OLS regression,
      tryCatch( {
        
        # Get the stock data using both the permanent company number (permco) and permanent issue number (permno)
        #
        # recall that the former is a CRSP unique company level identifier, while the latter is a unique share class identifier (within that firm).
        # We need both since one company may have multiple share classes listed at the same time, thus
        # having recorded multiple stock prices in the same date in CRSP. To uniquely pin down the firm
        # and the stock we need both identifiers.
        # filtering the stock data by that firm
        individual_stock_data <- acq_stock_data %>% filter(permco == acq_permco & permno == acq_permno) %>% arrange(date)
        individual_stock_data$date <- as.Date(individual_stock_data$date, format = "%Y-%m-%d")
        
        # Estimate abnormal and cumulative abnormal returns (CAR)
        acq_ar_and_ttest <- get_car(merger_anct_date, individual_stock_data)
        
        ### Setting up final output table
        
        #Extract the average and standard deviation of CAR estimates
        acq_abn_ret_rel <- acq_ar_and_ttest$abnormal_returns_rel
        acq_abn_ret_rel_csum <- acq_ar_and_ttest$abnormal_returns_rel_csum
        acq_abn_ret_rel_sd <- acq_ar_and_ttest$abnormal_returns_rel_sd
        
        acq_abn_ret_abs <- acq_ar_and_ttest$abnormal_returns_abs
        acq_abn_ret_abs_csum <- acq_ar_and_ttest$abnormal_returns_abs_csum
        acq_abn_ret_abs_sd <- acq_ar_and_ttest$abnormal_returns_abs_sd
        
        acq_time_rel_to_anct <- acq_ar_and_ttest$time_rel_to_anct
        
        # and grab the market cap (in millions, USD) as well as sic code (they were both stored globally in the function
        # so we can still access it even if the function raised an error - this applies to the next if-condition as we will
        # never get to this point if the function raised an error)
        acq_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds <- mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds
        # convert market cap to millions
        acq_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil <- acq_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds/1000 
        acq_sic_w_stock_data_tbl <- sic_code_w_stock_data_tbl
        
        # note that for the merging parties we already have the SIC code from the table with the identifying information
        # while the the one we just retrieved did so using the stock data table (as previously mentioned, the main purpose
        # was to get it for the rivals)
        # in principle they should be the same, so here we will only resort to the latter if the former is NA
        acq_sic <- coalesce(acq_sic, acq_sic_w_stock_data_tbl) 
        
        # lastly, once we have the SIC we can use it to find the industry name 
        # the SIC-industry match is performed using SEC classification table
        # if matching failed, we keep the industry name we already had from LSEG
        acq_industry <- data_to_map_sic_to_industry %>% filter( sic_code == acq_sic ) %>% summarize(industry_name = ifelse(n() == 1, 
                                                                                                                           paste(acq_industry, industry_name, sep = "|"), 
                                                                                                                           acq_industry ) ) %>% 
          pull(industry_name)
        
        #Add acquiror to the table
        add_data_to_table(merger_id, merger_anct_date, acq_time_rel_to_anct, 
                          acq_name, "A", 
                          acq_mkt_cap_4_weeks_bef_merger_anct_date_w_lseg_in_mil, acq_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil,
                          acq_industry, acq_public_status,
                          acq_tck, acq_cusip, 
                          acq_permco, acq_permno, acq_gvkey, acq_sic,
                          deal_val, deal_status, reg_agency, 
                          acq_abn_ret_rel,
                          acq_abn_ret_rel_csum, acq_abn_ret_rel_sd,
                          acq_abn_ret_abs,
                          acq_abn_ret_abs_csum, acq_abn_ret_abs_sd,
                          acq_tgt_rival_score,
                          acq_tgt_rival_year_hp,
                          acq_start_date_in_crsp,
                          acq_end_date_in_crsp) 
        
      }, error = function(e) {
        # if en error is raised (due to insufficient data), 
        # just assign NA code -98 and store in output table with the relevant identifying information
        
        # and grab the market cap as well as sic code (we can still get that data even if the function raised an error
        # since we defined both variables globally)
        ### ACTUALLY THAT IS NOT TRUE AND CALLING THESE 2 WILL RUN INTO AN ERROR WHEN THE FUNCTION CALL ALREADY
        # RAISED AN ERROR - AND MAKE SAME CORRECTION FOR TARGETS
        # acq_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds <- mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds
        # convert market cap to millions
        # acq_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil <- acq_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds/1000 
        acq_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil <- NA
        # acq_sic_w_stock_data_tbl <- sic_code_w_stock_data_tbl
        acq_sic_w_stock_data_tbl <- NA
        
        # note that for the acquirors we already have the SIC code from the table with the identifying information
        # while the the one we just retrieved did so using the stock data table (as previously mentioned, the main purpose
        # was to get it for the rivals)
        # in principle they should be the same, so here we will only resort to the latter if the former is NA
        acq_sic <- coalesce(acq_sic, acq_sic_w_stock_data_tbl) 
        
        # lastly, once we have the SIC we can use it to find the industry name 
        # the SIC-industry match is performed using SEC classification table
        # if matching failed, we keep the industry name we already had from LSEG
        acq_industry <- data_to_map_sic_to_industry %>% filter( sic_code == acq_sic ) %>% summarize(industry_name = ifelse(n() == 1, 
                                                                                                                           paste(acq_industry, industry_name, sep = "|"), 
                                                                                                                           acq_industry ) ) %>% 
          pull(industry_name)
        
        add_data_to_table(merger_id, merger_anct_date, NA, 
                          acq_name, "A", 
                          acq_mkt_cap_4_weeks_bef_merger_anct_date_w_lseg_in_mil, acq_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil,
                          acq_industry, acq_public_status,
                          acq_tck, acq_cusip, 
                          acq_permco, acq_permno, acq_gvkey, acq_sic,
                          deal_val, deal_status, reg_agency, 
                          -98, -98, -98, -98, -98, -98,
                          acq_tgt_rival_score,
                          acq_tgt_rival_year_hp,
                          acq_start_date_in_crsp,
                          acq_end_date_in_crsp) 
      }
      )
    } else {
      # othwerwise if no permco was found
      
      
      # assign an NA code -99 to the final table
      # note that if no permco was found then we have no way of obtaining the market cap or the SIC code
      # so the former is just an NA while the latter will simply be that retrieved using the table with 
      # the stock identifying information
      # and we simply keep the industry name from LSEG
      
      add_data_to_table(merger_id, merger_anct_date, NA, 
                        acq_name, "A", 
                        acq_mkt_cap_4_weeks_bef_merger_anct_date_w_lseg_in_mil, NA,
                        acq_industry, acq_public_status,
                        acq_tck, acq_cusip, 
                        acq_permco, acq_permno, acq_gvkey, acq_sic,
                        deal_val, deal_status, reg_agency, 
                        -99, -99, -99, -99, -99, -99,
                        acq_tgt_rival_score,
                        acq_tgt_rival_year_hp,
                        acq_start_date_in_crsp,
                        acq_end_date_in_crsp)
      
    }
    
    # now for its competitors
    merger_data_by_acq_score <- merger_data %>% arrange(desc(acq_rival_score))
    riv_permcos <- merger_data_by_acq_score %>% distinct(acq_rival_permco) %>% pull()
    # ensuring that we have at most 5 competitors
    riv_permcos <- riv_permcos[1:min(length(riv_permcos), num_of_rivals_per_firm)]
    
    # grabbing their identifying information
    riv_permnos <- merger_data_by_acq_score %>% distinct(acq_rival_permno) %>% pull()
    riv_names <- merger_data_by_acq_score %>% distinct(acq_rival_name) %>% pull()
    riv_tcks <- merger_data_by_acq_score %>% distinct(acq_rival_ticker) %>% pull()
    riv_cusips <- merger_data_by_acq_score %>% distinct(acq_rival_cusip) %>% pull()
    riv_gvkeys <- merger_data_by_acq_score %>% distinct(acq_rival_gvkey) %>% pull()
    riv_scores <- merger_data_by_acq_score %>% distinct(acq_rival_score) %>% pull()
    riv_start_dates <- merger_data_by_acq_score %>% distinct(acq_rival_start_date) %>% pull() # date the rival's stock data began in CRSP
    riv_end_dates <- merger_data_by_acq_score %>% distinct(acq_rival_end_date) %>% pull() # date the rival's stock data ended in CRSP
    
    # estimating abnormal returns for each competitor of the acquiror
    
    for (j in seq_along(riv_permcos)) {
      
      # grabbing the permco and identifying information for each rival of the acquiror
      riv_permco <- riv_permcos[j]
      riv_permno <- riv_permnos[j]
      riv_name <- riv_names[j]
      riv_tck <- riv_tcks[j]
      riv_cusip <- riv_cusips[j]
      riv_gvkey <- riv_gvkeys[j]
      riv_score <- riv_scores[j]
      riv_year_hp <- ifelse( !is.na(riv_score), merger_data_by_acq_score %>% filter(acq_rival_score == riv_score) %>% distinct(acq_year_hp) %>% pull(),
                             NA )
      riv_start_date <- riv_start_dates[j]
      riv_end_date <- riv_end_dates[j]
      
      # if permanent company number (permco) was found in CRSP then attempt to run OLS regression,
      if (!is.na(riv_permco)) {
        
        # if no error is raised upon running the OLS regression,
        tryCatch( {
          
          # Get the stock data using both the permanent company number (permco) and permanent issue number (permno)
          individual_stock_data <- acq_stock_data %>% filter(permco == riv_permco & permno == riv_permno) %>% arrange(date)
          individual_stock_data$date <- as.Date(individual_stock_data$date, format = "%Y-%m-%d")
          
          # Estimate abnormal and cumulative abnormal returns (CAR)
          riv_ar_and_ttest <- get_car(merger_anct_date, individual_stock_data)
          
          ### Setting up final output table
          
          #Extract the average and standard deviation of CAR estimates
          riv_abn_ret_rel <- riv_ar_and_ttest$abnormal_returns_rel
          riv_abn_ret_rel_csum <- riv_ar_and_ttest$abnormal_returns_rel_csum
          riv_abn_ret_rel_sd <- riv_ar_and_ttest$abnormal_returns_rel_sd
          
          riv_abn_ret_abs <- riv_ar_and_ttest$abnormal_returns_abs
          riv_abn_ret_abs_csum <- riv_ar_and_ttest$abnormal_returns_abs_csum
          riv_abn_ret_abs_sd <- riv_ar_and_ttest$abnormal_returns_abs_sd
          
          riv_time_rel_to_anct <- riv_ar_and_ttest$time_rel_to_anct
          
          # and grab the market cap as well as sic code (they were both stored globally in the function
          # so we can still access it even if the function raised an error - this applies to the next if-condition as we will
          # never get to this point if the function raised an error)
          riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds <- mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds
          # convert market cap to millions
          riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil <- riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds/1000
          riv_sic <- sic_code_w_stock_data_tbl
          
          # lastly, once we have the SIC we can use it to find the industry name 
          # the SIC-industry match is performed using SEC classification table
          # or assign NA if the SIC code was unmatched
          riv_industry <- data_to_map_sic_to_industry %>% filter( sic_code == riv_sic ) %>% summarize(industry_name = ifelse(n() == 1, industry_name, NA_character_)) %>% 
            pull(industry_name)
          
          #Add rival to the output table
          add_data_to_table(merger_id, merger_anct_date, riv_time_rel_to_anct, 
                            riv_name, "C_A", NA, riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil,
                            riv_industry, NA,
                            riv_tck, riv_cusip, 
                            riv_permco, riv_permno, riv_gvkey, riv_sic,
                            NA, NA, NA,
                            riv_abn_ret_rel,
                            riv_abn_ret_rel_csum, riv_abn_ret_rel_sd,
                            riv_abn_ret_abs,
                            riv_abn_ret_abs_csum, riv_abn_ret_abs_sd,
                            riv_score,
                            riv_year_hp,
                            riv_start_date,
                            riv_end_date) 

        }, error = function(e) {
          # if error is raised (i.e due to insufficient data), 
          # just assign NA code -98 and store in output table
          
          # and grab the market cap as well as SIC code (we can still get that data even if the function raised an error
          # since we both variables globally)
          # riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds <- mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds
          # convert market cap to millions
          # riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil <- riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds/1000
          riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil <- NA
          # riv_sic <- sic_code_w_stock_data_tbl
          riv_sic <- NA
          
          # lastly, once we have the SIC we can use it to find the industry name 
          # the SIC-industry match is performed using SEC classification table
          # or assign NA if the SIC code was unmatched
          riv_industry <- data_to_map_sic_to_industry %>% filter( sic_code == riv_sic ) %>% summarize(industry_name = ifelse(n() == 1, industry_name, NA_character_)) %>% 
            pull(industry_name)
          
          add_data_to_table(merger_id, merger_anct_date, NA, 
                            riv_name, "C_A", NA, riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil,
                            riv_industry, NA,
                            riv_tck, riv_cusip, 
                            riv_permco, riv_permno, riv_gvkey, riv_sic,
                            NA, NA, NA, 
                            -98, -98, -98, -98, -98, -98,
                            riv_score,
                            riv_year_hp,
                            riv_start_date,
                            riv_end_date) 
        }
        )
      } # otherwise if no permco was found 
      else {
        # assign an NA code -99 to the stock
        # and the market cap, sic and industry will just be NAs
        add_data_to_table(merger_id, merger_anct_date, NA, 
                          riv_name, "C_A", NA, NA,
                          NA, NA,
                          riv_tck, riv_cusip, 
                          riv_permco, riv_permno, riv_gvkey, NA,
                          NA, NA, NA,
                          -99, -99, -99, -99, -99, -99,
                          riv_score,
                          riv_year_hp,
                          riv_start_date,
                          riv_end_date) 
      }
    }
    
    ### Same thing for the target firm
    
    tgt_name <- merger_data %>% distinct(target_full_name) %>% pull()
    tgt_mkt_cap_4_weeks_bef_merger_anct_date_w_lseg_in_mil <- merger_data %>% distinct(target_market_value_4_weeks_prior_to_announcement) %>% pull()
    tgt_sic <- merger_data %>% distinct(tgt_sic_code) %>% pull()
    tgt_industry <- merger_data %>% distinct(target_macro_industry) %>% pull()
    tgt_permco <- merger_data %>% distinct(tgt_permco) %>% pull()
    tgt_permno <- merger_data %>% distinct(tgt_permno) %>% pull()
    tgt_tck <- merger_data %>% distinct(target_primary_ticker_symbol) %>% pull()
    tgt_cusip <- merger_data %>% distinct(target_6_digit_cusip) %>% pull()
    tgt_start_date_in_crsp <- merger_data %>% distinct(tgt_start_date_in_crsp) %>% pull()
    tgt_end_date_in_crsp <- merger_data %>% distinct(tgt_end_date_in_crsp) %>% pull()
    tgt_public_status <- merger_data %>% distinct(target_public_status) %>% pull()
    
    # if permanent code (permco) was found in CRSP attempt to run the OLS regression,
    if (!is.na(tgt_permco)) {
      
      # if no error is raised upon running the OLS regression,
      tryCatch( {
        
        # Get the stock data using both the permanent company number (permco) and permanent issue number (permno)
        individual_stock_data <- tgt_stock_data %>% filter(permco == tgt_permco & permno == tgt_permno) %>% arrange(date)
        individual_stock_data$date <- as.Date(individual_stock_data$date, format = "%Y-%m-%d")
        
        # Estimate abnormal and cumulative abnormal returns (CAR)
        tgt_ar_and_ttest <- get_car(merger_anct_date, individual_stock_data)
        
        ### Setting up final output table
        
        #Extract the average and standard deviation of CAR estimates
        tgt_abn_ret_rel <- tgt_ar_and_ttest$abnormal_returns_rel
        tgt_abn_ret_rel_csum <- tgt_ar_and_ttest$abnormal_returns_rel_csum
        tgt_abn_ret_rel_sd <- tgt_ar_and_ttest$abnormal_returns_rel_sd
        
        tgt_abn_ret_abs <- tgt_ar_and_ttest$abnormal_returns_abs
        tgt_abn_ret_abs_csum <- tgt_ar_and_ttest$abnormal_returns_abs_csum
        tgt_abn_ret_abs_sd <- tgt_ar_and_ttest$abnormal_returns_abs_sd
        
        tgt_time_rel_to_anct <- tgt_ar_and_ttest$time_rel_to_anct
        
        # and grab the market cap as well as sic code (they were both stored globally in the function
        # so we can still access it even if the function raised an error - this applies to the next if-condition as we will
        # never get to this point if the function raised an error)
        tgt_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds <- mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds
        # convert market cap to millions
        tgt_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil <- tgt_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds/1000
        tgt_sic_w_stock_data_tbl <- sic_code_w_stock_data_tbl
        
        # note that for the merging parties we already have the SIC code from the table with the identifying information
        # while the the one we just retrieved did so using the stock data table (as previously mentioned, the main purpose
        # was to get it for the rivals)
        # in principle they should be the same, so here we will only resort to the latter if the former is NA
        tgt_sic <- coalesce(tgt_sic, tgt_sic_w_stock_data_tbl) 
        
        # lastly, once we have the SIC we can use it to find the industry name 
        # the SIC-industry match is performed using SEC classification table
        # if matching failed, we keep the industry name we already had from LSEG
        tgt_industry <- data_to_map_sic_to_industry %>% filter( sic_code == tgt_sic ) %>% summarize(industry_name = ifelse(n() == 1, 
                                                                                                                           paste(tgt_industry, industry_name, sep = "|"), 
                                                                                                                           tgt_industry)) %>% 
          pull(industry_name)
        
        #Add target to the table
        add_data_to_table(merger_id, merger_anct_date, tgt_time_rel_to_anct, 
                          tgt_name, "T", 
                          tgt_mkt_cap_4_weeks_bef_merger_anct_date_w_lseg_in_mil, tgt_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil,
                          tgt_industry, tgt_public_status,
                          tgt_tck, tgt_cusip, 
                          tgt_permco, tgt_permno, tgt_gvkey, tgt_sic,
                          deal_val, deal_status, reg_agency, 
                          tgt_abn_ret_rel,
                          tgt_abn_ret_rel_csum, tgt_abn_ret_rel_sd,
                          tgt_abn_ret_abs,
                          tgt_abn_ret_abs_csum, tgt_abn_ret_abs_sd,
                          acq_tgt_rival_score,
                          acq_tgt_rival_year_hp,
                          tgt_start_date_in_crsp,
                          tgt_end_date_in_crsp)
        
      }, error = function(e) {
        # if error is raised (i.e due to insufficient data), 
        # just assign NA code -98 and store in output table
        
        # and grab the market cap as well as sic code (we can still get that data even if the function raised an error
        # since we both variables globally)
        # tgt_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds <- mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds
        # convert market cap to millions
        # tgt_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil <- tgt_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds/1000
        tgt_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil <- NA
        # tgt_sic_w_stock_data_tbl <- sic_code_w_stock_data_tbl
        tgt_sic_w_stock_data_tbl <- NA
        
        # note that for the merging parties we already have the SIC code from the table with the identifying information
        # while the the one we just retrieved did so using the stock data table (as previously mentioned, the main purpose
        # was to get it for the rivals)
        # in principle they should be the same, so here we will only resort to the latter if the former is NA
        tgt_sic <- coalesce(tgt_sic, tgt_sic_w_stock_data_tbl)
        
        # lastly, once we have the SIC we can use it to find the industry name 
        # the SIC-industry match is performed using SEC classification table
        # if matching failed, we keep the industry name we already had from LSEG
        tgt_industry <- data_to_map_sic_to_industry %>% filter( sic_code == tgt_sic ) %>% summarize(industry_name = ifelse(n() == 1, 
                                                                                                                           paste(tgt_industry, industry_name, sep = "|"), 
                                                                                                                           tgt_industry) ) %>% 
          pull(industry_name)
        
        add_data_to_table(merger_id, merger_anct_date, NA, 
                          tgt_name, "T", 
                          tgt_mkt_cap_4_weeks_bef_merger_anct_date_w_lseg_in_mil, tgt_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil,
                          tgt_industry, tgt_public_status,
                          tgt_tck, tgt_cusip, 
                          tgt_permco, tgt_permno, tgt_gvkey, tgt_sic,
                          deal_val, deal_status, reg_agency, 
                          -98, -98, -98, -98, -98, -98,
                          acq_tgt_rival_score,
                          acq_tgt_rival_year_hp,
                          tgt_start_date_in_crsp,
                          tgt_end_date_in_crsp) 
        
      }
      )
    } else {
      # otherwise if no permco was found
      
      
      # assign an NA code -99 to the stock
      # note that if no permco was found then we have no way of obtaining the market cap or the SIC code
      # so the former is just an NA while the latter will simply be that retrieved using the table with 
      # the stock identifying information
      # and we simply keep the industry name from LSEG
      add_data_to_table(merger_id, merger_anct_date, NA, 
                        tgt_name, "T", 
                        tgt_mkt_cap_4_weeks_bef_merger_anct_date_w_lseg_in_mil, NA,
                        tgt_industry, tgt_public_status,
                        tgt_tck, tgt_cusip, 
                        tgt_permco, tgt_permno, tgt_gvkey, tgt_sic,
                        deal_val, deal_status, reg_agency, 
                        -99, -99, -99, -99, -99, -99,
                        acq_tgt_rival_score,
                        acq_tgt_rival_year_hp,
                        tgt_start_date_in_crsp,
                        tgt_end_date_in_crsp)
      
    }
    
    # now for its competitors
    merger_data_by_tgt_score <- merger_data %>% arrange(desc(tgt_rival_score))
    riv_permcos <- merger_data_by_tgt_score %>% distinct(tgt_rival_permco) %>% pull()
    # ensuring that we have at most 5 competitors
    riv_permcos <- riv_permcos[1:min(length(riv_permcos), num_of_rivals_per_firm)]
    
    # grabbing their identifying information
    riv_permnos <- merger_data_by_tgt_score %>% distinct(tgt_rival_permno) %>% pull()
    riv_names <- merger_data_by_tgt_score %>% distinct(tgt_rival_name) %>% pull()
    riv_tcks <- merger_data_by_tgt_score %>% distinct(tgt_rival_ticker) %>% pull()
    riv_cusips <- merger_data_by_tgt_score %>% distinct(tgt_rival_cusip) %>% pull()
    riv_gvkeys <- merger_data_by_tgt_score %>% distinct(tgt_rival_gvkey) %>% pull()
    riv_scores <- merger_data_by_tgt_score %>% distinct(tgt_rival_score) %>% pull()
    riv_start_dates <- merger_data_by_tgt_score %>% distinct(tgt_rival_start_date) %>% pull()
    riv_end_dates <- merger_data_by_tgt_score %>% distinct(tgt_rival_end_date) %>% pull()
    
    # estimating abnormal returns for each competitor of the target
     
    for (j in seq_along(riv_permcos)) {
      
      # grabbing the permco and identifying information for each target's rival
      riv_permco <- riv_permcos[j]
      riv_permno <- riv_permnos[j]
      riv_name <- riv_names[j]
      riv_tck <- riv_tcks[j]
      riv_cusip <- riv_cusips[j]
      riv_gvkey <- riv_gvkeys[j]
      riv_score <- riv_scores[j]
      riv_year_hp <- ifelse( !is.na(riv_score), merger_data_by_tgt_score %>% filter(tgt_rival_score == riv_score) %>% distinct(tgt_year_hp) %>% pull(),
                             NA )
      riv_start_date <- riv_start_dates[j]
      riv_end_date <- riv_end_dates[j]
      
      # if permanent company number (permco) was found in CRSP then attempt to run OLS regression,
      if (!is.na(riv_permco)) {
        
        # if no error is raised upon running the OLS regression,
        tryCatch( {
          
          # Get the stock data using both the permanent company number (permco) and permanent issue number (permno)
          individual_stock_data <- tgt_stock_data %>% filter(permco == riv_permco & permno == riv_permno) %>% arrange(date)
          individual_stock_data$date <- as.Date(individual_stock_data$date, format = "%Y-%m-%d")
          
          # Estimate abnormal and cumulative abnormal returns (CAR)
          riv_ar_and_ttest <- get_car(merger_anct_date, individual_stock_data)
          
          ### Setting up final output table
          
          #Extract the average and standard deviation of CAR estimates
          riv_abn_ret_rel <- riv_ar_and_ttest$abnormal_returns_rel
          riv_abn_ret_rel_csum <- riv_ar_and_ttest$abnormal_returns_rel_csum
          riv_abn_ret_rel_sd <- riv_ar_and_ttest$abnormal_returns_rel_sd
          
          riv_abn_ret_abs <- riv_ar_and_ttest$abnormal_returns_abs
          riv_abn_ret_abs_csum <- riv_ar_and_ttest$abnormal_returns_abs_csum
          riv_abn_ret_abs_sd <- riv_ar_and_ttest$abnormal_returns_abs_sd
          
          riv_time_rel_to_anct <- riv_ar_and_ttest$time_rel_to_anct
          
          # and grab the market cap as well as sic code (they were both stored globally in the function
          # so we can still access it even if the function raised an error - this applies to the next if-condition as we will
          # never get to this point if the function raised an error)
          riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds <- mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds
          # convert market cap to millions
          riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil <- riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds/1000
          riv_sic <- sic_code_w_stock_data_tbl
          
          # lastly, once we have the SIC we can use it to find the industry name 
          # the SIC-industry match is performed using SEC classification table
          # or assign NA if the SIC code was unmatched
          riv_industry <- data_to_map_sic_to_industry %>% filter( sic_code == riv_sic ) %>% summarize(industry_name = ifelse(n() == 1, industry_name, NA_character_)) %>% 
            pull(industry_name)
          
          #Add rival to the table
          add_data_to_table(merger_id, merger_anct_date, riv_time_rel_to_anct, 
                            riv_name, "C_T", NA, riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil,
                            riv_industry, NA,
                            riv_tck, riv_cusip, 
                            riv_permco, riv_permno, riv_gvkey, riv_sic,
                            NA, NA, NA,
                            riv_abn_ret_rel,
                            riv_abn_ret_rel_csum, riv_abn_ret_rel_sd,
                            riv_abn_ret_abs,
                            riv_abn_ret_abs_csum, riv_abn_ret_abs_sd,
                            riv_score,
                            riv_year_hp,
                            riv_start_date,
                            riv_end_date) 
          
        }, error = function(e) {
          # if error is raised (i.e due to insufficient data), 
          # just assign NA code -98 and store in output table
          
          # and grab the market cap as well as SIC code (we can still get that data even if the function raised an error
          # since we both variables globally)
          # riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds <- mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds
          # convert market cap to millions
          # riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil <- riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_thnds/1000
          riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil <- NA
          # riv_sic <- sic_code_w_stock_data_tbl
          riv_sic <- NA
          # lastly, once we have the SIC we can use it to find the industry name 
          # the SIC-industry match is performed using SEC classification table
          # or assign NA if the SIC code was unmatched
          riv_industry <- data_to_map_sic_to_industry %>% filter( sic_code == riv_sic ) %>% summarize(industry_name = ifelse(n() == 1, industry_name, NA_character_)) %>% 
            pull(industry_name)
          
          add_data_to_table(merger_id, merger_anct_date, NA, 
                            riv_name, "C_T", NA, riv_mkt_cap_4_weeks_bef_merger_anct_date_w_crsp_in_mil,
                            riv_industry, NA,
                            riv_tck, riv_cusip, 
                            riv_permco, riv_permno, riv_gvkey, riv_sic,
                            NA, NA, NA, 
                            -98, -98, -98, -98, -98, -98,
                            riv_score,
                            riv_year_hp,
                            riv_start_date,
                            riv_end_date)

        }
        )
      } # otherwise if no permco was found 
      else {
        # assign an NA code -99 to the stock
        # and the market cap, sic and industry will just be NAs
        add_data_to_table(merger_id, merger_anct_date, NA, 
                          riv_name, "C_T", NA, NA,
                          NA, NA,
                          riv_tck, riv_cusip, 
                          riv_permco, riv_permno, riv_gvkey, NA,
                          NA, NA, NA,
                          -99, -99, -99, -99, -99, -99,
                          riv_score,
                          riv_year_hp,
                          riv_start_date,
                          riv_end_date) 
      }
      
    }
    
    
    # save the car dataset (for each batch)
    if (k == 1) {
      write.table(final_table, file = "data/processed_data/car/1989_99_car_data.csv", append = TRUE, sep = ",", col.names = T, row.names = FALSE) 
    } else if (k==2) {
      write.table(final_table, file = "data/processed_data/00_09_car_data.csv", append = TRUE, sep = ",", col.names = T, row.names = FALSE) 
    } else {
      write.table(final_table, file = "data/processed_data/10_23_car_data.csv", append = TRUE, sep = ",", col.names = T, row.names = FALSE) 
    }
    
    # and delete final table contents
    final_table <- final_table[0, ]
    
  }
  
  # weed out duplicated NA rows 
  # final_table <- final_table %>% distinct(`Merger id`, `Firm type`, `Firm`, `Trading day relative to anc't`, .keep_all = T)
  
  # View(final_table)
  
  # save the fianl dataset as RDS file
  # saveRDS(final_table, file = "data/processed_data/car_dataset.rds" )
  
  # and as CSV file
  # write.csv(final_table, "data/processed_data/car_dataset.csv", row.names = FALSE)
  
  # finally convert and save table in a neat output file
  # car_dataset <- final_table %>% gt() %>%
  #   tab_header(
  #    title = "Cumulative abnormal returns of acquirors, targets and rivals"
  # )
  # gtsave(car_dataset, "output/car_data_table.html")
  # 

}

# NB: for privacy purposes the WRDS user and pass should be stored as environment variables
# To store your own credentials (otherwise code will not run), open Windows Powershell and run:  
# Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "WRDS_USER=your_username"
# Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "WRDS_PASS=your_pass"
# Then restart R Studio and you are good to go

final_dataset_with_car <- get_final_dataset_with_car(Sys.getenv("working_dir"),
                                                     5, 
                                                     15, 
                                                     200)

