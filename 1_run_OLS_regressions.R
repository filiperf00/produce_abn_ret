get_car <- function(merger_anct_date, estimation_window_days, event_window_days,
                    stock_ind_data, stock_sp500_data) {
  
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
  # stock_ind_data <- acq_rivals_merger_data$stock_data[[1]]
  
  # first for individual firm
  firm_stck_data_at_or_after_merger_announcement <- stock_ind_data %>% filter( datadate >= merger_anct_date ) %>% 
    mutate(days_diff_to_merger_date = datadate - merger_anct_date) %>%
    filter(days_diff_to_merger_date<=4) %>%
    slice_min(order_by = days_diff_to_merger_date, n=1, with_ties = FALSE ) 
  
  firm_row_num_at_or_after_merger_announcement <- firm_stck_data_at_or_after_merger_announcement %>% pull(row_num)
  
  stock_ind_data <- stock_ind_data %>%
    filter(row_num >= firm_row_num_at_or_after_merger_announcement - event_window_days - estimation_window_days, 
           row_num <= firm_row_num_at_or_after_merger_announcement + event_window_days) %>% 
    mutate(id = row_number())
  
  # before proceeding we will compute the market cap of the firm 4 weeks prior to merger announcement as per CRSP
  # we do it here since the market cap is computed with reference to the merger announcement date (which can be
  # either merger announcement date itself or the next trading day in case the former falls on a non-trading day)
  # Note that for the merging parties we already have this data from LSEG so the purpose here is to obtain 
  # it for the rivals
  compustat_mkt_val_4_weeks_bef_merger_anct_th <- stock_ind_data %>% filter( row_num == firm_row_num_at_or_after_merger_announcement - 20 ) %>%
    pull(mkt_val_w_compustat)
  
  # run OLS regression on estimation window
  # but first we need the two dates spanning it
  reg_data <- stock_ind_data %>% inner_join( stock_sp500_data , by = "id" ) %>% 
    rename( ind_row_num = row_num.x, mkt_row_num = row_num.y ) %>%
    mutate(reg_row_num = row_number())
  
  # force error if incorrect data dimension
  #stock_ind_data$datadate[nrow(stock_ind_data)] - stock_ind_data$datadate[1] == 332
  
  #stock_ind_data$row_num[nrow(stock_ind_data)] - stock_ind_data$row_num[1]
  
  # raise error if we did not get trading days equal to event plus estimation window
  stopifnot(nrow(reg_data) == 231 || stock_ind_data$datadate[nrow(stock_ind_data)] - stock_ind_data$datadate[1] < 450)
  
  # Notice we use adjusted price (adj_prc) which takes into account changes in the stock price
  # caused by events such as stock splits, mergers or liquidations. These are recorded in CRSP under the
  # variable Cumulative Factor Price adjustment (cfacpr). Thus, to use the correct stock pricing
  # one needs to divide the stock price (prc) by cfacpr  -these are stored in the adjusted price (adj_prc) variable
  # which retrieves the stock data for all firms in the M&A dataset
  #
  # spindx is the level of the S&P500 composite index without including dividends
  # regress the two for the estimation window to obtain the market-beta
  mkt_reg_abs <- lm(adj_prc ~ spindx, reg_data, subset = (reg_row_num <= estimation_window_days))
  
  # get event data
  event_stock_data <- reg_data %>% filter(reg_row_num <= estimation_window_days + event_window_days & 
                                              reg_row_num >= estimation_window_days - event_window_days)
  
  market_stock_price <- event_stock_data %>% select(spindx)
  
  # obtain the counterfactual - i.e. price implied by the market-beta
  estimated_price <- predict(mkt_reg_abs, newdata = market_stock_price)
  
  # get the actual price observed
  actual_price <- event_stock_data %>% 
    pull(adj_prc)
  
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
  mkt_reg_ret <- lm(ret ~ vwretd, reg_data, subset = (reg_row_num <= estimation_window_days)  )
  
  # predict the returns implied by the market-beta
  market_stock_ret <- event_stock_data %>% select(vwretd)
  
  estimated_ret <- predict(mkt_reg_ret, newdata = market_stock_ret)
  
  # get the actual observed returns
  actual_ret <- event_stock_data %>% 
    pull(ret)
  
  # take the difference between the 2
  abnormal_returns_rel <- actual_ret - estimated_ret
  abnormal_returns_rel_csum <- cumsum(abnormal_returns_rel)
  #abnormal_returns_rel_avg <- cummean(abnormal_returns_rel)
  abnormal_returns_rel_sd <- sd(abnormal_returns_rel_csum)
  
  #t_test_rel <- t.test(abnormal_returns_rel_csum, mu=0)
  
  # store all the abnormal returns in a list
  return(list(abnormal_returns_abs = abnormal_returns_abs,
              abnormal_returns_abs_csum = abnormal_returns_abs_csum,
              abnormal_returns_abs_sd = abnormal_returns_abs_sd,
              abnormal_returns_rel = abnormal_returns_rel,
              abnormal_returns_rel_csum = abnormal_returns_rel_csum,
              abnormal_returns_rel_sd = abnormal_returns_rel_sd,
              compustat_mkt_val_4_weeks_bef_merger_anct_th = compustat_mkt_val_4_weeks_bef_merger_anct_th
  ) )
  
}
