# ------------------------------------------------------------------------------
# 8_run_OLS.R
#
# Author: Filipe Ribeiro Ferreira
#
# This script takes the stock data of both the acquirors and targets plus their rivals
# and estimates the abnormal and cumulative abnormal returns (CAR) for every firm
# using both the stock price and stock returns. It produces a table with the deal's
# info (i.e. deal value, bid premium, merger date, etc.), merging party's info such as
# gvkey, stock exchange and finally the abnormal returns.
#
# ------------------------------------------------------------------------------

# num_of_rivals_per_firm <- 5
# event_window_days <- 15
# estimation_window_days <- 200

# library(profvis)

get_final_dataset_with_car <- function(num_of_rivals_per_firm, event_window_days, 
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
  # namely its gvkey, cusip,  etc. and, importantly,
  # its abnormal and cumulative abnormal returns as well as their standard deviation 
  
  # start by loading the relevant libraries
  #For data manipulation
  library(dplyr)
  library(tidyr)
  # to work with dates
  library(lubridate)
  library(glue)
  # for parallel processing
  library(furrr)
  library(purrr)
  library(tibble)
  library(stringr)
  # options(future.globals.maxSize = 8 * 1024^3)  # 8 GiB
  
  # call script which has identifying and cleaned stock data
  # source("final_scripts/orbis_code/Estimate ARs/9_clean_stock_data.R")
  # retx = adj_prc*trfd/(lag(adj_prc)*lag(trfd) ) - 1 
  # acq_stock_data <- as.data.frame(crsp_stock_data %>% filter(permco %in% acq_permco & permno %in% acq_permno))
  
  # as well as targets and its rivals
  # tgt_stock_data <- readRDS("data/raw_data/stock_data/1989_99_tgt_stock_data.rds" )
  
  # hereby we call the script which will compute the cumulative abnormal returns 
  # source("final_scripts/orbis_code/Estimate ARs/10_run_OLS_regressions.R")
  
  # we shall also compute time relative to announcement here as it does not 
  # change per firm (i.e. its futile to run inside loop)
  time_rel_to_anct <- c(-event_window_days:event_window_days)
  
  # counter aux (just not to duplicate col names)
  k <- 1
  # running the loop for each merger
  for (i in seq(n_min, n_max, by = 750)) {
    
    ids_start <<- i
    ids_end <<- min(i + 749, n_max)
    
    idx <- ids_start:ids_end

    # filter the data frame with the identifying information for the merger in the loop
    # merger_data <- all_firms_id_data %>% filter( Deal.Number == 101709 )
    merger_data <- get_all_firms_id_data(idx)
    
    # slice 1st row as it contains most of the data
    merger_data_wout_rivals <- merger_data$deal_i_id_data %>% group_by(deal_id) %>% slice(1) %>% ungroup()
    
    # get the merger announcement date    
    merger_anct_dates <- merger_data_wout_rivals %>% pull(`Announcement date`) 
    
    # lastly we want the competitor score shared by the acquiror with the target, if it exists
    # grab their GVKEYs
          # mp_gvkeys <- c(
          #   merger_data_wout_rivals %>% pull(acq_gvkey),
          #   merger_data_wout_rivals %>% pull(tgt_gvkey)
          # ) %>%
          #   na.omit() %>%
          #   unique()
    
    # if the gvkey of acquiror and target was found and they are rivals as defined by the Hoberg-Philips dataset,
    # meaning they share a competitor score, then retrieve both their competitor score and the year that score refers to.
    # Otherwise, just assign an NA to their rival score and year
    # getting the score
    acq_gvkeys <- merger_data_wout_rivals %>% pull(acq_gvkey)
    tgt_gvkeys <- merger_data_wout_rivals %>% pull(tgt_gvkey)
    
    merging_parties_gvkeys <- tibble(
      merger_anct_date = merger_anct_dates,
      acq_gvkey = acq_gvkeys,
      tgt_gvkey = tgt_gvkeys   
    ) 
    
    merging_parties_gvkeys_wout_nas <- merging_parties_gvkeys %>% filter(!is.na(acq_gvkey) & !is.na(tgt_gvkey))

    # THIS IS TAKING A LOT OF TIME
              # acq_tgt_cmp_score <- merging_parties_gvkeys %>%
              #   mutate(
              #     out = pmap(
              #       list(merger_anct_date, acq_gvkey, tgt_gvkey),
              #       ~ get_rivals_scores_data(..1, ..2, ..3)
              #     ),
              #     cmp_score = map_dbl(out, "score"),
              #     year_score  = map_dbl(out, "year")
              #   ) %>% select(-out)
              # 
    
    # ID DATA
    mp_id_data <- merger_data_wout_rivals %>%
      select(
        deal_id,
        `Announcement date`,
        `Deal value`,
        `Deal status`,
        `Bid premium`,
        acq_gvkey,
        tgt_gvkey,
        acq_isin = Acquiror.ISIN.number,
        tgt_isin = Target.ISIN.number,
        `NAICS description` = Acquiror.primary.NAICS.2017.description,
        `Main stock exchange` = Acquiror.main.exchange,
        `Regulatory agencies required to approve deal`,
        `Regulatory agency decision`
      ) %>%
      pivot_longer(
        cols = c(acq_gvkey, tgt_gvkey, acq_isin, tgt_isin),
        names_to = c("firm_type", ".value"),
        names_pattern = "(acq|tgt)_(.*)"
      ) %>%
      mutate(
        firm_type = if_else(firm_type == "acq", "A", "T")
      )
    
    
    # getting acq-tgt score
    # CHECK THIS!!
    acq_tgt_cmp_data <- competitors_txt_def %>% filter( 
      (gvkey1%in%merging_parties_gvkeys_wout_nas$acq_gvkey & 
         gvkey2%in%merging_parties_gvkeys_wout_nas$tgt_gvkey)
      ) %>% mutate(gvkey1 = as.character(gvkey1))
    
    acq_tgt_cmp_scores_and_years <- merging_parties_gvkeys %>% left_join( acq_tgt_cmp_data , by=c("acq_gvkey"="gvkey1") ) %>%
      filter( year<= year(merger_anct_date) & year>=(year(merger_anct_date) -5) ) %>%
      group_by(acq_gvkey) %>%
      slice_max(order_by = interaction(year, score), n=1, with_ties = FALSE) %>% 
      select(merger_anct_date, acq_gvkey, tgt_gvkey, score, year) %>%
      filter(!is.na(score)) %>%
      rename("cmp_score" = "score",
             "year_score" = "year")
    
    acq_tgt_cmp_scores_and_years <- acq_tgt_cmp_scores_and_years %>%
      pivot_longer(
        cols = c(acq_gvkey, tgt_gvkey),
        names_to = "firm_type",         
        values_to = "gvkey"        
      ) %>% mutate(firm_type = if_else(startsWith(firm_type, "acq"), "A", "T"))
    
    # add to id_data
    mp_id_data <- mp_id_data %>% left_join(acq_tgt_cmp_scores_and_years %>% 
                                             select(-merger_anct_date), by = c("gvkey", "firm_type"))
    
    # STOCK DATA
    # gather all gvkeys together
    mp_gvkeys <- c(
      acq_gvkeys,
      tgt_gvkeys
    ) %>%
      unique() %>%
      .[!is.na(.)]
    
    mp_stock_data <- get_merging_parties_stock_data(mp_gvkeys) %>% 
      group_by(gvkey) %>% nest() %>%
      rename(stock_data=data)
    
    mp_df_to_get_car <- mp_id_data %>% select(merger_anct_date=`Announcement date`, deal_id, firm_type, gvkey) %>%
      inner_join( mp_stock_data, by = "gvkey" ) %>%
      mutate(
        stock_data = map2(stock_data, merger_anct_date, ~
                      .x %>%
                      filter(
                        datadate >= (.y %m-% years(4)) &
                          datadate <= (.y %m+% months(2))
                      )
        )
      ) %>%
      # drop rows where the filtered tibble is empty
      filter(map_lgl(stock_data, ~ nrow(.x) > 100))
    
    ## RIVALS
    
    acq_rivals_id_cols <- merger_data$acq_rivals_id_data %>%
      select(
        deal_id,
        Rumour.date,
        acq_rival_gvkey
      )
    
    acq_rivals_stock_data <- get_rivals_stock_data(unique(na.omit(acq_rivals_id_cols$acq_rival_gvkey))) %>%
      group_by(gvkey_num) %>% nest() %>%
      rename(stock_data = data)
    
    acq_rivals_df_to_get_car <- acq_rivals_id_cols %>% inner_join(acq_rivals_stock_data, 
                                                                  by = c("acq_rival_gvkey"="gvkey_num")) %>%
      rename(merger_anct_date = Rumour.date, gvkey=acq_rival_gvkey) %>% 
      mutate(firm_type="C_A",
             gvkey = as.character(gvkey),
             stock_data = map2(stock_data, merger_anct_date, ~
                            .x %>%
                            filter(
                              datadate >= (.y %m-% years(4)) &
                                datadate <= (.y %m+% months(2))
                            )
      )) %>%
      # drop rows where the filtered tibble is empty
      filter(map_lgl(stock_data, ~ nrow(.x) > 100)) 
    
    tgt_rivals_id_cols <- merger_data$tgt_rivals_id_data %>%
      select(
        deal_id,
        Rumour.date,
        tgt_rival_gvkey
      )
    
    tgt_rivals_stock_data <- get_rivals_stock_data(unique(na.omit(tgt_rivals_id_cols$tgt_rival_gvkey))) %>%
      group_by(gvkey_num) %>% nest() %>%
      rename(stock_data = data)
    
    tgt_rivals_df_to_get_car <- tgt_rivals_id_cols %>% inner_join(tgt_rivals_stock_data, 
                                                                  by = c("tgt_rival_gvkey"="gvkey_num")) %>%
      rename(merger_anct_date = Rumour.date, gvkey=tgt_rival_gvkey) %>% 
      mutate(firm_type="C_T",
             gvkey = as.character(gvkey),
             stock_data = map2(stock_data, merger_anct_date, ~
                                 .x %>%
                                 filter(
                                   datadate >= (.y %m-% years(4)) &
                                     datadate <= (.y %m+% months(2))
                                 )
             )) %>%
      # drop rows where the filtered tibble is empty
      filter(map_lgl(stock_data, ~ nrow(.x) > 100)) 
    
    final_df_to_get_car <- rbind(mp_df_to_get_car, acq_rivals_df_to_get_car, tgt_rivals_df_to_get_car) %>%
      select(deal_id, merger_anct_date, gvkey, firm_type, stock_data)
  
    ### ESTIMATING ABNORMAL AND CUMULATIVE ABNORMAL RETURNS
    
    # first we need S&P500 stock data
    
    get_car_table <- function(deal_id, merger_anct_date, gvkey, firm_type, stock_data) { 
      tryCatch({
        
        stock_mkt_data <- get_snp500_stock_data(merger_anct_date, estimation_window_days, event_window_days)
        
        firm_car <- get_car(merger_anct_date, 
                                    estimation_window_days, 
                                    event_window_days,
                                    stock_data, stock_mkt_data)
        
        tibble(
          deal_id = deal_id, 
          GVKEY = as.character(gvkey),
          firm_type = firm_type,
          time_rel_to_anct = time_rel_to_anct ,
          mkt_cap_th = firm_car$compustat_mkt_val_4_weeks_bef_merger_anct_th / 1000,
          abn_ret_rel = firm_car$abnormal_returns_rel,
          abn_ret_rel_csum = firm_car$abnormal_returns_rel_csum,
          abn_ret_rel_sd = firm_car$abnormal_returns_rel_sd,
          abn_ret_abs = firm_car$abnormal_returns_abs,
          abn_ret_abs_csum = firm_car$abnormal_returns_abs_csum,
          abn_ret_abs_sd = firm_car$abnormal_returns_abs_sd,
        )
        
      }, error = function(e) {
        # message(glue::glue("Error for GVKEY {gvkey}: {e$message}"))
        tibble(
          deal_id = deal_id, 
          GVKEY = as.character(gvkey),
          firm_type = firm_type,
          time_rel_to_anct = NA,
          mkt_cap_th = NA,
          abn_ret_rel = -98,
          abn_ret_rel_csum = -98,
          abn_ret_rel_sd = -98,
          abn_ret_abs = -98,
          abn_ret_abs_csum = -98,
          abn_ret_abs_sd = -98,
        ) 
      })
      
    }
    
    plan(multisession)
    firms_car <- future_pmap_dfr(final_df_to_get_car, get_car_table, .progress = F)
    
    # the final step is to merge financial variables of each firm
    
    # for merging parties
    mp_additional_id_data <- merger_data_wout_rivals %>% select(deal_id, `Announcement date`,
                                                                   Acquiror.gvkey = acq_gvkey,
                                          Acquiror.ISIN.number, Acquiror.primary.NAICS.2017.description,
                                          Acquiror.main.exchange,
                                          Pre.deal.Acquiror.market.capitalisation..Last.available.year..th.USD=
                                            Pre.deal.acquiror.market.capitalisation..Last.available.year..th.USD,
                                          Target.gvkey=tgt_gvkey,
                                          Target.ISIN.number, Target.primary.NAICS.2017.description,
                                          Target.main.exchange,
                                          Pre.deal.Target.market.capitalisation..Last.available.year..th.USD=
                                            Pre.deal.target.market.capitalisation..Last.available.year..th.USD,
                                          `Deal type` = Deal.type, `Deal value`,
                                          `Bid premium`, `Deal stock exchange`= Deal.Stock.exchange,
                                          `Regulatory agencies required to approve deal`,
                                          `Regulatory agency decision`) %>%
      pivot_longer(
        cols = c(Acquiror.gvkey, Target.gvkey,
                 Acquiror.ISIN.number, Target.ISIN.number, 
                 Acquiror.primary.NAICS.2017.description, Target.primary.NAICS.2017.description,
                 Acquiror.main.exchange, Target.main.exchange,
                 Pre.deal.Acquiror.market.capitalisation..Last.available.year..th.USD,
                 Pre.deal.Target.market.capitalisation..Last.available.year..th.USD,
                 ),
        names_to = c("firm_type",".value"),
        names_pattern = "(Acquiror|Target)(.*)"
      ) 
    
    mp_additional_id_data <- mp_additional_id_data %>% left_join( acq_tgt_cmp_scores_and_years %>%
                                                                    select(-merger_anct_date), 
                                                                  by = c(".gvkey"="gvkey", "firm_type")) %>%
      rename(ISIN=.ISIN.number,
             Industry = .primary.NAICS.2017.description,
             `Market cap` = .market.capitalisation..Last.available.year..th.USD,
             GVKEY = .gvkey,
             `Firm stock exchange` = .main.exchange,
             `Rival score` = cmp_score,
             `Rival score year` = year_score) %>%
      mutate(
        firm_type = if_else(firm_type == "Acquiror", "A", "T")
      )
    
    acq_rivals_additional_id_data <- merger_data$deal_i_id_data %>%    mutate(has_self = any(!is.na(acq_rival_gvkey) & acq_rival_gvkey == tgt_gvkey)) %>%
      filter( is.na(has_self) | has_self==F) %>% 
      group_by(deal_id) %>%
      slice_max(order_by = interaction(acq_rival_year, acq_rival_score), n=5, with_ties = FALSE) %>%
      ungroup() %>% transmute(deal_id, `Announcement date`,
                                                                   `Deal type` = NA_character_,             
                                                                   ISIN = NA_character_, Industry = NA_character_,
                                                                   `Firm stock exchange` = NA_character_,
                                                                   `Market cap`= NA_character_,
                                                                   `Deal value` = NA_character_,
                                                                   `Bid premium` = NA_character_, `Deal stock exchange` = NA_character_,
                                                                   `Regulatory agencies required to approve deal` = NA_character_,
                                                                   `Regulatory agency decision` = NA_character_,
                                                                   `Rival score` = acq_rival_score,
                                                                   `Rival score year` = acq_rival_year,
                                                                   firm_type = "C_A",
                                                                   GVKEY = acq_rival_gvkey
                                                                   ) %>% 
      distinct(deal_id, GVKEY, `Rival score year`, .keep_all = T)
    
    tgt_rivals_additional_id_data <- merger_data$deal_i_id_data %>%    mutate(has_self = any(!is.na(tgt_rival_gvkey) & tgt_rival_gvkey == acq_gvkey)) %>%
      filter( is.na(has_self) | has_self==F) %>% 
      group_by(deal_id) %>%
      slice_max(order_by = interaction(tgt_rival_year, tgt_rival_score), n=5, with_ties = FALSE) %>%
      ungroup() %>% transmute(deal_id, `Announcement date`,
                              `Deal type` = NA_character_,             
                              ISIN = NA_character_, Industry = NA_character_,
                              `Firm stock exchange` = NA_character_,
                              `Market cap`= NA_character_,
                              `Deal value` = NA_character_,
                              `Bid premium` = NA_character_, `Deal stock exchange` = NA_character_,
                              `Regulatory agencies required to approve deal` = NA_character_,
                              `Regulatory agency decision` = NA_character_,
                              `Rival score` = tgt_rival_score,
                              `Rival score year` = tgt_rival_year,
                              firm_type = "C_T",
                              GVKEY = tgt_rival_gvkey
      ) %>% distinct(deal_id, GVKEY, `Rival score year`, .keep_all = T)
    
    final_additional_id_data <- rbind(mp_additional_id_data, acq_rivals_additional_id_data, 
                                      tgt_rivals_additional_id_data) 
    
    # merge all together
    
    final_output_table <- final_additional_id_data %>% left_join(firms_car %>% select(-firm_type), 
                                                                 by = c("deal_id", "GVKEY")) %>%
      rename(`Firm type` = firm_type) %>% arrange(deal_id, `Firm type`)
    
    # save the car dataset (for each batch)
    filepath5 <- paste0("data/raw_data/orbis/with_merger_authority_decision/batch/",k,"/", k/k+10, "_plus_ARs.rds")
    write.table(final_output_table, file = filepath5, append = TRUE, sep = ",",
                col.names = if (k==1) TRUE else FALSE,
                row.names = FALSE) 
    
    k <- k+1
    
    # and delete final table contents
    #rm(final_table)
    
    # 1) correct duplicate competitors
    # 2) do quick sanity check of these matches
    # 3) scale this
    
  }
  
}

# run function
timing <- system.time( final_dataset_with_car <- get_final_dataset_with_car(5,
                                                                            15, 
                                                                            200)
)


#b <- read.csv(filepath5)
#View(b)
a <- read.csv("data/raw_data/orbis/with_merger_authority_decision/batch/1/11_plus_ARs.rds")
