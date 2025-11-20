# first import all the data
filepath1 <- paste0("data/raw_data/orbis/with_merger_authority_decision/batch/",k,"/", k/k+6, "_acq_tgt_and_rivals_merged.rds")
filepath2 <- paste0("data/raw_data/orbis/with_merger_authority_decision/batch/",k,"/", k/k+7, "_us_canadian_merging_parties_stock_data.rds")
filepath3 <- paste0("data/raw_data/orbis/with_merger_authority_decision/batch/",k,"/", k/k+8, "_non_us_canadian_merging_parties_stock_data.rds")
filepath4 <- paste0("data/raw_data/orbis/with_merger_authority_decision/batch/",k,"/", k/k+9, "_us_canadian_rivals_stock_data.rds")

# First let us get the identifying data
all_firms_id_data <- readRDS(filepath1) %>% mutate(deal_id = dense_rank(Deal.Number) + n_min - 1)
# nrows <<- all_firms_id_data %>% distinct( deal_id ) %>% nrow()

us_cn_merging_parties_stock_data <- readRDS(filepath2) %>%
  mutate(mkt_val_w_compustat = prccd*cshoc/1000,
         adj_prc = prccd/ajexdi,
         adj_prc = ifelse( !is.na(adj_prc), adj_prc, prccd ),
         ret = ifelse(!is.na(trfd), adj_prc*trfd/(lag(adj_prc)*lag(trfd)) - 1,
                      adj_prc/lag(adj_prc) ) ) %>% 
  group_by(gvkey) %>% arrange(datadate) %>% mutate(row_num = row_number()) %>% ungroup() %>%
  select(datadate, gvkey, adj_prc, ret, mkt_val_w_compustat, row_num)

non_us_cn_merging_parties_stock_data <- readRDS(filepath3) %>%
  mutate(mkt_val_w_compustat = prccd*cshoc/1000,
         adj_prc = prccd/ajexdi,
         adj_prc = ifelse( !is.na(adj_prc), adj_prc, prccd ),
         ret = ifelse(!is.na(trfd), adj_prc*trfd/(lag(adj_prc)*lag(trfd)) - 1,
                      adj_prc/lag(adj_prc) ) )  %>% 
  group_by(gvkey) %>% arrange(datadate) %>% mutate(row_num = row_number()) %>% ungroup() %>%
  select(datadate, gvkey, adj_prc, ret, mkt_val_w_compustat, row_num)

all_merging_parties_stock_data <- bind_rows(
  us_cn_merging_parties_stock_data,
  non_us_cn_merging_parties_stock_data
)

rm(us_cn_merging_parties_stock_data, non_us_cn_merging_parties_stock_data)

us_cn_rivals_stock_data <- readRDS(filepath4) %>%
  mutate(mkt_val_w_compustat = prccd*cshoc/1000,
         adj_prc = prccd/ajexdi,
         adj_prc = ifelse( !is.na(adj_prc), adj_prc, prccd ),
         ret = ifelse(!is.na(trfd), adj_prc*trfd/(lag(adj_prc)*lag(trfd)) - 1,
                      adj_prc/lag(adj_prc) ) )  %>% 
  group_by(gvkey) %>% arrange(datadate) %>% mutate(row_num = row_number()) %>% ungroup() %>%
  select(datadate, gvkey_num, adj_prc, ret, mkt_val_w_compustat, row_num)

get_all_firms_id_data <- function(deal_id_i) {
  # loading the data with all the identifying information of all firms (i.e.
  # acquirors, targets and respective rivals)
  deal_i_id_data <- all_firms_id_data %>% 
    filter(deal_id %in% deal_id_i)
  
  # and now subset it for each group of rivals
  # first the acquirors
  acq_rivals_id_data <- all_firms_id_data %>% 
    filter(deal_id %in% deal_id_i) %>%
    select(deal_id, Rumour.date, tgt_gvkey, acq_rival_gvkey, acq_rival_score, acq_rival_year) %>%
    distinct(deal_id, acq_rival_gvkey, acq_rival_year, .keep_all = T) %>%
    group_by(deal_id) %>%
    mutate(has_self = any(!is.na(acq_rival_gvkey) & acq_rival_gvkey == tgt_gvkey)) %>%
    filter( is.na(has_self) | has_self==F) %>% 
    arrange(desc(acq_rival_year), desc(acq_rival_score)) %>%
    slice_head(n = 5) %>%
    ungroup() %>% select(-tgt_gvkey)
  
  tgt_rivals_id_data <- all_firms_id_data %>% 
    filter(deal_id %in% deal_id_i) %>%
    select(deal_id, Rumour.date, acq_gvkey, tgt_rival_gvkey, tgt_rival_score, tgt_rival_year) %>%
    distinct(deal_id, tgt_rival_gvkey, tgt_rival_year, .keep_all = T) %>%
    group_by(deal_id) %>%
    mutate(has_self = any(!is.na(tgt_rival_gvkey) & tgt_rival_gvkey == acq_gvkey)) %>%
    filter( is.na(has_self) | has_self==F) %>% 
    arrange(desc(tgt_rival_year), desc(tgt_rival_score)) %>%
    slice_head(n = 5) %>%
    ungroup() %>% select(-acq_gvkey)
  
  # rename id cols in preparation for final output table
  deal_i_id_data <- deal_i_id_data %>% rename(
    "Merger id"= Deal.Number, "Announcement date" = Rumour.date, 
    "Deal value" = Deal.value.th.USD,
    "Deal status" = Deal.status,
    "Bid premium" = Bid.premium...Announced.date..,
    "Regulatory agencies required to approve deal" = Regulatory.body.name,
    "Regulatory agency decision" = Regulatory.body.decision
  )
  
  return(list(deal_i_id_data=deal_i_id_data, 
              acq_rivals_id_data=acq_rivals_id_data, 
              tgt_rivals_id_data=tgt_rivals_id_data))

}

get_merging_parties_stock_data <- function(gvkey_i) {

  deal_i_merging_parties_stock_data <- all_merging_parties_stock_data %>% filter(gvkey %in% gvkey_i)
  
  return(deal_i_merging_parties_stock_data)
  
}

get_rivals_stock_data <- function(gvkey_i) {

  deal_i_rivals_stock_data <- us_cn_rivals_stock_data %>%
    filter(gvkey_num %in% gvkey_i)
  
  return(deal_i_rivals_stock_data)
  
}

get_snp500_stock_data <- function(merger_anct_date, estimation_window_days, event_window_days) {
  
  sp500_row_num_at_or_after_merger_announcement <- sp500_data %>% filter( date >= merger_anct_date ) %>% 
    mutate(days_diff_to_merger_date = date - merger_anct_date) %>%
    filter(days_diff_to_merger_date<=4) %>%
    slice_min(order_by = days_diff_to_merger_date, n=1, with_ties = FALSE ) %>% pull(row_num)
  
  sp500_row_num_at_or_after_merger_announcement <- ifelse( length(sp500_row_num_at_or_after_merger_announcement) == 0,
                                                           NA,
                                                           sp500_row_num_at_or_after_merger_announcement)
  
  stock_mkt_data <- sp500_data %>%
    filter(row_num >= sp500_row_num_at_or_after_merger_announcement - event_window_days - estimation_window_days, 
           row_num <= sp500_row_num_at_or_after_merger_announcement + event_window_days) %>% 
    mutate(id = row_number())
  
  return(stock_mkt_data)
  
}

get_rivals_scores_data <- function(merger_anct_date, acq_gvkey_i, tgt_gvkey_i) {
  # finally competitors score data
  acq_and_tgt_cmp_data <- competitors_txt_def %>% filter( (gvkey1%in%acq_gvkey_i & gvkey2%in%tgt_gvkey_i) &
                                                                                           year<= year(merger_anct_date) & year>=(year(merger_anct_date) -5) ) %>%
    group_by(gvkey1) %>%
    slice_max(order_by = year, n=1, with_ties = FALSE)
  
  # if(nrow(acq_and_tgt_cmp_data) == 0 ) {
  #   acq_and_tgt_cmp_score <- NA
  #   
  #   acq_and_tgt_cmp_year <- NA
  #   
  # } else{
  #   acq_and_tgt_cmp_score <- acq_and_tgt_cmp_data %>% pull(score)
  #   
  #   acq_and_tgt_cmp_year <- acq_and_tgt_cmp_data %>% pull(year)
  #}
  
  return(acq_and_tgt_cmp_data)
}
