# ------------------------------------------------------------------------------
# 8_get_stock_data.R
#
# Author: Filipe Ribeiro Ferreira
#
# This script is the 8th out of a series of 9 scripts.
#
# This script filter firms' stock data from Compustat and then locally saves it
# ------------------------------------------------------------------------------
get_stock_data <- function(wrds_user, wrds_pass) {
  
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
  
  # loading the data with all the identifying information of all firms (i.e.
  # acquirors, targets and respective rivals)
  all_firms_id_data <- readRDS(filepath1) 
  
  # the stock data of acquirers and its rivals
  ### COMPUTE MKT CAP HERE AND RETURNS AS WELL
  # crsp_stock_data <- tbl(wrds, sql("SELECT * FROM crsp_q_stock.dsf")) %>%
  #   select( date, permco, permno, prc, retx, cfacpr, cfacshr, shrout) %>%
  #   filter(prc!=0 ) %>% # these values indicate the variable has missing value
  #   mutate( row_num = row_number(),
  #          adj_prc = abs(prc)/cfacpr,
  #          adj_shares = shrout/cfacshr,
  #          mkt_val_in_th = adj_prc*shrout*cfacshr
  #          )
  
  # get stock data of:
  # acquirors
  compustat_na_stock_data <- tbl(wrds, sql("SELECT * FROM comp_na_daily_all.secd")) %>%
    select( datadate, gvkey, iid, ajexdi, cshoc, prccd, trfd  ) %>%
    mutate( gvkey_num = as.numeric(gvkey) )
  
  acq_us_cn_firms_gvkeys <- all_firms_id_data %>% distinct(acq_gvkey_w_cusip) %>% filter(!is.na(acq_gvkey_w_cusip)) %>% pull()
  tgt_us_cn_firms_gvkeys <- all_firms_id_data %>% distinct(tgt_gvkey_w_cusip) %>% filter(!is.na(tgt_gvkey_w_cusip)) %>% pull()
  us_cn_firms_gvkeys <- union(acq_us_cn_firms_gvkeys, tgt_us_cn_firms_gvkeys)
  
  us_cn_firms_stock_data <- as.data.frame( compustat_na_stock_data %>% filter(gvkey %in% us_cn_firms_gvkeys) %>%
                                             group_by(gvkey) %>% filter(iid == min(iid) ) %>% # this ensures we uniquely pick a security for any given firm
                                             ungroup()
  ) 

  # save dataset as RDS file
  filepath2 <<- paste0("data/raw_data/orbis/with_merger_authority_decision/batch/",k,"/", k/k+7, "_us_canadian_merging_parties_stock_data.rds")
  saveRDS(us_cn_firms_stock_data, filepath2)
  
  # targets
  compustat_global_stock_data <- tbl(wrds, sql("SELECT * FROM comp_global_daily.g_secd")) %>%
    select( datadate, gvkey, iid, ajexdi, cshoc, prccd, trfd  ) 
  
  non_acq_us_cn_firms_gvkeys <- all_firms_id_data %>% distinct(acq_gvkey_w_isin) %>% filter(!is.na(acq_gvkey_w_isin)) %>% pull()
  non_tgt_us_cn_firms_gvkeys <- all_firms_id_data %>% distinct(tgt_gvkey_w_isin) %>% filter(!is.na(tgt_gvkey_w_isin)) %>% pull()
  non_us_cn_firms_gvkeys <- union(non_acq_us_cn_firms_gvkeys, non_tgt_us_cn_firms_gvkeys)
  
  non_us_cn_firms_stock_data <- as.data.frame( compustat_global_stock_data %>% filter(gvkey %in% non_us_cn_firms_gvkeys) %>%
                                                 group_by(gvkey) %>% filter(iid == min(iid) ) %>% # this ensures we uniquely pick a security for any given firm
                                                 ungroup() 
  )
  
  # save dataset as RDS file
  filepath3 <<- paste0("data/raw_data/orbis/with_merger_authority_decision/batch/",k,"/", k/k+8, "_non_us_canadian_merging_parties_stock_data.rds")
  saveRDS(non_us_cn_firms_stock_data, filepath3)
  
  # and their rivals
  acq_rival_gvkeys <- all_firms_id_data %>% distinct(acq_rival_gvkey) %>% filter(!is.na(acq_rival_gvkey)) %>% pull()
  tgt_rival_gvkeys <- all_firms_id_data %>% distinct(tgt_rival_gvkey) %>% filter(!is.na(tgt_rival_gvkey)) %>% pull()
  rival_gvkeys <- union(acq_rival_gvkeys, tgt_rival_gvkeys)
  
  rivals_stock_data <- as.data.frame( compustat_na_stock_data %>% filter(gvkey_num %in% rival_gvkeys) %>%
                                        group_by(gvkey_num) %>% filter(iid == min(iid) ) %>% # this ensures we uniquely pick a security for any given firm
                                        ungroup()
  )
  
  # save dataset as RDS file
  filepath4 <<- paste0("data/raw_data/orbis/with_merger_authority_decision/batch/",k,"/", k/k+9, "_us_canadian_rivals_stock_data.rds")
  saveRDS(rivals_stock_data, filepath4)
  
  # disconnect from database
  dbDisconnect(wrds)
  
}

get_stock_data(Sys.getenv("WRDS_USER"), 
               Sys.getenv("WRDS_PASS")
)
