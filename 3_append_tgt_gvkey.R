# ------------------------------------------------------------------------------
# 3_append_tgt_gvkey.R
#
# Author: Filipe Ribeiro Ferreira
#
# This script is the 2nd out of a series of 9 scripts.
#
# It appends the Compustat uniquer firm identifier - called GVKEY. Matching is 
# carried via the ISIN number. For US and Canadian firms we use the CUSIP (extracted
# in a previous script). For the remainder we use the ISIN as it is. As Compustat
# keeps record of these two groups of firms in two separate tables we will
# need to import both tables.
#
# The former is a CRSP unique company level identifier, while the latter is a 
# unique share class identifier (within that firm).
#
# This updated M&A dataset is then stored locally as a data frame
# ------------------------------------------------------------------------------

append_tgt_gvkey <- function(wrds_user, wrds_pass) {
  
  # The function takes as arguments the WRDS username and password to establish 
  # the API connection.
  #
  # It then matches the target CUSIP and ISIN numbers with Compustat to retrieve
  # their GVKEY
  
  #Library to connect with WRDS database
  library(RPostgres)
  
  # Connecting to WRDS database
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
  
  # starting with US and canadian firms
  # import compustat table
  us_cn_stock_id <- tbl(wrds, sql("SELECT * FROM comp_na_daily_all.funda")) %>%
    mutate(cusip = substr(cusip, 1, 8))
  
  # M&A data just for targets
  subset_orbis_df <- readRDS(filepath2)
  tgt_subset_orbis_df <- subset_orbis_df %>% select(-matches("(?i)(Acquiror|acq)"))
  
  # extract cusips
  tgt_cusips <- tgt_subset_orbis_df %>% distinct(tgt_cusip) %>% filter(!is.na(tgt_cusip)) %>%
    pull()
  
  # get gvkeys
  compustat_tgt_gvkeys_w_cusip <- as.data.frame(us_cn_stock_id %>% select(cusip, tgt_gvkey_w_cusip = gvkey) %>% 
                                                  filter(cusip %in% tgt_cusips) %>% distinct(cusip, tgt_gvkey_w_cusip) )
  
  # merge back to our dataset
  tgt_subset_orbis_df_w_gvkeys_aux1 <- tgt_subset_orbis_df %>% left_join(compustat_tgt_gvkeys_w_cusip, by = c("tgt_cusip" = "cusip"))
  
  # now we repeat this using simply the ISIN as it is
  # this will overlap with the permcos retrieved with the CUSIP (i.e. US firms)
  # but for international ones we will get their gvkey
  non_us_stock_id <- tbl(wrds, sql("SELECT * FROM comp_global_daily.g_funda")) 
  
  tgt_isins <- tgt_subset_orbis_df %>% filter(!is.na(Target.ISIN.number)) %>% distinct(Target.ISIN.number) %>% pull()
  
  compustat_tgt_gvkeys_w_isin <- as.data.frame(non_us_stock_id %>% select(isin, tgt_gvkey_w_isin = gvkey) %>% filter(isin %in% tgt_isins) %>% distinct(isin, tgt_gvkey_w_isin))
  
  tgt_subset_orbis_df_w_gvkeys_aux2 <- tgt_subset_orbis_df_w_gvkeys_aux1 %>% left_join(compustat_tgt_gvkeys_w_isin, by = c("Target.ISIN.number" = "isin"))
  
  # storing the GVKEYS under one variable
  tgt_subset_orbis_df_w_gvkeys <- tgt_subset_orbis_df_w_gvkeys_aux2 %>% mutate( tgt_gvkey = ifelse( !is.na(tgt_gvkey_w_cusip), tgt_gvkey_w_cusip,
                                                                                                    tgt_gvkey_w_isin) )
  
  # if permco was not already retrieved the u
  # tgt_reg_id_info <- tgt_reg_id_info %>% mutate( tgt_id_crsp = ifelse(!is.na(tgt_permco_w_tck), tgt_permco_w_tck, tgt_gvkey_w_isin) )
  
  ### GETTING THE PERMCO AND PERMNO - DELETE?
  
  # # connecting the table with the identifying information - stock_id
  # stock_id <- tbl(wrds, sql("SELECT * FROM crspq.dsfhdr"))
  # 
  # # convert its cusip and ncusip from 8 to 6 digit to be consistent with M&A dataset
  # # since CUSIP will also be used as a matching variable between M&A dataset and CRSP
  # stock_id <- stock_id %>%
  #   mutate(six_dig_cusip = substr(cusip, 1, 8))
  # 
  # tgt_cusips <- tgt_mergers_df %>% filter(!is.na(tgt_six_dig_cusip)) %>% distinct(tgt_six_dig_cusip) %>% pull()
  # 
  # tgt_permcos_and_id_w_cusip <- stock_id %>% select(begdat, enddat, hcusip, six_dig_cusip, permco, permno) %>% 
  #   filter( six_dig_cusip %in% tgt_cusips   )
  # 
  # tgt_mergers_df_w_permco <- merge(tgt_mergers_df, tgt_permcos_and_id_w_cusip, by.x="tgt_six_dig_cusip", by.y="six_dig_cusip", all.x=T) %>%
  #   rename(tgt_permco = permco, tgt_permno = permno, 
  #          tgt_start_date_in_crsp = begdat, tgt_end_date_in_crsp = enddat,
  #          tgt_8_dig_cusip_in_crsp = hcusip)
  # 
  # tgt_reg_id_info <- tgt_reg_id_info %>% group_by(Deal.Number) %>% 
  #   distinct(tgt_permco, .keep_all = T) %>% ungroup()
  
  # save dataset as RDS file
  tgt_filepath1 <<- paste0("data/raw_data/orbis/with_merger_authority_decision/batch/",k,"/", k/k+3, "_plus_tgt_gvkeys.rds")
  saveRDS(tgt_subset_orbis_df_w_gvkeys, tgt_filepath1)
  
  #Disconnect from database
  dbDisconnect(wrds)
  
}

# run function
append_tgt_gvkey(Sys.getenv("WRDS_USER"), 
                 Sys.getenv("WRDS_PASS"))
