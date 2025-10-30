# ------------------------------------------------------------------------------
# 5_get_and_append_tgt_rivals_id_data.R
#
# Author: Filipe Ribeiro Ferreira
#
# This script is the 4th out of a series of 9 scripts.
#
# It appends the top-5 rivals of the target.  This shall be defined according
# to the Hoberg-Philips dataset which assigns a competitor's score from 0 to 1 
# to each rival of a given firm in every year from 1989-2021. The higher the score 
# the closer the competitor. 
#
# Importantly, we will pick the 5 rivals with highest score by iteratively going
# through each year that is either the same as the merger year or before it, but
# never after.
#
# This means that starting with the merger announcement year we attempt to pick 
# the top-5 competitors. If we do not get all 5, we move one year down to get 
# the remainder of the rivals to get all 5. We continue to do so until either 
# all 5 have been obtained or there are no more years left to search.
#
# To be precise, we retrieve the top-6 competitors for it may happen that one
# of the rivals is the target itself. However, when we actually scoop the stock
# data we make sure to filter out the 6th rival which will be either the target
# or the competitor with lowest score. Thus we effectively only use 5.
#
# This updated M&A dataset is then stored locally as a data frame
# ------------------------------------------------------------------------------

get_and_append_tgt_rivals_id_data <- function(wrds_user, wrds_pass, n_rivals) {
  
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
  
  # This function imports the Hoberg-Philips dataset, filters it by the GVKEYs
  # in our Orbis M&A dataset and retrieves the 6 rivals with highest score for
  # each target. The rivals' score are then added back to the M&A dataset.
  
  ### GETTING COMPETITORS DATA
  
  # we shall do it by applying the text based definition from Hoberg and Phillips which use
  # GVKEY (COMPUSTAT identifier) as firm id
  # importing the (restricted) dataset but for all available years (1988-2021)
  
  # the identifying variable they use ig 'GVKEY', so now the first step is 
  # to find the GVKEY of each firm in our M&A dataset using CRSP
  # we shall do using the firms' permanent numbers (permco and permno) which we have previously retrieved using ticker
  # pulling the permcos
  tgt_subset_orbis_df_w_gvkeys_of_tgt <- readRDS(tgt_filepath1) %>%
    mutate(tgt_gvkey_num = as.numeric( substr(tgt_gvkey, 1, 6) ) )
  
  ### HERE DO BY CHUNKS IF NEED TO RENDER CODE MORE TIME EFFICIENT
  
  # tgt_permcos <- tgt_reg_id_info %>% filter(!is.na(tgt_permco)) %>% distinct(tgt_permco) %>% pull()
  # tgt_permnos <- tgt_reg_id_info %>% filter(!is.na(tgt_permno)) %>% distinct(tgt_permno) %>% pull()
  
  # get the target GVKEYs 
  tgt_gvkeys <- tgt_subset_orbis_df_w_gvkeys_of_tgt %>% filter(!is.na(tgt_gvkey_num)) %>% distinct(tgt_gvkey_num) %>% pull()
  
  # to first find each firm's GVKEY we need to access the CRSP/COMPUSTAT linking table (as it has both the 
  # permanent numbers and the GVKEY) so we can filter by the permanent number and thereby obtain the gvkey of each firm
  # importing the CRSP/COMPUSTAT linking table
  # crsp_compstat_link_table <- tbl(wrds, sql("SELECT * FROM crsp_q_ccm.ccmxpf_lnkhist")) 
  # 
  # # tgt_id_and_gvkeys_w_permcos <- crsp_compstat_link_table %>% filter(lpermco %in% tgt_permcos & lpermno %in% tgt_permnos) 
  # 
  # tgt_gkvey_w_ <- crsp_compstat_link_table %>% filter(lpermco %in% tgt_permcos & lpermno %in% tgt_permnos) 
  # 
  # tgt_reg_id_info_with_gvkeys_aux1 <- merge(tgt_reg_id_info, tgt_id_and_gvkeys_w_permcos,
  #                                           by.x=c("tgt_permco", "tgt_permno"), by.y=c("lpermco", "lpermno"), all.x = T) %>% 
  #   rename(tgt_gvkey_w_permco=gvkey, tgt_linkdt = linkdt, tgt_linkenddt = linkenddt)
  # 
  # tgt_reg_id_info_with_gvkeys_aux1 <- tgt_reg_id_info_with_gvkeys_aux1 %>% group_by(Deal.Number) %>% 
  #   distinct(tgt_gvkey_w_permco, .keep_all = T) %>% ungroup()
  # 
  # # tgt_reg_id_info_with_gvkeys_aux1 <- tgt_reg_id_info_with_gvkeys_aux1 %>% group_by(Deal.Number) %>% 
  # #   slice(1) %>% ungroup()
  # 
  # tgt_reg_id_info_with_gvkeys_aux1 <- tgt_reg_id_info_with_gvkeys_aux1 %>% 
  #   mutate(tgt_gvkey = ifelse(!is.na(tgt_gvkey_w_permco), tgt_gvkey_w_permco, tgt_gvkey_w_isin) )
  # 
  # tgt_reg_id_info_with_gvkeys_aux1 <- tgt_reg_id_info_with_gvkeys_aux1 %>% mutate( tgt_gvkey = as.numeric(tgt_gvkey) )
  
  # tgt_gvkeys <- tgt_reg_id_info_with_gvkeys_aux1 %>% distinct(tgt_gvkey) %>% filter(!is.na(tgt_gvkey)) %>% pull() 
  
  # getting all competitors for every firm in the Hoberg-Philips dataset 
  competitors_per_tgt <- competitors_txt_def %>% filter(gvkey1 %in% tgt_gvkeys) %>% rename(tgt_rival_gvkey = gvkey2, tgt_rival_year = year,
                                                                                           tgt_rival_score = score)
  
  # adding the competitors back to the original dataset
  tgt_subset_orbis_df_w_gvkeys_of_tgt_and_rivals <- merge(tgt_subset_orbis_df_w_gvkeys_of_tgt, competitors_per_tgt, by.x = "tgt_gvkey_num", by.y = "gvkey1", all.x = T) %>% filter( tgt_rival_year <= year(Rumour.date) ) %>%
    group_by(Deal.Number, tgt_rival_gvkey) %>% slice_max( order_by = tgt_rival_year, n=1, with_ties = FALSE  )
  
  rm(competitors_per_tgt)
  ### FILTER OUT THE COMPETITORS HERE
  
  tgt_subset_orbis_df_w_gvkeys_of_tgt_and_rivals <- left_join(tgt_subset_orbis_df_w_gvkeys_of_tgt, 
                                                              tgt_subset_orbis_df_w_gvkeys_of_tgt_and_rivals %>%
                                                                select(Deal.Number, tgt_rival_gvkey, 
                                                                       tgt_rival_score, tgt_rival_year), 
                                                              by = c("Deal.Number") 
  )
  
  tgt_rivals_gvkeys <- tgt_subset_orbis_df_w_gvkeys_of_tgt_and_rivals %>% filter(!is.na(tgt_rival_gvkey)) %>% distinct(tgt_rival_gvkey) %>% pull()
  
  tgt_rivals_start_end_date_cs <- as.data.frame(tbl(wrds, sql("SELECT * FROM comp_na_daily_all.funda")) %>% select(gvkey, datadate) %>% 
                                                  mutate(gvkey=as.numeric(gvkey)) %>% filter( gvkey %in% tgt_rivals_gvkeys ) %>% group_by(gvkey) %>%
                                                  filter(datadate == min(datadate) | datadate == max(datadate) ) %>% distinct(gvkey, datadate)  %>%
                                                  mutate(pos = if_else(datadate == min(datadate), "start_date_cs", "end_date_cs")) %>%
                                                  pivot_wider(names_from = pos, values_from = datadate))
  
  tgt_subset_orbis_df_w_gvkeys_of_tgt_and_rivals_aux <- tgt_subset_orbis_df_w_gvkeys_of_tgt_and_rivals %>% 
    left_join( tgt_rivals_start_end_date_cs, by=c("tgt_rival_gvkey"="gvkey")) %>% group_by(Deal.Number) %>%
    filter(
      start_date_cs <= (Rumour.date %m-% years(3)) &
        end_date_cs   >= (Rumour.date %m+% months(2))
    ) %>% group_by(Deal.Number) %>%
    slice_max( n=n_rivals, order_by = interaction(tgt_rival_year, tgt_rival_score), with_ties = F) %>% 
    filter( year(Rumour.date) - tgt_rival_year <= 3 ) %>%
    select(Deal.Number, 25:29 )
  
  tgt_and_rivals_id_data <- tgt_subset_orbis_df_w_gvkeys_of_tgt %>% left_join(tgt_subset_orbis_df_w_gvkeys_of_tgt_and_rivals_aux,
                                                                              by = "Deal.Number")
  
  # save dataset as RDS file
  tgt_filepath2 <<- paste0("data/raw_data/orbis/with_merger_authority_decision/batch/",k,"/", k/k+5, "_plus_tgt_top5_rivals_gvkeys.rds")
  saveRDS(tgt_and_rivals_id_data, tgt_filepath2)
  
  #Disconnect from database
  dbDisconnect(wrds)
  
}

# run function
get_and_append_tgt_rivals_id_data(Sys.getenv("WRDS_USER"), 
                                  Sys.getenv("WRDS_PASS"),
                                  6)

