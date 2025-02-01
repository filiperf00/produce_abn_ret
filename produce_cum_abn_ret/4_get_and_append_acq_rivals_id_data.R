################################################################################################################################################################################

# This script is the 4th out of a series of 9 scripts.
#
# It imports the dataset with the original M&A data (obtained from the LSEG workspace)
# appended with the permanent company number (permco) and permanent issue number (permno)
# of the acquirors and retrieves the identifying information of its top-5 rivals
# That is, their company name, ticker, CUSIP, permco, permno and Global Comapany Key (GVKEY)
#
# What constitutes a rival and its rank shall be defined using the Hoberg-Philips dataset which assigns
# a competitor's score from 0 to 1 to each rival of a given firm in every year from 1989-2021 (the higher the score the closer the competitor)
#
# Importantly, we will pick the top-5 rivals that are closest, but never a year or more after, to the merger announcement year
# So, we prioritize those whose competitor score year is closest, if not the same, as the merger announcement year (but never after)
# before picking the rivals with highest competitor score
#
# This means that for every year starting with the merger announcement year we attempt to pick the top-5 competitors. If we do not get all 5,
# we move one year down (i.e. before) to get the remainder of the rivals to get all 5. We continue to do so until all 5 have been obtained.
# Naturally it may, and most likely will happen, that in some cases we will be missing some, or even all, rivals
#
# In addition, the Hoberg-Philips dataset uses the GVKEY as a unique identifying variable which we so far lack
# Thus, we first find the GVKEY of the acquirors, filter the Hoberg-Philips dataset to find
# their rivals and lastly obtain their identifying information
#
# This updated M&A dataset is then stored locally as a data frame

################################################################################################################################################################################

get_and_append_acq_rivals_id_data <- function(working_dir, wrds_user, wrds_pass, num_of_rivals_per_firm) {
  
  # As per previous scripts, the function starts by taking the working directory and the WRDS username and pass
  # to establish the API connection 
  # It also takes the number of rivals to be collected for each acquiror from the Hoberg-Philips dataset
  # (orderded by those closest to the merger announcement year)
  
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
  
  # and import the table with the identifying information - stock_id. This table is necessary
  # as it contains the stock ticker (which the table with stock prices/returns does not) and it will allow
  # us to retrieve the name, ticker and CUSIP of the competitors
  stock_id <- tbl(wrds, sql("SELECT * FROM crsp.dsenames"))
  
  # convert its cusip and ncusip from 8 to 6 digit to be consistent with M&A dataset
  # since CUSIP will also be used as a matching variable between M&A dataset and CRSP
  stock_id <- stock_id %>%
    mutate(cusip = substr(cusip, 1, 6), ncusip = substr(ncusip, 1, 6))
  
  ### GETTING COMPETITORS DATA
  
  # we shall do it by applying the text based definition from Hoberg and Phillips which use
  # GVKEY (COMPUSTAT identifier) as firm id
  # importing the (restricted) dataset but for all available years (1988-2021)
  competitors_txt_def <- read.table("data/raw_data/hoberg_philips.txt", header = TRUE, sep = "\t", stringsAsFactors = FALSE)
  
  # removing NAs in the competitors score variable and zero scores
  competitors_txt_def <- competitors_txt_def %>%  filter(score!=0 & !is.na(score))
  
  # save as RDS file
  saveRDS(competitors_txt_def, file = "data/raw_data/hoberg_philips.rds" )
  
  # the identifying variable they use ig 'GVKEY', so now the first step is 
  # to find the GVKEY of each firm in our M&A dataset using CRSP
  # we shall do using the firms' permanent numbers (permco and permno) which we have previously retrieved using ticker
  # pulling the permcos
  acq_reg_id_info <- readRDS("data/processed_data/id_data/acq_id_data_with_permcos_and_permnos.rds")
  
  ### HERE DO BY CHUNKS IF NEED TO RENDER CODE MORE TIME EFFICIENT
  
  acq_permcos <- acq_reg_id_info %>% filter(!is.na(acq_permco)) %>% distinct(acq_permco) %>% pull()
  acq_permnos <- acq_reg_id_info %>% filter(!is.na(acq_permno)) %>% distinct(acq_permno) %>% pull()
  
  # to first find each firm's GVKEY we need to access the CRSP/COMPUSTAT linking table (as it has both the 
  # permanent numbers and the GVKEY) so we can filter by the permanent number and thereby obtain the gvkey of each firm
  # importing the CRSP/COMPUSTAT linking table
  crsp_compstat_link_table <- tbl(wrds, sql("SELECT * FROM crspq.ccmxpf_linktable")) %>% 
    mutate(gvkey = as.integer(gvkey), usedflag=as.integer(usedflag))
  
  acq_id_and_gvkeys_w_permcos <- crsp_compstat_link_table %>% filter(lpermco %in% acq_permcos & lpermno %in% acq_permnos) %>% 
    select(lpermco, lpermno, gvkey, linkdt, linkenddt)
  
  # adding the firms' gvkeys to the initial dataset, i.e the one with the each acquiror's 
  # identifying information (name, ticker, cusip, permco, etc.)
  # and record the dates that the acquiror was added to the CRSP/COMPUSTAT linking table
  # as well as its end date
  # finally we remove gvkey duplicates for the same (permco, permno) combination 
  acq_reg_id_info_with_gvkeys_aux1 <- merge(acq_reg_id_info, acq_id_and_gvkeys_w_permcos,
                                        by.x=c("acq_permco", "acq_permno"), by.y=c("lpermco", "lpermno"), all.x = T) %>% 
    rename(acq_gvkey=gvkey, acq_linkdt = linkdt, acq_linkenddt = linkenddt) %>% group_by(merger_id, acq_permco, acq_permno) %>%
    distinct(acq_gvkey, .keep_all = T)
  
  # in dealing with multiple GVKEYs retrieved for the same (permco, permno) combination,
  # we shall pick the one whose date range in CRSP included the merger announcement date
  acq_reg_id_info_with_gvkeys_aux2 <- acq_reg_id_info_with_gvkeys_aux1 %>% filter( ifelse( is.na(acq_gvkey) | n()==1 , TRUE,  
                                                                                                                      date_announced >= acq_linkdt & ( date_announced <= acq_linkenddt | is.na(acq_linkenddt) ) ) 
  ) %>% ungroup()
  
  # but we must be careful to keep any merger id we might have lost with the above filtering
  # first find the missing merger ids
  missing_merger_ids <- setdiff(acq_reg_id_info_with_gvkeys_aux1$merger_id, acq_reg_id_info_with_gvkeys_aux2$merger_id)
  
  # keep only the observation whose merger announcement date is closest to the date the firm was added to CRSP/COMPUSTAT universe 
  # (as these might also have multiple GVKEYS) 
  missing_data <- acq_reg_id_info_with_gvkeys_aux1 %>% 
    filter(merger_id %in% missing_merger_ids) %>% group_by(merger_id, acq_permco, acq_permno) %>% mutate( days_diff = as.numeric( abs( date_announced - acq_linkdt ) ) ) %>%
    slice_min( order_by = days_diff, n=1, with_ties = F ) %>% select( -days_diff ) %>% ungroup()
  
  # finally add them back to the dataset
  acq_reg_id_info_with_gvkeys <- bind_rows(acq_reg_id_info_with_gvkeys_aux2, missing_data)
  
  # once we have a unique gvkey for each firm we can access the Hoberg-Philips dataset to retrieve their competitors
  # pulling the gvkeys
  acq_gvkeys <- acq_reg_id_info_with_gvkeys %>% filter(!is.na(acq_gvkey)) %>% distinct(acq_gvkey) %>% pull()
  
  # getting all competitors for every firm in the Hoberg-Philips dataset 
  competitors_per_acq <- competitors_txt_def %>% filter(gvkey1 %in% acq_gvkeys) 
  
  # adding the competitors back to the original dataset
  all_acq_and_rivals_id_aux <- merge(acq_reg_id_info_with_gvkeys, competitors_per_acq, by.x = "acq_gvkey", by.y = "gvkey1", all.x = T) 
  
  # for each firm, we only want to consider the rivals whose year of the competitor score is at least the same year
  # as the merger announcement date. This allows us to pick the rivals of the firm at the time of the merge
  # In addition, we weed out rivals' duplicates (since the same rival is recorded over many years) by picking
  # the most recent year its score is available starting (again) the year of the merger announcement date
  ### TAKING LONG TIME
  all_acq_and_rivals_id <- all_acq_and_rivals_id_aux %>% group_by(merger_id, gvkey2) %>% filter( ifelse( is.na(gvkey2), TRUE, year <= year(date_announced) ) ) %>%
    filter( ifelse( is.na(gvkey2), TRUE, year == max(year) ) ) %>%
    ungroup()
  
  # since we depart from the rival's gvkey, the path to obtain their id info is the opposite of the one we took for the acquirors
  # that is, we first use the rivals' gvkey to obtain the permco, through the CRSP/COMPUSTAT linking table
  # once we have the permcos we obtain the remaining id info, i.e. company name, cusip, ticker, etc. through stock_id table
  
  # all_acq_and_rivals_id contains all the identifying information of the acquiror plus the gvkey of all its rivals
  # the next step is to retrieve the identifying info of these competitors, which we are forced to do based 
  # on their gvkey
  acq_rivals_gvkeys <- all_acq_and_rivals_id %>% filter(!is.na(gvkey2)) %>% distinct( gvkey2 ) %>% pull()
  
  # so we start by getting their permanent company and issue number (permco and permno, respectively)
  acq_rivals_gvkeys_and_permcos_aux1 <- crsp_compstat_link_table %>% select(gvkey, lpermco, lpermno, linkdt, linkenddt) %>% 
    filter(gvkey %in% acq_rivals_gvkeys & !is.na(lpermco)) %>% distinct(gvkey, lpermco, lpermno, .keep_all = T) 
  
  # joining the rivals' permco to the dataset with the original dataset, that is, all_acq_and_rivals_id which has the acquirors' id info
  # plus the rivals' gvkeys
  # so we now have all the acquirors' identifying information, plus the rivals' gvkeys and permcos
  # we also keep track of the dates the rivals were added to COMPUSTAT/CRSP table
  all_acq_and_rivals_id_w_permcos_aux2 <- merge(all_acq_and_rivals_id, acq_rivals_gvkeys_and_permcos_aux1, by.x="gvkey2", by.y="gvkey", all.x=TRUE) %>% 
    rename(acq_rival_gvkey=gvkey2, acq_rival_permco=lpermco, acq_rival_permno=lpermno, acq_rival_linkdt=linkdt, acq_rival_linkenddt=linkenddt)
  
  # similarly to what we did before, hereby we keep track of any merger ids we might have lost, in particular,
  # if they did not have any competitors with competitor's score year below the merger announcement year
  missing_merger_ids <- setdiff(all_acq_and_rivals_id_aux$merger_id, all_acq_and_rivals_id_w_permcos_aux2$merger_id)
  
  # for those merger ids we merely assign an NA to the columns imported from Hoberg-Philips dataset 
  missing_data <- all_acq_and_rivals_id_aux %>% 
    filter(merger_id %in% missing_merger_ids) %>% group_by(merger_id, acq_permco) %>% slice(1) %>%
    mutate( year = NA,
            gvkey2 = NA,
            score = NA
    ) %>% ungroup()
  
  # finally add them back to the dataset
  all_acq_and_rivals_id_w_permcos <- bind_rows(all_acq_and_rivals_id_w_permcos_aux2, missing_data)
  
  # the next step is get their names, ticker, cusip as well as the starting and ending dates of their records
  # in CRSP (the reason for the latter will become clear in just a few lines)
  # for that end, we need the stock_id column and, to filter it, we need the rivals' permanent numbers
  acq_rivals_permcos <- all_acq_and_rivals_id_w_permcos %>% filter(!is.na(acq_rival_permco)) %>% distinct(acq_rival_permco) %>% pull()
  acq_rivals_permnos <- all_acq_and_rivals_id_w_permcos %>% filter(!is.na(acq_rival_permno)) %>% distinct(acq_rival_permno) %>% pull()
  
  # getting their id info and their (start and end) dates in CRSP
  # we also rename the date columns so that when we later merge this table back
  # we can distinguish the date record of acquirors and their rivals
  acq_rivals_id <- stock_id %>% select(permco, permno, namedt, nameendt, cusip, ncusip, ticker, comnam) %>% filter(permco %in% acq_rivals_permcos & permno %in% acq_rivals_permnos) %>% # subset dataset on the acquirors' rivals
    group_by(permco, permno) %>% # for each permno inside every permco
    mutate(namedt=min(namedt)) %>% # keep only the starting date for which each rival's stock data began being recorded in CRSP
    slice_max(order_by = nameendt, n=1, with_ties = F) %>% # find the ending date of each rival's stock data in CRSP (there are no NAs in this column)
    rename(acq_rival_end_date=nameendt, acq_rival_start_date=namedt,
           acq_rival_name=comnam, acq_rival_cusip=cusip, 
           acq_rival_ncusip=ncusip, acq_rival_ticker=ticker) %>% 
    ungroup()
  
  # now merge it back to the table of interest: all_acq_and_rivals_id_w_permcos which contains
  # the acquirors id info plus the rivals gvkey and (permco, permno)
  # this table will have all that data plus the rivals id info, i.e. name, cusip, ticker etc.
  all_acq_and_rivals_reg_id_info_aux1 <- merge(all_acq_and_rivals_id_w_permcos, acq_rivals_id, by.x=c("acq_rival_permco", "acq_rival_permno"), 
                                               by.y=c("permco", "permno"), 
                                               all.x=T) 
  
  # the final step is to pick the top-6 competitors for each firm
  # recall we always give priority to those whose competitor score year is closest (or even the same year)
  # but never a year or more after the merger announcement year
  # This means we first attempt to pick the top-5 rivals in the same year of the merger announcement year
  # and, if some are missing, we move down the years until we get all 5. 
  # In some cases, we will not get all 5 or even none at all
  # NB: although ultimately we want only the top5 competitors we shall start by retrieving 6
  # the reason is that in some cases the target firm will belong to the list of acquiror's competitors
  # (and vice-versa) meaning we'd effectively only have 4 competitors. Thus we increase the list to 6
  # so that in those cases we end up with 5. In the end (in particular in the final for loop)
  # we always pick at most (as in some cases we may not get all 5) the top-5 competitors as ranked by the competitor score
  
  # but we need to ensure every rival is uniquely assigned a (permco, permno) 
  # to that end, we pick those that have data in CRSP for the estimation and event window to begin with
  # otherwise it is redundant to get such competitor
  all_acq_and_rivals_reg_id_info_aux2 <- all_acq_and_rivals_reg_id_info_aux1 %>% group_by(merger_id) %>% filter( ifelse(is.na(acq_rival_permco), TRUE, acq_rival_start_date<= date_announced - 350 & 
                                                                                                                          (acq_rival_end_date>= date_announced + 40 | is.na(acq_rival_end_date) )  ) ) %>% ungroup()
  
  # however, that might not fully solve the issue. hereby we get a unique permno for every permco by picking that which has the highest amount of data points in CRSP
  # this rule is not so `scientific` but its enough for our purposes as per our previous filter we already have the (permco, permno) combinations that have enough data points
  # in CRSP to estimate the cumulative abnormal returns
      ### TAKING LONG TIME - THE LONGEST ACTUALLY
      # all_acq_and_rivals_reg_id_info_aux3 <- all_acq_and_rivals_reg_id_info_aux2 %>% group_by(merger_id, acq_rival_gvkey, acq_rival_permco) %>% distinct(acq_rival_permno, .keep_all = T) %>% 
      #   mutate(days_in_crsp = acq_rival_end_date - acq_rival_start_date) %>% slice_max(order_by = days_in_crsp, n=1, with_ties = F) %>% ungroup() %>% select(-days_in_crsp)
      # 
  all_acq_and_rivals_reg_id_info_aux3 <- all_acq_and_rivals_reg_id_info_aux2 %>% group_by(merger_id, acq_rival_gvkey, acq_rival_permco) %>% slice_max(acq_rival_start_date, n=1, with_ties = F) %>% ungroup()
  
  # finally we uniquely select a unique permco (and thereby unique permno given our filtering above) for each rival by, once again, picking that with the maximum amount
  # of data points
      # all_acq_and_rivals_reg_id_info_aux4 <- all_acq_and_rivals_reg_id_info_aux3 %>% group_by(merger_id, acq_rival_gvkey) %>% distinct(acq_rival_permco, .keep_all = T) %>% 
      #   mutate(days_in_crsp = acq_rival_end_date - acq_rival_start_date) %>% slice_max(order_by = days_in_crsp, n=1, with_ties = F) %>% ungroup() %>% ungroup() %>% select(-days_in_crsp)
  all_acq_and_rivals_reg_id_info_aux4 <- all_acq_and_rivals_reg_id_info_aux3 %>% group_by(merger_id, acq_rival_gvkey) %>% distinct(acq_rival_permco, .keep_all = T) %>% slice_max(acq_rival_start_date, n=1, with_ties = F) %>% ungroup()
  
  # now we can readily pick the top-6 competitors (ordered by those whose competitor score year is closest to the merger announcement year)
  num_of_rivals_per_firm <- num_of_rivals_per_firm + 1
  
  all_acq_and_rivals_reg_id_info_aux5 <- all_acq_and_rivals_reg_id_info_aux4 %>% group_by(merger_id) %>% slice_max(order_by = tibble(year, score), 
                                                                                                                   n = num_of_rivals_per_firm, 
                                                                                                                   with_ties = F) %>%
    ungroup()
  
  # once more we need to correct for any merger ids we might have lost as some firms
  # might not have any rival with sufficient data points (see filtering for all_acq_and_rivals_reg_id_info_aux2)
  # although those cases should be very rare
  missing_merger_ids <- setdiff(all_acq_and_rivals_reg_id_info_aux1$merger_id, all_acq_and_rivals_reg_id_info_aux5$merger_id) 
  
  # for those merger ids we merely assign an NA to the columns imported from Hoberg-Philips dataset 
  missing_data <- all_acq_and_rivals_reg_id_info_aux1 %>% 
    filter(merger_id %in% missing_merger_ids) %>% group_by(merger_id, acq_permco) %>% slice(1) %>%
    mutate( year = NA,
            gvkey2 = NA,
            score = NA,
            acq_rival_end_date = as.Date(NA), acq_rival_start_date = as.Date(NA),
            acq_rival_name = as.character(NA), acq_rival_cusip = as.character(NA), 
            acq_rival_ncusip = as.character(NA), acq_rival_ticker = as.character(NA)
    )
  
  # finally add them back to the dataset
  all_acq_and_rivals_reg_id_info_aux6 <- bind_rows(all_acq_and_rivals_reg_id_info_aux5, missing_data)
  
  # finally, rename some columns
  # this is important for we will then merge this dataframe with that of the targets
  # so it is important to distinguish which id info belongs to which firm (acquiror or target) competitor
  all_acq_and_rivals_reg_id_info <- all_acq_and_rivals_reg_id_info_aux6  %>%
    rename(acq_rival_score=score,
           acq_year_hp = year)
  
  # and store the updated M&A dataset locally
  saveRDS(all_acq_and_rivals_reg_id_info, "data/processed_data/id_data/acq_and_rivals_id_data.rds")
  
  #Disconnect from database
  dbDisconnect(wrds)
  
}

# NB: for privacy purposes the WRDS user and pass should be stored as environment variables
# To store your own credentials (otherwise code will not run), open Windows Powershell and run:  
# Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "WRDS_USER=your_username"
# Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "WRDS_PASS=your_pass"
# Then restart R Studio and you are good to go

acq_and_rivals_id_info <- get_and_append_acq_rivals_id_data(Sys.getenv("working_dir"),
                                                            Sys.getenv("WRDS_USER"), 
                                                            Sys.getenv("WRDS_PASS"),
                                                            5
) 

