# ------------------------------------------------------------------------------
# 1_extract_cusips.R
#
# Author: Filipe Ribeiro Ferreira
#
# This script imports the Orbis M&A dataset with the merger authority decision
# and extracts the CUSIP identifier from the ISIN variable for US and Canadian
# firms (only for these firms does the ISIN encapsulate the CUSIP)
#
# DELETE FOLLOWING COMMENTS AND MOVE TO MASTER SCRIPT
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
# ------------------------------------------------------------------------------

extract_cusips_from_isin <- function() {
  
  # The function imports M&A batch and extracts the CUSIP identifiers
  # from the ISIN for US and Canadian firms.
  
  # import data
  subset_orbis_df <- readRDS(filepath1)
  
  # create the CUSIP - note we drop the last digit as it is merely a mathematical
  # check-digit
  
  # first for acquirors
  subset_orbis_df <- subset_orbis_df %>% mutate(acq_cusip =
                                        ifelse( substr(Acquiror.ISIN.number, 1, 2) %in% c("US", "CN"),
                                        substr(Acquiror.ISIN.number, 3, 10),
                                        NA) 
                                      )
  
  # and repeat for the targets
  subset_orbis_df <- subset_orbis_df %>% mutate(tgt_cusip =
                                        ifelse( substr(Target.ISIN.number, 1, 2) %in% c("US", "CN"),
                                                substr(Target.ISIN.number, 3, 10),
                                                NA) 
  )
  
  # finally set all empty cells to NA
  subset_orbis_df[subset_orbis_df == ""] <- NA
  
  # save dataset as RDS file
  filepath2 <<- paste0("data/raw_data/orbis/with_merger_authority_decision/batch/",k,"/", k/k+1, "_with_cusip_extracted.rds")
  saveRDS(subset_orbis_df, filepath2)
  
}

# run function
extract_cusips_from_isin()


