# ------------------------------------------------------------------------------
# 6_merge_acq_and_tgt_data.R
#
# Author: Filipe Ribeiro Ferreira
#
# This script is the 6th out of a series of 9 scripts.
#
# It merges the acquiror and target datasets, which thus far have been handled
# in separate scripts, back into one dataset.
#
# This updated M&A dataset is then stored locally as a data frame
# ------------------------------------------------------------------------------

merge_acq_and_tgt_data <- function() {
  
  # The function imports the datasets of both the acquiror and the target (i.e.
  # which has appended their GVKEY and closest rivals) and merges the two under
  # one single dataset.
  
  # importing the dataset with the acquirers plus its rivals identifying info
  acq_subset_orbis_df_w_gvkeys_of_acq_and_top6_rivals <- readRDS(acq_filepath2)
  
  # lets rename it for simplicity
  acq_and_rivals_id_data <- acq_subset_orbis_df_w_gvkeys_of_acq_and_top6_rivals
  
  # making sure we dont get multiple matches per deal number
  acq_and_rivals_id_data <- acq_and_rivals_id_data %>%
    group_by(Deal.Number) %>%
    mutate(
      aux_id = paste0(Deal.Number, letters[row_number()])
    ) %>% ungroup()
  
  # we will also import that of the targets plus its rivals
  tgt_subset_orbis_df_w_gvkeys_of_acq_and_top6_rivals <- readRDS(tgt_filepath2)
  
  # lets rename it for simplicity
  tgt_and_rivals_id_data <- tgt_subset_orbis_df_w_gvkeys_of_acq_and_top6_rivals
  
  tgt_and_rivals_id_data <- tgt_and_rivals_id_data %>%
    group_by(Deal.Number) %>%
    mutate(
      aux_id = paste0(Deal.Number, letters[row_number()])
    ) %>% ungroup()
  
  # merging the two under a single data frame
  # note that by firms we mean the acquirors, targets and all their respective
  # rivals
  all_firms_id_data_aux1 <- merge(acq_and_rivals_id_data, tgt_and_rivals_id_data %>% select( Deal.Number, aux_id, matches("Target|target|tgt")), by="aux_id",
                                all=T)
  
  # remove duplicate deal number col
  all_firms_id_data_aux2 <- all_firms_id_data_aux1 %>% mutate( Deal.Number = ifelse( !is.na(Deal.Number.x), Deal.Number.x, Deal.Number.y ) ) %>%
    select(-Deal.Number.x, - Deal.Number.y)
  
  #       all_reg_id_info_aux2 <- merge(all_acq_and_rivals_reg_id_info, all_tgt_and_rivals_reg_id_info %>% select( aux_id, matches("Target|target|tgt")), by="aux_id")
  
  all_firms_id_data <- all_firms_id_data_aux2 %>% 
    mutate( acq_rival_gvkey = ifelse( (acq_rival_gvkey == tgt_gvkey) & !is.na(tgt_gvkey), NA, acq_rival_gvkey ),
            tgt_rival_gvkey = ifelse( tgt_rival_gvkey == acq_gvkey & !is.na(acq_gvkey) , NA, tgt_rival_gvkey ) ) 
  
  # save dataset as RDS file
  filepath1 <<- paste0("data/raw_data/orbis/with_merger_authority_decision/batch/",k,"/", k/k+6, "_acq_tgt_and_rivals_merged.rds")
  saveRDS(all_firms_id_data, filepath1)
  
}

merge_acq_and_tgt_data()
