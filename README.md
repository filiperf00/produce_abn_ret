Cumulative Abnormal Returns Analysis for Mergers
Overview
This repository contains a set of R scripts designed to analyze the cumulative abnormal returns (CAR) of companies involved in mergers and acquisitions (M&A). The analysis includes both the merging parties (acquirer and target) and their top-5 rivals, as identified using the Hoberg-Philips dataset. The study covers an event window of 15 trading days before and after the merger announcement date.

The project is part of an RA project for Professors Jamie Coen and Patrick Coen.

Project Workflow
The project follows a step-by-step process, executed by a master script (master_script.R) that orchestrates the entire pipeline. Here’s a brief explanation of each step:

Step 1: Import and Clean M&A Data
Script: 1_import_and_clean_MnA_data.R
Purpose: Imports and cleans the raw M&A data from the LSEG workspace, including basic transformations and exclusion of financial firms if specified.
Step 2: Get Acquiror Identifiers
Script: 2_get_and_append_acq_permcos_and_permnos.R
Purpose: Matches acquirors in the M&A dataset with their permanent company number (permco) and issue number (permno) using CRSP identifiers, essential for retrieving stock data.
Step 3: Get Target Identifiers
Script: 3_get_and_append_tgt_permcos_and_permnos.R
Purpose: Similar to Step 2 but for target firms, appending their CRSP identifiers to the M&A dataset.
Step 4: Identify Acquiror Rivals
Script: 4_get_and_append_acq_rivals_id_data.R
Purpose: Retrieves the top-5 rivals for acquirors based on the Hoberg-Philips dataset, prioritized by proximity to the merger announcement date.
Step 5: Identify Target Rivals
Script: 5_get_and_append_tgt_rivals_id_data.R
Purpose: Similar to Step 4 but for targets, identifying their top-5 rivals.
Step 6: Get Acquiror and Rivals’ Stock Data
Script: 6_get_stck_data_of_acq_and_rivals.R
Purpose: Fetches historical stock price and return data for acquirors and their top-5 rivals using CRSP identifiers.
Step 7: Get Target and Rivals’ Stock Data
Script: 7_get_stck_data_for_tgt_and_rivals.R
Purpose: Similar to Step 6 but for target firms and their rivals.
Step 8: Import S&P500 Data
Script: 8_get_sp500_data.R
Purpose: Imports S&P500 data, used to estimate abnormal returns relative to the market.
Step 9: Estimate Abnormal and Cumulative Abnormal Returns
Script: 9_run_OLS.R
Purpose: Calculates abnormal returns and cumulative abnormal returns (CAR) for acquirors, targets, and rivals over the 31-day event window (15 days before and after the merger date) using an OLS regression model.
Master Script
Script: master_script.R
Purpose: Automatically runs all the scripts sequentially and processes the data in three batches:
1989-1999
2000-2009
2010-2023
