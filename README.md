# **Estimating Cumulative Abnormal Returns of M&A Deals**

## **Author:**
Filipe Ferreira

## **Overview**
This repository contains a set of R scripts used to estimate the cumulative abnormal returns (CAR) of firms involved in mergers and acquisitions (M&A). The analysis includes both the merging parties (acquirer and target) and their top-5 rivals, as identified using the **Hoberg-Philips** dataset. The study covers an event window of 15 trading days before and after the merger announcement date (31 days in total).

The project is part of RA work for Jamie Coen at Imperial College and Patrick Coen (Toulouse School of Economics).

## **Data Requirements**
- **LSEG/Refinitiv workspace:**  
  - Database used to obtain M&A data from 1989 to 2023 (inclusive).
- **[(Baseline) Hoberg-Philips Dataset](https://hobergphillips.tuck.dartmouth.edu/tnic_basedata.html):**  
  - The dataset produces a competitor score ranked from 0 to 1, based on the (cosine) similarity score of the product section of the firms' 10-K filings. 
- **CRSP Data:**  
  - Stock data for S&P500, merging parties and respective competitors.

Using Refinitiv as database, M&A deals were filtered by those above $1Mn in value and whose merging parties belonged to the US. It spans the years of 1989 up until 2023 and contains deal info such as merger announcement date, firm name, industry, stock ticker, market value, among other. The rivals of each merging party were retrieved from the Hoberg-Philips dataset and then matched with CRSP to obtain their stock data.

Matching was carried using the CRSP and COMPUSTAT unique identifiers (permco, permno; GVKEY).

## **Methodology**
The analysis was carried using the market-model whereby we produce a linear fit of historical stock data of the firm on the market (i.e. S&P500). An estimation window of 200 trading days prior to the event window was used to estimate the coefficients. Formally,

<div align="center">

$E[R_{it} | X_t] = \mu_i + \beta_i R_{mt}$

</div>

where $t=1, 2, ..., 200$ represents the trading days of the estimation window and
- $\mathbf{R_{it}}$ denotes the return of the stock $i$ at time $t$.
- $\mathbf{\mu_i}$ is the intercept representing firm-specific average return.
- $\mathbf{\beta_i}$ is sensitivity of the firm’s returns to the market.
- $\mathbf{R_{mt}}$ is the benchmark market return (e.g., S&P500).

Such model is then leveraged to extrapolate the firm's returns throughout the event window (15 trading days surrounding the merger announcement date). It thus produces the counterfactual, that is, the firm's estimated returns in absence of the deal announcement. 

The difference between the _actual_ company returns and those predicted (through the market model) is defined as the _abnormal_ returns. The latter were computed in cumulative terms.

---

## **Project Workflow**
The project is divided into several steps, each assigned to a single script, that are executed by a **master script (`master_script.R`)**. The steps are as follows:

### **Step 1: Import and Clean M&A Data**
- **Script:** `1_import_and_clean_MnA_data.R`
- **Purpose:** Imports and cleans the raw M&A data from the LSEG workspace, including basic data cleaning steps.

### **Step 2: Get Acquiror Identifiers**
- **Script:** `2_get_and_append_acq_permcos_and_permnos.R`
- **Purpose:** Matches acquirors in the M&A dataset to CRSP - with the stock ticker and CUSIP - to obtain their permanent company number (`permco`) and stock level (`permno`) CRSP identifiers. These will be key to later retrieve their stock data.

### **Step 3: Get Target Identifiers**
- **Script:** `3_get_and_append_tgt_permcos_and_permnos.R`
- **Purpose:** Similar to Step 2 but for target firms, appending their CRSP identifiers (permco and permno) to the M&A dataset. By the end of this step, the dataset includes all initial M&A info plus CRSP permanent identifiers.

### **Step 4: Identify Acquiror Rivals**
- **Script:** `4_get_and_append_acq_rivals_id_data.R`
- **Purpose:** Retrieves the top-5 rivals for acquirors, at the time of the merger, based on the Hoberg-Philips dataset. It ranks competitors from 0 to 1 as computed by the similarity score of their product description on SEC filings. 

### **Step 5: Identify Target Rivals**
- **Script:** `5_get_and_append_tgt_rivals_id_data.R`
- **Purpose:** Identical to Step 4 but for targets firms.

### **Step 6: Get Acquiror and Rivals’ Stock Data**
- **Script:** `6_get_stck_data_of_acq_and_rivals.R`
- **Purpose:** With the CRSP identifiers of merging parties and rivals scooped, this script fetches the historical stock price and returns data for all acquirors and respective rivals.

### **Step 7: Get Target and Rivals’ Stock Data**
- **Script:** `7_get_stck_data_for_tgt_and_rivals.R`
- **Purpose:** Similar to Step 6 but for target firms and their rivals.

### **Step 8: Import S&P500 Data**
- **Script:** `8_get_sp500_data.R`
- **Purpose:** Imports S&P500 data, used to estimate the counterfactual returns of the individual firm had the merger not taken place.

### **Step 9: Estimate Abnormal and Cumulative Abnormal Returns**
- **Script:** `9_run_OLS.R`
- **Purpose:** Calculates abnormal returns and cumulative abnormal returns (CAR) for acquirors, targets, and rivals over the 31-day event window (15 days before and after the merger date) using an OLS regression model.

### **Master Script**
- **Script:** `master_script.R`
- **Purpose:** Automatically runs all the scripts sequentially and processes the data in three batches for time-efficiency purposes:  
  - 1989-1999  
  - 2000-2009  
  - 2010-2023  

---

## **Prerequisites**
- **R and RStudio:** Ensure you have R installed with a working environment.
- **Required R Packages:**  
  Install the following R packages:
  ```r
  install.packages(c("dplyr", "readxl", "lubridate", "bizdays", "tidyverse", "DBI", "RPostgres", "scales", "RSQLite"))
  ```
- **WRDS Database Access:**  
  This project connects to the WRDS database to retrieve financial data. Ensure you have WRDS credentials set as environment variables:
  ```bash
  # Example commands (in PowerShell)
  Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "WRDS_USER=your_username"
  Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "WRDS_PASS=your_password"
  ```
- **Downloading M&A Deals:**  
  Ensure to download M&A deals above $1Mn of US firms through the LSEG/Refinitiv Workspace.

---

## **Output**
- The project generates a dataset with the following outputs for each deal:
  - Cumulative Abnormal Returns (CAR) over the 31-day event window relative to the market both in relative (i.e. using stock returns) and absolute terms (using stock prices).
  - Firm-specific information (industry, ticker, market cap, rivals, etc.).

---
