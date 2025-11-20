# **Estimating Cumulative Abnormal Returns of M&A Deals**

## **Author:** Filipe Ferreira

## **Overview**
This repository contains a set of R scripts used to estimate the cumulative abnormal returns (CAR) of firms involved in mergers and acquisitions (M&A). This includes both merging parties (acquirer and target) as well as their top-5 rivals, obtained from the **Hoberg-Philips** dataset. The abnormal returns are estimated for an event window of 15 trading days before and after the merger announcement date (31 days in total).

The project is part of RA work for Jamie Coen at Imperial College and Patrick Coen (Warwick Business School).

## **Data Sources**
**Orbis M&A:**  
  - Orbis M&A is a comprehensive database from Moody's containing more than 3 million M&A deals since 1997 up until today.
  - The working sample consisted of 75k deals with a value of at least $1Mn and in which one of the merging parties must be a U.S firm.
  - It spans the years of 1989 up until 2023 and contains deal and merging parties' information such as merger announcement date, firm name, CUSIP, industry, stock ticker, market value, among other.
  - **Key firm identifier used:** CUSIP (which needed to be extracted from the ISIN).[^1]

[^1]: The former is a North American financial security identifier comprised of 9 digits while the latter is a 12-digit international security identifier that contains the CUSIP when applied to US firms.
    
**[(Baseline) Hoberg-Philips](https://hobergphillips.tuck.dartmouth.edu/tnic_basedata.html) (H-P henceforth):**  
  - The dataset produces competitor scores of U.S public firms ranked from 0 (not a competitor) to 1 (very close competitor), based on the (cosine) similarity score of the product section of the firms' 10-K filings. The database also reports the year of the competitor score which is simply the fiscal year of the 10-K filing (i.e. no lags). That is, a rival score with a date of 2020 means it was computed using the 10-K published in 2020 by each firm.
  - Firms are identified via Compustat's GVKEY (firm unique permanent identifier). Its coverage dates from 1988 to present.
  - **Key firm identifier used:** GVKEY - a Compustat unique firm identifier that tracks it throughout all its history, regardless of corporate events such as mergers or spin-offs.
    
**Compustat:** 
  - Compustat is a market and corporate financial database dating as far back as the 1950s. 
  - The dataset contains stock data, namely stock prices (adjusted for events such as stock splits), of S&P500, merging parties and respective competitors.
  - **Key firm identifier used:** GVKEY and CUSIP

## **Matching steps**

1. Merge Orbis dataset with Compustat via CUSIP to retrieve the merging parties' GVKEYS.
2. Filter the H-P dataset with the latter's GVKEYS.
3. The top-5 rivals ranked by H-P's competitors score (0 being the lowest and 1 the highest) were picked after applying the following filters:

   - Only the latest rival score of each competitor was kept.
   - Year of the competitor score (which equals the 10-K publishing date) was either the same as the merger announcement date or at most 3 years before.
   - Competitors needed to have stock data available in Compustat at least 2 months after and 3 years before the merger announcement date.

4. With the GVKEYS of the merging parties and respective competitors, stock data was retrieved from Compustat.


## **Methodology**
The analysis was carried using the market-model whereby we produce a linear fit of historical stock data of the firm on the market (i.e. S&P500). An estimation window of 200 trading days prior to the event window was used to estimate the market coefficient (i.e. how correlated is the individual stock with the market portfolio). Formally,

<div align="center">

$E[R_{i,t} | X_t] = \mu_i + \beta_i R_{m,t}$

</div>

where $t=1, 2, ..., 200$ represents the trading days of the estimation window and
- $\mathbf{R_{i,t}}$ denotes the return of the stock $i$ at time $t$.
- $\mathbf{\mu_i}$ is the intercept representing firm-specific average return.
- $\mathbf{\beta_i}$ is sensitivity of the firm’s returns to that of the market.
- $\mathbf{R_{m,t}}$ is the benchmark market return (e.g., S&P500).

Such model is then leveraged to extrapolate the firm's returns throughout the event window (15 trading days surrounding the merger announcement date) had the M&A deal never been announced. It thus produces the counterfactual - the firm's estimated returns in absence of the deal announcement. This will be subtracted from the _actual_ observed stock returns of the firm to produce the abnormal returns (which are reported in cumulative terms). 

---

## **Project's scripts**
The project is divided into several steps, each assigned to a single script, that are executed by a **master script (`master_script.R`)**. The steps are as follows:

### **Step 1: Extract first 6 digits of CUSIP from ISIN**
- **Script:** `1_extract_cusips.R`
- **Purpose:** Extracts the 6-digit CUSIP from the ISIN variable (the ISIN contains the CUSIP when it pertains to US firms) to allow matching with Compustat. 

### **Step 2: Match acquirors with Compustat**
- **Script:** `2_append_acq_gvkey.R`
- **Purpose:** Match acquirors in Compustat using the CUSIP to retrieve its GVKEY. The former's last digit was removed beforehand for it is simply a mathematical check digit. 

### **Step 3: Match acquirors with Compustat**
- **Script:** `3_append_tgt_gvkey.R`
- **Purpose:** Identical to Step 2 but for target firms.

### **Step 4: Identify Acquiror Rivals**
- **Script:** `4_get_and_append_acq_rivals_id_data.R`
- **Purpose:** Filter the Hoberg-Philips dataset by the acquiror's GVKEYS to retrieve its top-5 rivals which are ranked by H-P's competitors score. The latter is a obtained by computing the (cosine) similarity score of firm's product description on SEC filings with that of the rival. It ranges from 0 to 1 (0 being the lowest and 1 the highest) 

### **Step 5: Identify Target Rivals**
- **Script:** `5_get_and_append_tgt_rivals_id_data.R`
- **Purpose:** Identical to Step 4 but for target firms.

### **Step 6: Get Acquiror and Rivals’ Stock Data**
- **Script:** `6_merge_acq_and_tgt_data.R`
- **Purpose:** By the end of step 5, the code has generated two datasets. Both contain deal information (e.g. announcement, deal value, bid premium, deal type, etc.). However, one includes the acquirors' and rivals' GVKEYS as well as their competitor scores, while the second contains the analogous data but for targets. This script combines both datasets into one.

### **Step 7: Get All Firms (Merging Parties plus Rivals) Stock Data**
- **Script:** `7_get_firms_stock_data.R`
- **Purpose:** Once all the identifying data has been retrieved, the next step is to scoop stock prices for individual firms and S&P500 returns (next script) from Compustat. 

### **Step 8: Get S&P500 Stock Data**
- **Script:** `8_get_sp500_data.R` 
- **Purpose:** Obtain S&P500 index price from Compustat.

### **Step 9: Clean Stock Data (for Regressions)**
- **Script:** `9_clean_stock_data.R`
- **Purpose:** Hereby stock data is cleaned and prepared for regressions to produce the (cumulative) abnormal returns. Among others, the key steps applied were: i) adjust stock prices for events such as stock splits (Compustat reports a variable that keeps track of such events); ii) compute stock returns and total market value using i); iii) ensure the target was not mixed up in the list of acquiror's rivals obtained from H-P database, and vice-versa. 

### **Step 10: Create Funcion to Estimate Cumulative Abnormal Returns (CAR)**
- **Script:** `10_run_OLS_regressions.R`
- **Purpose:** Creates the function - which shall be called in the following script - that runs the market-model regression and produces the cumulative abnormal returns.

### **Step 11: Estimate CAR and Report Them in a Dataframe**
- **Script:** `11_estimate_cum_abn_ret.R`
- **Purpose:** Calls the function created in `10_run_OLS_regressions.R` to produce the CAR and report them in a final output table.

### **Master Script**
- **Script:** `master_script.R`
- **Purpose:** Automatically runs all the scripts sequentially and processes the data in batches of 5k deals (to avoid memory clogging).  

---

## **Prerequisites**
- **R and RStudio:** Ensure you have R installed with a working environment.
- **Required R Packages:**  
  The code requires prior installation of the following packages to run:
  ```r
  install.packages(c("dplyr", "tidyr", "lubridate", "RPostgres"))
  ```
- **WRDS Database Access:**  
  To establish a connection to the WRDS API, the code requires a WRDS username and password. For privacy purposes, these have been stored as environmental variables. Run the two lines below in the Windows PowerShell to create them.
  ```bash
  # Example commands (in PowerShell)
  Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "WRDS_USER=your_username"
  Add-Content c:\Users\$env:USERNAME\Documents\.Renviron "WRDS_PASS=your_password"
  ```

---

## **Output**
The project generates a final dataset with the following variables:
  - Deal information such as announcement date, merging parties GVKEY, industry as well as deal type and bid premium. 
  - Cumulative Abnormal Returns (CAR) over the 31-day event window estimated both in relative terms (i.e. with stock returns) and in absolute terms (i.e. with stock prices). Standard deviations are also reported.
---
