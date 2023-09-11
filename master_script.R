# load necessary packages
library(dbplyr,quietly = T)
library(RSQLite,quietly = T)
library(DBI,quietly = T)
library(digest,quietly = T)
library(dplyr,quietly = T)
library(odbc,quietly = T)
library(gt,quietly = T)

# Look for the current working directory
wd_path <- function(x){paste0(getwd(),"/",x)}

# INSERT your study id (noted in Castor->Settings->Study->General)
study_id <- "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"

# Standard URL of Castor API
base_url <- "https://data.castoredc.com/"

# load functions for extraction of data from Castor
source(wd_path("manualcastorapi.R"))

# load functions for encryption
source(wd_path("Keykey.R"))

################################################ RUN THIS CODE BELOW ONCE AND REMOVE IT LATER ON
################################################ RUN THIS CODE BELOW ONCE AND REMOVE IT LATER ON

# These are the client ID (oauth_key) and client secret (oauth_secret) 
# find these on Castor->Account->Settings->Castor EDC API 
oauth_key <- "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
oauth_secret <- "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# GO to https://www.random.org/strings/ and generate 2 random strings
# On the website you should generate 2 random string of 16 and 32 characters long 
# (numeric digits, Uppercase, and Lowercase letters enabled)
# For the random iv, you need to use a random 16 character string
# For the random key, you need to use a random 32 character string
random_iv <- "XXXXXXXXXXXXXXXX"
random_key <- "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# Write the 2 random strings to new iv.key and script_key.key file
writeBin(object = random_iv,con = wd_path("iv.key"),size = 16)
writeBin(object = random_key,con = wd_path("script_key.key"),size = 32)

# Read the 2 random strings
iv <- readBin(wd_path("iv.key"), "raw", 16)
key <- readBin(wd_path("script_key.key"), "raw", 32)

# Use the random iv and random key to encrypt the oauth_key and oauth_secret
key_encryption(iv = iv, key = key)

# Place the oauth_secret and oauth_keys inside an encrypted database
putKey("oauth_secret",oauth_secret)
putKey("oauth_key",oauth_key)

################################################ RUN THIS CODE ABOVE ONCE AND REMOVE IT LATER ON
################################################ RUN THIS CODE ABOVE ONCE AND REMOVE IT LATER ON

################################################ KEEP THE CODE BELOW THIS LINE
################################################ KEEP THE CODE BELOW THIS LINE

# Read in the random iv and key to decrypt and encrypt objects
iv <- readBin(wd_path("iv.key"), "raw", 16)
key <- readBin(wd_path("script_key.key"), "raw", 32)
key_encryption(iv = iv, key = key)

# write a function that fetches the oauth_key and oauth_secret
oauth_key <- function(){fetchKey("oauth_key")}
oauth_secret <- function(){fetchKey("oauth_secret")}

# Extract all data from Castor
source("fetch_data.R")

# EXAMPLE ANALYSIS: SUMMARIZE number of patients per hospital
records %>%
  group_by(`_embedded.institute.name`) %>%
  summarise(`Number of records` = n()) %>%
  ungroup() %>% gt()
