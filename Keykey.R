key_encryption <- function(iv, key){
# browser()
#Encrypted key management
  encKey <- function(x){
    aes <- AES(key, mode = "CFB", IV = iv)
    aes$encrypt(x)
  }
  
  decKey <- function(x){
    aes <- AES(key, mode = "CFB", IV = iv)
    aes$decrypt(x)
  }
  
  putKey <- function(keyname, keyvalue){
    dbK <- dbConnect(RSQLite::SQLite(), wd_path("Keys.db"))
    dfKeys <- tibble(keyname, keyvalue) %>%
      mutate(keyvalue = lapply(keyvalue, encKey))
    dbWriteTable(dbK, "keys", dfKeys, append = T)
  }
  
  fetchKey <- function(x){
    dbK <- dbConnect(RSQLite::SQLite(), wd_path("Keys.db"))
    out <- tbl(dbK, "keys") %>%
      filter(`keyname` == x) %>%
      select(keyvalue) %>%
      collect() %>%
      `[[`(1) %>%
      `[[`(1) %>%
      decKey()
    return(out)
    dbDisconnect(dbK)
  }
  
list2env(list(encKey = encKey, decKey = decKey, putKey = putKey, fetchKey = fetchKey),envir = .GlobalEnv)
}