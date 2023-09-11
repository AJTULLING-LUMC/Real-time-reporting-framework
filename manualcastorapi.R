library(httr)
library(jsonlite)
library(foreach)
library(magrittr)
library(tidyr)
library(labelled)
library(haven)

#Most of these functions require base_url to be set to the Castor server:
base_url <- "https://data.castoredc.com/"

# Most of these functions require castorApiToken to be set to the result of castorOAuth
# e.g.: 
# castorApiToken <- castorOAuth("some_key", "oauth_secret", base_url)


# labeldf and associated functions require fields to be set to fields with option groups attached
# fields <- getEndpoint(paste("api", "study", study_id, "field?include=optiongroup", sep = "/"))
# where study_id is the ID of the current study

castorOAuth <- function(key, secret, baseurl) {
  castor_app <- httr::oauth_app("CastorEDC", key = key, secret = secret)
  
  castor_endpoint <- httr::oauth_endpoint(request = NULL,
                                         base_url = paste0(baseurl, "oauth"),
                                         access = "token",
                                         "authorize")
  
  castor_token <- httr::oauth2.0_token(castor_endpoint,
                                      castor_app,
                                      client_credentials = TRUE,
                                      use_oob = FALSE,
                                      cache = FALSE)
  
  return(castor_token)
}
getAllPagesNew <- function (pagedRequest, ..., rbind = T, included_fields = NULL, restart_on_total_change = T){
  out <- list()
  i <- 1
  dfcols <- character()
  total_count <- NULL
  repeat{
    content <- rawToChar(pagedRequest$content)
    parsedContent <- fromJSON(content, simplifyVector = F)
    waitForRateLimit(pagedRequest)
    if(i == 1){
      total_count <- parsedContent$total_items
      first_url <- pagedRequest$url
    }else if(!identical(total_count, parsedContent$total_items) && !restart_on_total_change){
      warning("Total items changed during data fetch! Pages may be shifted, data is unreliable")
    }
    if(!identical(total_count, parsedContent$total_items) && restart_on_total_change){
      warning("Total items changed during data fetch. Restarting fetch at the start.")
      i <- 1
      dfcols <- character()
      out <- list()
      pagedRequest <- VERB(pagedRequest$request$method, url = first_url, ...)
    }else{
      out <- c(out, list(parsedContent$`_embedded`[[1]]))
      dfcols <- unique(c(dfcols, unlist(lapply(colnames(out[[i]]), 
                                               function(x){
                                                 if(is.data.frame(out[[i]][[x]])){
                                                   return(x)
                                                 }else{
                                                   return(character())
                                                 }
                                               })))) #List data frame columns
      nextURL <- unlist(parsedContent$`_links`$`next`)
      if(is.null(nextURL)){ #Stop if last request
        break
      }
      i <- i + 1
      pagedRequest <- VERB(pagedRequest$request$method, url = nextURL, ...) #Next request
    }
    
    
  }

  if(!is.null(included_fields)) out <- lapply(out, function(x) x[included_fields])
  if(rbind)
    out <- make_data_frame_castor(out)
  return(out)
}

waitForRateLimit <- function(request){
  if(!is.null(request$headers$`x-ratelimit-remaining`) && request$headers$`x-ratelimit-remaining` == "0" && !is.null(request$headers$`x-ratelimit-retry-after`)){
    # Note: x-ratelimit-retry-after appears to be POSIX timestamp instead of a valid HTTP date or time interval
    # Clarification/modification has been requested
    retry_when <-  as.POSIXct(as.numeric(request$headers$`x-ratelimit-retry-after`), tz = "UTC", origin = "1970-01-01")
    wait_time <- difftime(retry_when, Sys.time(), units = "secs")
    if(wait_time > 0) Sys.sleep(wait_time)
  }
}

make_data_frame_castor <- function(list_pages_items){
  # Merge pages
  list_items <- do.call(c, list_pages_items)
  names_list_items <- character()
  
  # Identify all occurring names
  for(l in list_items){
    names_list_items <- union(names(l), names(list_items))
  }
  
  # Fill in missing names
  for(l in seq_along(list_items)){
    missing_names <- names_list_items[!names_list_items %in% names(list_items[[l]])]
    # Fill in missing names
    list_items[[l]][missing_names] <- NA
    # Ensure common order
    list_items[[l]] <- list_items[[l]][names_list_items]
  }
  
  
  # Wrap list items in an additional list
  list_items <- lapply(list_items, function(x){
    x[vapply(x, is.list, logical(1))] <- lapply(x[vapply(x, is.list, logical(1))], list)
    x
  })
  
  
  list_dfs <- lapply(list_items, function(x) do.call(tibble, null_to_na(x)))
  data_frame_output <- do.call(rbind, list_dfs)
  data_frame_output
}

unnest_all_wider <- function(df, recursive = F, names_sep = ".", ...){
  for(c in colnames(df)){
    if(is.list(df[[c]])){
      df <- unnest_wider(df, !!sym(c), names_sep = names_sep, ...)
      if(recursive){
        df <- unnest_all_wider(df, recursive, names_sep, ...)
        break
      }
    }
  }
  df
}

null_to_na <- function(some_list){
  some_list[vapply(some_list, is.null, logical(1))] <- NA
  some_list
}


getEndpoint <- function(endpoint, rbind = T, included_fields = NULL){
  request <- GET(base_url, path = endpoint,
                 config(token = castorApiToken), accept_json())
  return(getAllPagesNew(request, config(token = castorApiToken), accept_json(), rbind = rbind, included_fields = included_fields))
}

verbEndpoint <- function(verb, endpoint, jsonList = NULL){
  VERB(verb, base_url, path = endpoint, config(token = castorApiToken), accept_json(), encode = "json", body = jsonList)
}

sendSurvey <- function(record_id, survey_package_id, mail_subject, mail_text){
  if(anyNA(c(record_id, survey_package_id, mail_subject, mail_text)) | length(c(record_id, survey_package_id, mail_subject, mail_text)) != 4){
    stop("Ongeldige input naar sendSurvey! Survey niet gepoogd te verzenden")
    return(F)
  }
  callString <- paste0(deparse(match.call()), collapse = " ")
  params <- list(
    "record_id" = record_id,
    "survey_package_id" = survey_package_id,
    "package_invitation_subject" = mail_subject,
    "package_invitation" = mail_text,
    "auto_send" = T,
    "auto_lock_on_finish" = F
  )
  response <- verbEndpoint("POST", endpoint = paste("api", "study", study_id, "surveypackageinstance", sep = "/"), params)
  waitForRateLimit(response)
  if(response$status_code >= 400){
    fetchBack <- getEndpoint(paste("api", "study", study_id, "surveypackageinstance", sep = "/"))
    fetchBack <- fetchBack[fetchBack$record_id == record_id & fetchBack$survey_package_id == survey_package_id, ]
    responseBody <- rawToChar(response$content)
    response %>%
      capture.output(.) %>%
      paste(collapse = "\r\n") %>%
      paste("Fout bij versturen survey!", "Response: ", record_id, ., "ResponseBody:", responseBody, "Call:", callString, if_else(nrow(fetchBack) == 0, "Survey package NIET aangemaakt. Wordt WEL opnieuw geprobeerd", "Survey package WEL aangemaakt maar alsnog fout. Wordt NIET opnieuw geprobeerd"), sep = "\r\n") %>%
      notifyErik()
    return(F)
  }else{
    return(T)
  }
}
updateField <- function(record_id, field_id, new_value){
  params <- list(
    "field_value" = as.character(new_value)
  )
  response <- verbEndpoint("POST", endpoint = paste("api", "study", study_id, "record", record_id, "study-data-point", field_id, sep = "/"), params)
  stop_for_status(response)
  waitForRateLimit(response)
  return(response)
}




labeldf <- function(df, cast = T){
  for(cn in colnames(df)){
    field <- fields %>%
      filter(field_variable_name == cn)
    if(NROW(field) == 1){
      if(length(field$option_group) == 1 && !is.na(field$option_group[[1]])){
          if(field$field_type == "checkbox"){
            replacementColumn <- lapply(df[[cn]], function(x){
              optvalues <- field$option_group[[1]]$options %>% vapply(function(x) x$value, character(1))
              names(optvalues) <- field$option_group[[1]]$options %>% vapply(function(x) x$name, character(1))
              if(is.na(x)){
                out <- character(0)
                val_labels(out) <- optvalues
                out <- list(as_factor(out))
                out <- structure(out, .Names = cn, row.names = integer(0), class = "data.frame")
              }else{
                out <- strsplit(x, ";", T)
                out <- lapply(out, function(x){
                  val_labels(x) <- optvalues
                  as_factor(x, levels = "label")
                })
                out <- structure(out, .Names = cn, row.names = seq_len(length(out[[1]])), class = "data.frame")
              }
              
              out
            }
          )
          df[[cn]] <- replacementColumn
        }else{
          optvalues <- field$option_group[[1]]$options %>% vapply(function(x) x$value, character(1))
          names(optvalues) <- field$option_group[[1]]$options %>% vapply(function(x) x$name, character(1))
          val_labels(df[cn]) <- optvalues
        }
      }else if(cast && field$field_type %in% c("date", "time", "datetime", "numeric")){
        if(field$field_type == "date"){
          df[cn] <- as.Date(df[[cn]], format = "%d-%m-%Y")
        }else if(field$field_type == "numeric"){
          df[cn] <- as.numeric(df[[cn]]) 
        }else if(field$field_type == "time"){
          #df[cn] <- strptime(df[[cn]], format = "%H:%M") #To-do: validate when we have actual data in these fields
        }else if(field$field_type == "datetime"){
          #df[cn] <- strptime(df[[cn]], format = "%d-%m-%Y %H:%M")
        }
      }
      var_label(df[[cn]]) <- field$field_label
    }
  }
  as_factor(df, levels = "label")
}



fetchSurveys <- function(surveyPackageInstances, pivot = T, label = T, cast = T, excludeCalculated = T){
  if((!pivot) & label)
    stop("Can't label an unpivoted data frame")
  spiData <- foreach(surveypackageinstance = iter(surveyPackageInstances, by = "row"), .combine = c) %do% {
    getEndpoint(paste("api", "study", study_id, "record", surveypackageinstance$record_id, "data-point-collection", "survey-package-instance", surveypackageinstance$id, sep = "/"), rbind = F)
  }
  #Remove empty responses
  i <- 1
  while(i <= length(spiData)){
    if(length(spiData[[i]]) == 0){
      spiData[[i]] <- NULL
    }else{
      i = i + 1
    }
  }
  #Combine all pages to one data frame
  out <- make_data_frame_castor(spiData)
  out %<>%
    inner_join(fields, by = c("field_id" = "id"))
  
  if(excludeCalculated){
    out %<>% filter(field_type != "calculation")
  }
  
  #Pivot the data frame, then add labels to it
  if(pivot){
    out %<>%
      arrange(field_number) %>%
      select(record_id, survey_package_id, field_variable_name, field_value) %>%
      pivot_wider(names_from = field_variable_name, values_from = field_value)
  }
  if(label){
    out %<>% labeldf(cast = cast)
  }
  out
}

fetchReports <- function(reportInstances, pivot = T, label = pivot, cast = T, excludeCalculated = T, v2 = T){
  if(v2) return(fetchReportsv2(reportInstances, pivot, label, cast, excludeCalculated))
  if((!pivot) & label)
    stop("Can't label an unpivoted data frame")
  reportData <- foreach(reportInstance = iter(reportInstances, by = "row"), .combine = c) %do% {
    getEndpoint(paste("api", "study", study_id, "record", reportInstance$record_id, "data-point-collection", "report-instance", reportInstance$id, sep = "/"), rbind = F)
  }
  #Remove empty responses
  i <- 1
  while(i <= length(reportData)){
    if(length(reportData[[i]]) == 0){
      reportData[[i]] <- NULL
    }else{
      i = i + 1
    }
  }
  #Combine all pages to one data frame
  out <- make_data_frame_castor(reportData)
  
  if(NROW(out) == 0 | !"field_id" %in% colnames(out)){
    return(out)
  }
  out %<>%
    inner_join(fields, by = c("field_id" = "id"))
  
  if(excludeCalculated){
    out %<>% filter(field_type != "calculation")
  }
  
  #Pivot the data frame, then add labels to it
  if(pivot){
    out %<>%
      arrange(field_number) %>%
      select(record_id, report_instance_id, field_variable_name, field_value) %>%
      pivot_wider(names_from = field_variable_name, values_from = field_value)
  }
  if(label){
    out %<>% labeldf(cast = cast)
  }
  out
}


fetchReportsv2 <- function(reportInstances, pivot = T, label = pivot, cast = T, excludeCalculated = T){
  if((!pivot) & label)
    stop("Can't label an unpivoted data frame")
  if(!exists("reportdata")) {
    assign("reportdata", getEndpoint(paste("api", "study", study_id, "data-point-collection", "report-instance", sep = "/")), .GlobalEnv)
  }
  out <- reportdata %>% filter(report_instance_id %in% reportInstances$id)
  
  out %<>%
    inner_join(fields, by = c("field_id" = "id"))
  
  if(excludeCalculated){
    out %<>% filter(field_type != "calculation")
  }
  
  #Pivot the data frame, then add labels to it
  if(pivot){
    out %<>%
      arrange(field_number) %>%
      select(record_id, report_instance_id, field_variable_name, field_value) %>%
      pivot_wider(names_from = field_variable_name, values_from = field_value)
  }
  if(label){
    out %<>% labeldf(cast = cast)
  }
  out
}

rbind_pages_alt <- function(...){
  dfs <- list(...)
  out <- NULL
  df_as_rowlist <- function(df){
    out <- list()
    for(r in seq_len(NROW(df))){
      out[[r]] <- df[r,]
    }
    out
  }
  
  rbind_two <- function(df1, df2){
    if(!is.data.frame(df1)) return(df2)
    out <- list()
    shared_names <- union(colnames(df1), colnames(df2))
    for(c in shared_names){
      df1_data <- df1[[c]]
      df2_data <- df2[[c]]
      if(is.data.frame(df1_data)) df1_data <- df_as_rowlist(df1_data)
      if(is.data.frame(df2_data)) df2_data <- df_as_rowlist(df2_data)
      if(length(df1_data) != NROW(df1)) df1_data <- replicate(NROW(df1), NULL)
      if(length(df2_data) != NROW(df2)) df2_data <- replicate(NROW(df2), NULL)
      if(class(df1_data) != class(df2_data) & !all(is.na(df1_data)) & !all(is.na(df2_data))){
        df1_data <- as.list(df1_data)
        df2_data <- as.list(df2_data)
      }
      out[[c]] <- c(df1_data, df2_data)
    }
    as_tibble(out)
  }
  for(i in seq_along(dfs)){
    out <- rbind_two(out, dfs[[i]])
  }
  
  listcols_to_df <- function(df){
    for(c in colnames(df)){
      col <- df[[c]]
      if(is.list(col) && all(sapply(col, is.data.frame)) && all(sapply(col, NROW) == 1)){
        df[[c]] <- bind_rows(df[[c]])
      }
    }
    df
  }
  out <- listcols_to_df(out)
  out
}


createReportInstance <- function(record_id, report_id, report_name, parent_id){
  params <- list(
    "report_id" = report_id,
    "report_name_custom" = as.character(report_name),
    "parent_id" = parent_id
  )
  response <- verbEndpoint("POST", endpoint = paste("api", "study", study_id, "record", record_id, "report-instance", sep = "/"), params)
  stop_for_status(response)
  waitForRateLimit(response)
  return(response)
}
updateReportField <- function(record_id, field_id, report_id, new_value){
  params <- list(
    "field_value" = as.character(new_value)
  )
  response <- verbEndpoint("POST", endpoint = paste("api", "study", study_id, "record", record_id, "data-point", "report", report_id, field_id, sep = "/"), params)
  stop_for_status(response)
  waitForRateLimit(response)
  return(response)
}
# Requires reports to be set

copyReport <- function(report_instance_id, record_id_target, new_name = NULL, parent_id = NULL){
  stopifnot(is.character(report_instance_id))
  stopifnot(is.character(record_id_target))
  report <- reports %>%
    filter(id == report_instance_id)
  stopifnot(NROW(report) == 1)
  if(!is.character(new_name))
    new_name <- report$name
  if(!is.character(parent_id))
    parent_id <- report$parent_id
  newreport <- createReportInstance(record_id_target, report$`_embedded`$report$id, new_name, parent_id)
  newreport <- content(newreport, as = "parsed", type = "application/json")  
  reportdata <- fetchReports(report, pivot = F, label = F, excludeCalculated = T) %>%
    arrange(field_number)
  foreach(r = iter(reportdata, by = "row")) %do% {
    updateReportField(record_id_target, r$field_id, newreport$id, r$field_value)
  }
  subreports <- reports %>%
    filter(parent_type == "reportInstance" & parent_id == report_instance_id)
  
  foreach(r = iter(subreports, by = "row")) %do% {
    copyReport(r$id, record_id_target, parent_id = newreport$id)  
  }
}

unnest_list_cols_wide <- function(df){
  list_cols <- names(df)[sapply(seq_len(NCOL(df)), function(x) is.list(df[[x]]))]
  for(l in list_cols){
    df[[l]] <- lapply(df[[l]], function(y){
      values_row <- y %>% as.list() %>% unlist() %>% as.character()
      if(length(values_row) == 0) return(character())
      names(values_row) <- paste(l, values_row, sep = "_")
      return(values_row)
    })
  }
  unnest_wider(df, where(is.list)) 
}

fetch_full_export_csv_text <- function(parse = TRUE){
  study_data_response <- GET(base_url, path = paste("api", "study", study_id, "export", "data", sep = "/"), config(token = castorApiToken), accept("text/csv"))
  content_text <- content(study_data_response, as = "text", encoding = "utf8")
  stop_for_status(study_data_response, paste0("fetch csv:\n", content_text))
  if(parse) content_text <- parse_full_export_csv_text(content_text)
  return(content_text)
}

parse_full_export_csv_text <- function(csv_text){
  huge_csv <- readr::read_delim(I(csv_text), delim = ";", col_types = readr::cols(
    `Study ID` = readr::col_character(),
    `Record ID` = readr::col_character(),
    `Form Type` = readr::col_character(),
    `Form Instance ID` = readr::col_character(),
    `Form Instance Name` = readr::col_character(),
    `Field ID` = readr::col_character(),
    Value = readr::col_character(),
    Date = readr::col_datetime(format = ""),
    `User ID` = readr::col_character()
  ), progress = F)
  names(huge_csv) <- c(
    "study_id",
    "record_id",
    "form_type",
    "form_instance_id",
    "form_instance_name",
    "field_id",
    "field_value",
    "date",
    "user_id"
  )
  return(list(
    records = huge_csv %>%
      filter(is.na(form_type)) %>%
      select(study_id, record_id, user_id),
    study_data = huge_csv %>%
      filter(form_type == "Study") %>%
      select(-form_instance_id, -form_instance_name),
    reports = huge_csv %>%
      filter(form_type == "Report" & is.na(field_id)) %>%
      select(-field_id, -field_value, -date),
    report_data = huge_csv %>%
      filter(form_type == "Report" & !is.na(field_id)) %>%
      rename(report_instance_id = form_instance_id, report_instance_name = form_instance_name),
    surveys = huge_csv %>%
      filter(form_type == "Survey" & is.na(field_id)) %>%
      select(-field_id, -field_value, -date),
    survey_data = huge_csv %>%
      filter(form_type == "Survey" & !is.na(field_id))%>%
      rename(survey_instance_id = form_instance_id, survey_instance_name = form_instance_name)
  ))
}


