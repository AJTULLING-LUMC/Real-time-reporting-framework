castorApiToken <- castorOAuth(oauth_key(), oauth_secret(), base_url) 
fields <- getEndpoint(paste("api", "study", study_id, "field?include=optiongroup", sep = "/"))
records <- getEndpoint(paste("api", "study", study_id, "record", sep = "/"))
steps <- getEndpoint(paste("api", "study", study_id, "step", sep = "/")) 
records <- unnest_all_wider(records, recursive = T) 
reports <- getEndpoint(paste("api", "study", study_id, "report-instance", sep = "/"))
reportdata <- getEndpoint(paste("api", "study", study_id, "data-point-collection", "report-instance", sep = "/"))
rapporten <- reports %>% fetchReportsv2()
surveyPackageInstances <- getEndpoint(paste("api", "study", study_id, "surveypackageinstance", sep = "/"))
alldata <- fetch_full_export_csv_text()
allStudyData <- alldata$study_data

alldataextended <- allStudyData %>%
inner_join(fields, by = c("field_id" = "id")) %>%
inner_join(steps, by = c("parent_id" = "step_id")) %>%
mutate(field_value = if_else(field_value == "Missing (not applicable)", NA_character_, field_value))

rm(allStudyData)

participant_data <- alldataextended %>%
  arrange(step_order, field_number) %>%
  select(record_id, field_variable_name, field_value) %>%
  pivot_wider(names_from = field_variable_name, values_from = field_value) %>%
  labeldf()