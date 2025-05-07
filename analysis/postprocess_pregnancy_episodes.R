library(dplyr)
library(lubridate)
library(data.table)

# Example: df has columns patient_id, event_date (as.Date), event_type, etc.
setDT(df)
setorder(df, patient_id, event_date)

# Define a max gap (e.g., 42 weeks = 294 days) to separate episodes
max_gap <- 294

df[, episode := cumsum(
  c(TRUE, diff(event_date) > max_gap | diff(patient_id) != 0)
), by = patient_id]

df <- df %>%
  group_by(patient_id, episode) %>%
  mutate(delivery_date = max(event_date[event_type == "delivery"], na.rm = TRUE),
         gestational_age_weeks = as.numeric(difftime(delivery_date, event_date, units = "days")) / 7)

df <- df %>%
  mutate(window = case_when(
    event_date < delivery_date ~ "antenatal",
    event_date == delivery_date ~ "delivery",
    event_date > delivery_date ~ "postnatal"
  ))

# Classify preterm, term, postterm
df <- df %>%
  group_by(patient_id, episode) %>%
  mutate(gest_age_at_delivery = gestational_age_weeks[event_type == "delivery"][1],
         delivery_class = case_when(
           gest_age_at_delivery < 37 ~ "preterm",
           gest_age_at_delivery >= 37 & gest_age_at_delivery < 42 ~ "term",
           gest_age_at_delivery >= 42 ~ "postterm"
         ),
         c_section = ifelse(mode_of_delivery == "C-section", TRUE, FALSE))          