library(dplyr)
library(lubridate)

# Load your data
df <- read.csv("/workspaces/my_cohort_repo/dataset.csv") # or your actual file name

# Convert date columns to Date type
df <- df %>%
  mutate(
    first_antenatal_date = as.Date(first_antenatal_date),
    last_antenatal_date = as.Date(last_antenatal_date),
    pregnancy_outcome_date = as.Date(pregnancy_outcome_date)
  )

# 1. Each row is a pregnancy episode (patient-level)
df <- df %>%
  mutate(episode_id = row_number()) # Optional: unique episode ID

# 2. Calculate gestational age at outcome (in weeks)
df <- df %>%
  mutate(
    gestational_age_weeks = as.numeric(difftime(pregnancy_outcome_date, first_antenatal_date, units = "days")) / 7
  )

# 3. Apply temporal windows (example: define antenatal and postnatal windows)
df <- df %>%
  mutate(
    antenatal_window = interval(first_antenatal_date, pregnancy_outcome_date),
    postnatal_window = interval(pregnancy_outcome_date + 1, pregnancy_outcome_date + weeks(6))
  )

# 4. Classify deliveries
df <- df %>%
  mutate(
    delivery_type = case_when(
      has_live_birth == TRUE & gestational_age_weeks < 37 ~ "Preterm",
      has_live_birth == TRUE & gestational_age_weeks >= 37 & gestational_age_weeks < 42 ~ "Term",
      has_live_birth == TRUE & gestational_age_weeks >= 42 ~ "Postterm",
      has_stillbirth == TRUE ~ "Stillbirth",
      TRUE ~ "Other"
    ),
    c_section = ifelse(had_mode_of_delivery_recorded == TRUE, "C-section", "Other")
  )

# Write the processed data
write.csv(df, "output/pregnancy_episodes_processed.csv", row.names = FALSE)