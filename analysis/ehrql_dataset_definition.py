from datetime import timedelta
from ehrql import create_dataset, codelist_from_csv
from ehrql.tables.core import patients, clinical_events

dataset = create_dataset()

# Load snomedct pregnancy codes
pregnancy_snomedct = codelist_from_csv("codelists/nhsd-primary-care-domain-refsets-c19preg_cod.csv", column="code")

# Patient demographics
age = patients.age_on("2020-03-31")
dataset.define_population((age > 12) & (age < 55) & (patients.sex == "female"))
dataset.age = age
dataset.sex = patients.sex
dataset.died = patients.date_of_death.is_not_null()

# Filter for pregnancy events
pregnancy_events = clinical_events.where(
    clinical_events.snomedct_code.is_in(pregnancy_snomedct)
)

# First and last pregnancy dates
first_pregnancy = pregnancy_events.sort_by(pregnancy_events.date).first_for_patient()
last_pregnancy = pregnancy_events.sort_by(pregnancy_events.date).last_for_patient()

dataset.first_pregnancy_date = first_pregnancy.date
dataset.last_pregnancy_date = last_pregnancy.date

# Determine if there is only a single pregnancy event (True or False)
single_event = pregnancy_events.count_for_patient() == 1  # Single pregnancy event check


# Episode-level metadata (mark if there are multiple episodes)
dataset.multiple_episodes = pregnancy_events.count_for_patient() > 1
dataset.has_interpregnancy_gap = dataset.multiple_episodes

# Configure dummy data
dummydata2 = dataset.configure_dummy_data(population_size=1000)