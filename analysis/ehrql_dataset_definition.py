from datetime import timedelta
from ehrql import weeks
from ehrql import create_dataset, codelist_from_csv
from ehrql.tables.core import patients, clinical_events

dataset = create_dataset()
# Patient demographics
age = patients.age_on("2020-03-31")
dataset.age = age
dataset.sex = patients.sex
dataset.define_population((age > 12) & (age < 55) & (patients.sex == "female"))


## 2. Load Subcategorized Codelists
# Antenatal
antenatal_screening_codes = codelist_from_csv("codelists/local/A1_antenatal_screening.csv", column="code")
antenatal_risk_codes = codelist_from_csv("codelists/local/A2_risk_assessment.csv", column="code")
antenatal_procedures_codes = codelist_from_csv("codelists/local/A3_antenatal_procedures.csv", column="code")
# Pregnancy Outcomes
live_birth_codes = codelist_from_csv("codelists/local/B1_live_birth.csv",column="code")
stillbirth_codes = codelist_from_csv("codelists/local/B2_stillbirth.csv", column="code")
neonatal_complication_codes = codelist_from_csv("codelists/local/B3_neonatal_complications.csv", column="code")
# Complications
htn_codes = codelist_from_csv("codelists/local/C1_hypertension.csv", column="code")
diabetes_codes = codelist_from_csv("codelists/local/C2_diabetes.csv", column="code")
infection_codes = codelist_from_csv("codelists/local/C3_infections.csv", column="code")
preeclampsia_codes = codelist_from_csv("codelists/local/C4_preeclampsia.csv", column="code")
other_complication_codes = codelist_from_csv("codelists/local/C5_other.csv", column="code")
# Postnatal
maternal_recovery_codes = codelist_from_csv("codelists/local/D1_maternal_recovery.csv", column="code")
neonatal_care_codes = codelist_from_csv("codelists/local/D2_neonatal_care.csv", column="code")
# Delivery
mode_delivery_codes = codelist_from_csv("codelists/local/E1_mode_of_delivery.csv", column="code")
delivery_complication_codes = codelist_from_csv("codelists/local/E2_delivery_complications.csv", column="code")

# --- Define events ---
antenatal_events = clinical_events.where(clinical_events.snomedct_code.is_in(antenatal_screening_codes))
risk_assessment_events = clinical_events.where(clinical_events.snomedct_code.is_in(antenatal_risk_codes))
procedure_events = clinical_events.where(clinical_events.snomedct_code.is_in(antenatal_procedures_codes))

live_birth_events = clinical_events.where(clinical_events.snomedct_code.is_in(live_birth_codes))
stillbirth_events = clinical_events.where(clinical_events.snomedct_code.is_in(stillbirth_codes))
neonatal_complication_events = clinical_events.where(clinical_events.snomedct_code.is_in(neonatal_complication_codes))

htn_events = clinical_events.where(clinical_events.snomedct_code.is_in(htn_codes))
dm_events = clinical_events.where(clinical_events.snomedct_code.is_in(diabetes_codes))
infection_events = clinical_events.where(clinical_events.snomedct_code.is_in(infection_codes))
pre_eclampsia_events = clinical_events.where(clinical_events.snomedct_code.is_in(preeclampsia_codes))
other_complication_events = clinical_events.where(clinical_events.snomedct_code.is_in(other_complication_codes))

maternal_recovery_events = clinical_events.where(clinical_events.snomedct_code.is_in(maternal_recovery_codes))
neonatal_care_events = clinical_events.where(clinical_events.snomedct_code.is_in(neonatal_care_codes))

mode_delivery_events = clinical_events.where(clinical_events.snomedct_code.is_in(mode_delivery_codes))
delivery_complication_events = clinical_events.where(clinical_events.snomedct_code.is_in(delivery_complication_codes))

# --- Patient-level derived values ---
pregnancy_dataset = {
    # Antenatal
    "first_antenatal_date": antenatal_events.date.minimum_for_patient(),
    "last_antenatal_date": antenatal_events.date.maximum_for_patient(),
    "antenatal_event_count": antenatal_events.count_for_patient(),

    "risk_assessment_count": risk_assessment_events.count_for_patient(),
    "procedure_count": procedure_events.count_for_patient(),

    # Pregnancy Outcomes
    "has_live_birth": live_birth_events.exists_for_patient(),
    "has_stillbirth": stillbirth_events.exists_for_patient(),
    "has_neonatal_complications": neonatal_complication_events.exists_for_patient(),

    # Pregnancy Complications
    "has_hypertension": htn_events.exists_for_patient(),
    "has_diabetes": dm_events.exists_for_patient(),
    "has_infections": infection_events.exists_for_patient(),
    "has_pre_eclampsia": pre_eclampsia_events.exists_for_patient(),
    "has_other_complications": other_complication_events.exists_for_patient(),

    # Postnatal
    "has_maternal_recovery_codes": maternal_recovery_events.exists_for_patient(),
    "has_neonatal_care": neonatal_care_events.exists_for_patient(),

    # Delivery Features
    "had_mode_of_delivery_recorded": mode_delivery_events.exists_for_patient(),
    "had_delivery_complications": delivery_complication_events.exists_for_patient(),
}

# Add the rest of your variables as before
for varname, expr in pregnancy_dataset.items():
    setattr(dataset, varname, expr)

# --- Patient-level derived values ---

# Earliest antenatal event date
earliest_antenatal_date = antenatal_events.date.minimum_for_patient()

# Date of pregnancy outcome (live birth or stillbirth, whichever comes first)
first_live_birth_date = live_birth_events.date.minimum_for_patient()
first_stillbirth_date = stillbirth_events.date.minimum_for_patient()

# Choose the earliest pregnancy outcome date (if both exist)
from ehrql import minimum_of
pregnancy_outcome_date = minimum_of(first_live_birth_date, first_stillbirth_date)

# Time (in days) between earliest antenatal event and pregnancy outcome
time_antenatal_to_outcome = (pregnancy_outcome_date - earliest_antenatal_date).days

# Assign to dataset
dataset.earliest_antenatal_date = earliest_antenatal_date
dataset.pregnancy_outcome_date = pregnancy_outcome_date
dataset.time_antenatal_to_outcome = time_antenatal_to_outcome



dataset.configure_dummy_data(population_size=100)
