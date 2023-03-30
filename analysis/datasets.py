# where EHRQl is defined. Only really need Dastaset, tehe others are specific
from databuilder.ehrql import days, case, when

# this is where we import the schema to run the study with
from databuilder.tables.beta.tpp import (
  patients,
  clinical_events,
  sgss_covid_all_tests,
  hospital_admissions
)
import datetime

from variable_lib import (
  age_as_of,
  has_died,
  address_as_of,
  create_sequential_variables,
  hospitalisation_diagnosis_matches
)
import codelists

study_start_date = datetime.date(2020, 3, 1)
study_end_date = datetime.date(2021, 3, 1)
minimum_registration = 90


def add_common_variables(dataset, study_start_date, study_end_date):
    dataset.pt_start_date = case(
        when(dataset.start_date + days(minimum_registration) > study_start_date).then(dataset.start_date + days(minimum_registration)),
        default=study_start_date,
    )

    dataset.pt_end_date = case(
        when(dataset.end_date.is_null()).then(study_end_date),
        when(dataset.end_date > study_end_date).then(study_end_date),
        default=dataset.end_date,
    )

    # Demographic variables
    dataset.sex = patients.sex
    dataset.age = age_as_of(study_start_date)
    dataset.has_died = has_died(study_start_date)
    dataset.msoa = address_as_of(study_start_date).msoa_code
    dataset.imd = address_as_of(study_start_date).imd_rounded
    dataset.death_date = patients.date_of_death

    # covid tests
    dataset.first_test_positive = sgss_covid_all_tests \
        .where(sgss_covid_all_tests.is_positive) \
        .except_where(sgss_covid_all_tests.specimen_taken_date >= dataset.pt_end_date) \
        .sort_by(sgss_covid_all_tests.specimen_taken_date).first_for_patient().specimen_taken_date

    all_test_positive = sgss_covid_all_tests \
        .where(sgss_covid_all_tests.is_positive) \
        .except_where(sgss_covid_all_tests.specimen_taken_date <= study_start_date) \
        .except_where(sgss_covid_all_tests.specimen_taken_date >= dataset.pt_end_date)

    dataset.all_test_positive = all_test_positive.count_for_patient()

    # get the date of each of up to 5 test positives
    create_sequential_variables(
      dataset,
      "covid_testdate_{n}",
      num_variables=5,
      events=all_test_positive,
      column="specimen_taken_date"
    )

    # covid hospitalisation
    covid_hospitalisations = hospitalisation_diagnosis_matches(hospital_admissions, codelists.hosp_covid)

    dataset.first_covid_hosp = covid_hospitalisations \
        .sort_by(covid_hospitalisations.admission_date) \
        .first_for_patient().admission_date

    dataset.all_covid_hosp = covid_hospitalisations \
        .except_where(covid_hospitalisations.admission_date >= dataset.pt_end_date) \
        .count_for_patient()

    # Any covid identification
    primarycare_covid = clinical_events \
        .where(clinical_events.ctv3_code.is_in(codelists.any_primary_care_code)) \
        .except_where(clinical_events.date >= dataset.pt_end_date)

    dataset.latest_primarycare_covid = primarycare_covid \
        .sort_by(clinical_events.date) \
        .last_for_patient().date

    dataset.total_primarycare_covid = primarycare_covid \
        .count_for_patient()

    # comorbidities
    comorbidities = clinical_events \
        .where(clinical_events.date <= dataset.pt_start_date - days(1)) \
        .where(clinical_events.ctv3_code.is_in(codelists.comorbidities_codelist))

    dataset.comorbid_count = comorbidities.count_for_patient()

    # negative control - hospital fractures
    fracture_hospitalisations = hospitalisation_diagnosis_matches(hospital_admissions, codelists.hosp_fractures)

    dataset.first_fracture_hosp = fracture_hospitalisations \
        .where(fracture_hospitalisations.admission_date.is_between(study_start_date, study_end_date)) \
        .sort_by(fracture_hospitalisations.admission_date) \
        .first_for_patient().admission_date

    # shielding codes
    dataset.highrisk_shield = clinical_events \
        .where(clinical_events.snomedct_code.is_in(codelists.high_risk_shield)) \
        .sort_by(clinical_events.date) \
        .first_for_patient().date

    dataset.lowrisk_shield = clinical_events \
        .where(clinical_events.snomedct_code.is_in(codelists.low_risk_shield)) \
        .sort_by(clinical_events.date) \
        .first_for_patient().date

    # care home flag
    dataset.care_home = clinical_events \
        .where(clinical_events.date <= dataset.pt_start_date - days(1)) \
        .where(clinical_events.snomedct_code.is_in(codelists.care_home_flag)) \
        .sort_by(clinical_events.date) \
        .first_for_patient().date

    # final age restriction
    age_restrict = (dataset.age > 0) & (dataset.age < 110)
    dataset.define_population(age_restrict)
