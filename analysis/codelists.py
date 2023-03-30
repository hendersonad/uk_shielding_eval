from databuilder.ehrql import codelist_from_csv

# covid identification
hosp_covid = codelist_from_csv(
    "codelists/opensafely-covid-identification.csv",
    column="icd10_code",
)
covid_primary_care_positive_test = codelist_from_csv(
    "codelists/opensafely-covid-identification-in-primary-care-probable-covid-positive-test.csv",
    column="CTV3ID",
)
covid_primary_care_code = codelist_from_csv(
    "codelists/opensafely-covid-identification-in-primary-care-probable-covid-clinical-code.csv",
    column="CTV3ID",
)
covid_primary_care_sequalae = codelist_from_csv(
    "codelists/opensafely-covid-identification-in-primary-care-probable-covid-sequelae.csv",
    column="CTV3ID",
)
any_primary_care_code = (
    covid_primary_care_code +
    covid_primary_care_positive_test +
    covid_primary_care_sequalae
)
dementia_codes = codelist_from_csv(
    "codelists/opensafely-dementia.csv",
    column="CTV3ID"
)
other_neuro_codes = codelist_from_csv(
    "codelists/opensafely-other-neurological-conditions.csv",
    column="CTV3ID",
)
chronic_respiratory_disease_codes = codelist_from_csv(
    "codelists/opensafely-chronic-respiratory-disease.csv",
    column="CTV3ID",
)
asthma_codes = codelist_from_csv(
    "codelists/opensafely-asthma-diagnosis.csv", 
    column="CTV3ID"
)
salbutamol_codes = codelist_from_csv(
    "codelists/opensafely-asthma-inhaler-salbutamol-medication.csv",
    column="id",
)
ics_codes = codelist_from_csv(
    "codelists/opensafely-asthma-inhaler-steroid-medication.csv",
    column="id",
)
prednisolone_codes = codelist_from_csv(
    "codelists/opensafely-asthma-oral-prednisolone-medication.csv",
    column="snomed_id",
)
clear_smoking_codes = codelist_from_csv(
    "codelists/opensafely-smoking-clear.csv",
    column="CTV3Code",
    category_column="Category",
)
stroke_gp_codes = codelist_from_csv(
    "codelists/opensafely-stroke-updated.csv",
    column="CTV3ID"
)
lung_cancer_codes = codelist_from_csv(
    "codelists/opensafely-lung-cancer.csv",
    column="CTV3ID"
)
haem_cancer_codes = codelist_from_csv(
    "codelists/opensafely-haematological-cancer.csv",
    column="CTV3ID"
)
other_cancer_codes = codelist_from_csv(
    "codelists/opensafely-cancer-excluding-lung-and-haematological.csv",
    column="CTV3ID",
)
chronic_cardiac_disease_codes = codelist_from_csv(
    "codelists/opensafely-chronic-cardiac-disease.csv",
    column="CTV3ID"
)
hiv_codes = codelist_from_csv(
    "codelists/opensafely-hiv.csv",
    column="CTV3ID",
    category_column="CTV3ID",
)
permanent_immune_codes = codelist_from_csv(
    "codelists/opensafely-permanent-immunosuppression.csv",
    column="CTV3ID",
)
temp_immune_codes = codelist_from_csv(
    "codelists/opensafely-temporary-immunosuppression.csv",
    column="CTV3ID",
)
aplastic_codes = codelist_from_csv(
    "codelists/opensafely-aplastic-anaemia.csv",
    column="CTV3ID"
)
spleen_codes = codelist_from_csv(
    "codelists/opensafely-asplenia.csv",
    column="CTV3ID"
)
organ_transplant_codes = codelist_from_csv(
    "codelists/opensafely-solid-organ-transplantation.csv",
    column="CTV3ID",
)
sickle_cell_codes = codelist_from_csv(
    "codelists/opensafely-sickle-cell-disease.csv",
    column="CTV3ID"
)
ra_sle_psoriasis_codes = codelist_from_csv(
    "codelists/opensafely-ra-sle-psoriasis.csv",
    column="CTV3ID"
)
chronic_liver_disease_codes = codelist_from_csv(
    "codelists/opensafely-chronic-liver-disease.csv",
    column="CTV3ID"
)
diabetes_codes = codelist_from_csv(
    "codelists/opensafely-diabetes.csv",
    column="CTV3ID"
)
psychosis_schizophrenia_bipolar_codes = codelist_from_csv(
    "codelists/opensafely-psychosis-schizophrenia-bipolar-affective-disease.csv",
    column="CTV3Code",
)
depression_codes = codelist_from_csv(
    "codelists/opensafely-depression.csv",
    column="CTV3Code"
)
comorbidities_codelist = (
    diabetes_codes +
    haem_cancer_codes +
    lung_cancer_codes +
    other_cancer_codes +
    asthma_codes +
    chronic_cardiac_disease_codes +
    chronic_liver_disease_codes +
    chronic_respiratory_disease_codes +
    other_neuro_codes +
    stroke_gp_codes +
    dementia_codes +
    ra_sle_psoriasis_codes +
    psychosis_schizophrenia_bipolar_codes +
    permanent_immune_codes +
    temp_immune_codes
)

hosp_fractures = codelist_from_csv(
    "codelists/opensafely-fracture-potential-emergency-opcs-codes.csv",
    column="Code"
)

high_risk_shield = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-shield.csv",
    column="code"
)

low_risk_shield = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-nonshield.csv",
    column="code"
)

care_home_flag = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-carehome_cod.csv",
    column="code"
)
