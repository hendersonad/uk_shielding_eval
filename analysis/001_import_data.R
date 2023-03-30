library(tidyverse)
library(magrittr)
library(here)
library(lubridate)
library(arrow)

data <- readr::read_csv(here("output/dataset_all.csv.gz")) %>% 
  janitor::clean_names()
spec(data) %>% print()

data %>%
  mutate(
    # create time variable for follow up (years)
    t =  pt_start_date %--% pt_end_date / dyears(1),
    # convert IMD to quintiles
    imd_q5 = cut(imd,
                 breaks = c(32844 * seq(0, 1, 0.2)),
                 labels = c("1 (most deprived)",
                            "2",
                            "3",
                            "4",
                            "5 (least deprived)")
    ),
    # create an age category variable for easy stratification
    age_cat = cut(
      age, 
      breaks = c(0, 31, 41, 51, 61, 71, Inf),
      labels = c(
        "18-29",
        "30-39",
        "40-49",
        "50-59",
        "60-69",
        "70+"
      )),
    # age centred (for modelling purposes)
    age_centred = age - mean(age, na.rm = TRUE),
  ) %>% 
  # only keep people with recorded sex
  filter(
    sex %in% c("male", "female", "intersex")
  ) %>% 
  mutate(sex = factor(sex, levels = c("male", "female", "intersex"))) %>% 
  # treat region as a factor
  mutate(practice_nuts = factor(practice_nuts)) %>% 
  # convert number of comorbidities to factor (0,1,2+)
  mutate(comorbidities = cut(
    comorbid_count, 
    breaks = c(0,1,2, Inf),
    labels = c("0", "1", "2+"))
  ) %>% 
  write_csv(here::here("output/data_edited.csv"))
