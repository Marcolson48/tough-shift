library(tidyverse)
library(here)
library(lubridate)
library(arrow)


# TRAFFIC STOP CLEAN ------------------------------------------

# read in
df <- read.csv(here("data", "stops", "1.1.2018_2.27.2025_traffic_citations.csv"))

df <- df %>% 
  janitor::clean_names()

# clean dow and month formatting
df <- df %>%
  mutate(day_of_week = case_when(
    day_of_week == "1-SUNDAY" ~ "sun",
    day_of_week == "2-MONDAY" ~ "mon",
    day_of_week == "3-TUESDAY" ~ "tue",
    day_of_week == "4-WEDNESDAY" ~ "wed",
    day_of_week == "5-THURSDAY" ~ "thu",
    day_of_week == "6-FRIDAY" ~ "fri",
    day_of_week == "7-SATURDAY" ~ "sat",
    TRUE ~ day_of_week
  ))

df <- df %>%
  mutate(month = case_when(
    month == "01-JANUARY" ~ "jan",
    month == "02-FEBRUARY" ~ "feb",
    month == "03-MARCH" ~ "mar",
    month == "04-APRIL" ~ "apr",
    month == "05-MAY" ~ "may",
    month == "06-JUNE" ~ "jun",
    month == "07-JULY" ~ "jul",
    month == "08-AUGUST" ~ "aug",
    month == "09-SEPTEMBER" ~ "sep",
    month == "10-OCTOBER" ~ "oct",
    month == "11-NOVEMBER" ~ "nov",
    month == "12-DECEMBER" ~ "dec",
    TRUE ~ month
  ))

# rename and condense columns
df <- df %>% 
  select(., ticket, tick_date, day_of_week, month, year, hour_of_day,
         subj_sex, simple_subj_re_grp, subj_age, issuing_officer,
         issuing_officer_sex, simple_empl_re_grp, hundredblockaddr,
         precinct_num, precinct, beat_num, mapgrid, council_district_num,
         civil_traffic_violations, criminal_traffic_violations) %>% 
  rename(
    date = tick_date,
    dow = day_of_week,
    hour = hour_of_day,
    subj_race = simple_subj_re_grp,
    officer = issuing_officer,
    officer_sex = issuing_officer_sex,
    officer_race = simple_empl_re_grp,
    hund_block = hundredblockaddr,
    beat = beat_num,
    district = council_district_num,
    civil_vio = civil_traffic_violations,
    crim_vio = criminal_traffic_violations
  )


# format conversions
df <- df %>% 
  mutate(across(c(dow, month, year, hour, subj_sex,
                  subj_race, officer, officer_sex, 
                  officer_race, precinct_num, beat,
                  district), as.factor))

# date to actual date format
df <- df %>% 
  mutate(date = as.Date(date, format = "%m/%d/%Y"))

# save out cleaned stops
write_parquet(df, here("data", "processed", "cleaned_stops.parquet"))


# CALLS FOR SERVICE CLEAN -------------------------------------------------

# read in path
folder_path <- here("data", "cfs")

csv_files <- list.files(folder_path, pattern = "\\.csv$", full.names = TRUE)

# Read and combine all CSVs into one dataframe
df <- csv_files %>%
  map_df(~ read_csv(.x, col_types = cols(.default = "c")))

df <- df %>% 
  janitor::clean_names()

# remove all calls with no action required
df <- df %>% 
  filter(disposition != "NO ACTION REQUIRED")

# drop unnecessary rows
df <- df %>% 
  select(., final_call_type, call_received)

# clean the call_received var, make date, time, hour vars
df <- df %>%
  mutate(call_received = str_trim(call_received),
    dt = parse_date_time(call_received,
                         orders = c("ymd_HMS", "mdy_IMSp")),
    date = format(dt, "%Y/%m/%d"),
    time = format(dt, "%H:%M"),
    hour = if_else(hour(dt) == 0, 24L, hour(dt)))

# condense into hourly totals
shrunk <- df %>% 
  group_by(date, hour) %>%
  summarize(total = n(), .groups = "drop")

# save out
write_parquet(shrunk, here("data", "processed", "cleaned_cfs.parquet"))


# MERGE STOPS AND CFS DATA -------------------------------------------------------------------

stops <- read_parquet(here("data", "processed", "cleaned_stops.parquet"))

stops$hour <- as.numeric(stops$hour)

cfs <- read_parquet(here("data", "processed", "cleaned_cfs.parquet"))

cfs$date <- as.Date(cfs$date)

joined <- stops %>%
  left_join(cfs, by = c("date", "hour"))

write_parquet(joined, here("data", "processed", "stops_cfs.parquet"))
