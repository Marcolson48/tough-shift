library(dplyr)
library(lubridate)
library(tidyr)
library(arrow)
library(fixest)
library(stringr)
library(purrr)
library(tibble)
library(future.apply)




set.seed(15)
### workload settings for this specific laptop
# Sys.setenv(OMP_NUM_THREADS = "1", MKL_NUM_THREADS = "1", OPENBLAS_NUM_THREADS = "1")
# fixest::setFixest_nthreads(1)

# INITIAL SHIFT ASSIGNMENT PROCEDURE -------------------------
stops <- read_parquet(here::here("data", "processed", "stops_cfs.parquet"))

# switch hour == 24 to hour 0 on the following day for ease of interpretation
stops <- stops %>%
  mutate(
    date = as_date(date),
    date = if_else(hour == 24, date + days(1), date),
    hour = if_else(hour == 24, 0L, as.integer(hour))
  )

# create flags for stops that confidently determine shift
flags <- stops %>%
  group_by(officer, date) %>%
  summarise(
    any_7_12   = any(hour >= 7  & hour <= 12),
    any_17_19  = any(hour >= 16 & hour <= 19),
    any_1_4    = any(hour >= 1  & hour <= 4),
    any_5_6    = any(hour %in% c(5L,6L)),
    any_13_16  = any(hour >= 13 & hour <= 16),
    any_20_23  = any(hour >= 20 & hour <= 23),
    any_0      = any(hour == 0L),
    any_5      = any(hour == 5L),
    any_13     = any(hour == 13L),
    any_20     = any(hour == 20L),
    .groups = "drop_last"
  ) %>%
  arrange(officer, date) %>%
  mutate(
    any_5_6_tomorrow    = lead(any_5_6, default = FALSE),
    any_20_23_yesterday = lag(any_20_23, default = FALSE),
    any_0_tomorrow      = lead(any_0,   default = FALSE)
  ) %>%
  ungroup()

stops <- stops %>% left_join(flags, by = c("officer","date"))

# assign shift based on flags
stops <- stops %>%
  mutate(shift = NA_integer_) %>%
  mutate(shift = if_else(any_7_12, 1L, shift)) %>%
  mutate(shift = if_else(is.na(shift) & any_17_19, 2L, shift)) %>%
  mutate(shift = if_else(is.na(shift) & any_1_4,   3L, shift)) %>%
  mutate(
    shift = case_when(
      is.na(shift) & any_5_6 & any_13_16 ~ 1L,
      is.na(shift) & any_13_16 & (any_20_23 | any_0_tomorrow) ~ 2L,
      is.na(shift) & (any_20_23 | any_0_tomorrow) & any_5_6_tomorrow ~ 3L,
      is.na(shift) & (any_20_23_yesterday | any_0) & any_5_6 ~ 3L,
      TRUE ~ shift
    )
  )

# Keep only columns we actually need downstream
df_base <- stops %>%
  select(officer, district, dow, month, year, date, hour, shift,
         any_5, any_13, any_20, any_5_6, any_13_16, any_20_23, any_0,
         any_5_6_tomorrow, any_20_23_yesterday, any_0_tomorrow,
         crim_vio, civil_vio, subj_sex, subj_race, subj_age)  # keep if you might reuse later
rm(stops, flags); invisible(gc())

# calls for service data read in
# switch hour == 24 to hour 0 on the following day for ease of interpretation
calls <- read_parquet(here::here("data", "processed", "cleaned_cfs.parquet")) %>%
  mutate(date = as_date(date),
         date = if_else(hour == 24, date + days(1), date),
         hour = if_else(hour == 24, 0L, as.integer(hour)))




########## do a single iteration and save out for descriptive data
df_single <- df_base %>% 
  mutate(
    .rand = runif(n()),   # a random [0,1) for each row
    shift = case_when(
      # keep any already‐assigned shifts
      !is.na(shift) ~ shift,
      
      # only any_6_7: between shift 1 & 3
      is.na(shift) & any_5_6 & !any_13_16 & !any_20_23 & !any_0 ~ if_else(.rand < 0.5, 1L, 3L),
      
      # only any_14_15: between shift 1 & 2
      is.na(shift) & any_13_16 & !any_5_6 & !any_20_23 & !any_0 ~ if_else(.rand < 0.5, 1L, 2L),
      
      # only any_22_23: between shift 2 & 3
      is.na(shift) & (any_20_23 | any_0_tomorrow) & !any_5_6 & !any_13_16 ~ if_else(.rand < 0.5, 2L, 3L),
      is.na(shift) & (any_20_23_yesterday | any_0) & !any_5_6 & !any_13_16 ~ if_else(.rand < 0.5, 2L, 3L),
      
      TRUE ~ NA_integer_
    )
  ) %>%
  select(-.rand)

# filter out any that were not assigned a shift
# we can infer that these were special assignment 
df_single <- df_single %>% 
  filter(!is.na(shift))

# create early/late start to assign early to those w stops in the correct time frame
df_single <- df_single %>% 
  mutate(early_late = case_when(
    shift == 1 & any_5 ~ "early",
    shift == 2 & any_13 ~ "early",
    shift == 3 & any_20 ~ "early",
    TRUE ~ NA
  )) %>% 
  select(., -starts_with("any"))

# create shift start times based on shift and early/late start
df_single <- df_single %>%
  mutate(
    # this randomly assigns early/late to NAs
    early_late = ifelse(is.na(early_late), ifelse(runif(n()) < 0.5, "early", "late"), early_late),
    shift_start = case_when(
      shift == 1 & early_late == "early" ~ ymd_hms(paste(date, "05:00:00")),
      shift == 1 & early_late == "late" ~ ymd_hms(paste(date, "06:00:00")),
      
      shift == 2 & hour < 11 & early_late == "early" ~ ymd_hms(paste(date - days(1), "13:00:00")),
      shift == 2 & hour < 11 & early_late == "late" ~ ymd_hms(paste(date - days(1), "14:00:00")),
      
      shift == 2 & hour > 11 & early_late == "early" ~ ymd_hms(paste(date, "13:00:00")),
      shift == 2 & hour > 11 & early_late == "late" ~ ymd_hms(paste(date, "14:00:00")),
      
      shift == 3 & hour < 9 & early_late == "early" ~ ymd_hms(paste(date - days(1), "20:00:00")),
      shift == 3 & hour < 9 & early_late == "late" ~ ymd_hms(paste(date - days(1), "21:00:00")),
      
      shift == 3 & hour > 17 & early_late == "early" ~ ymd_hms(paste(date, "20:00:00")),
      shift == 3 & hour > 17 & early_late == "late" ~ ymd_hms(paste(date, "21:00:00")),
      
      TRUE ~ NA_POSIXct_))

# create hour into shift based on start time
df_single <- df_single %>%
  mutate(
    hour_into_shift = case_when(
      shift == 1 & early_late == "early" ~ hour - 4,
      shift == 2 & early_late == "early" & hour > 12 ~ hour - 12,
      shift == 2 & early_late == "early" & hour < 12 ~ hour + 12,
      shift == 3 & early_late == "early" & hour > 17 ~ hour - 19,
      shift == 3 & early_late == "early" & hour < 10 ~ hour + 5,
      shift == 1 & early_late == "late" ~ hour - 5,
      shift == 2 & early_late == "late" & hour > 12 ~ hour - 13,
      shift == 2 & early_late == "late" & hour < 12 ~ hour + 11,
      shift == 3 & early_late == "late" & hour > 17 ~ hour - 20,
      shift == 3 & early_late == "late" & hour < 10 ~ hour + 4
    ))

# filter to shift hours of interest (no overtime), and format shift date
df_single <- df_single %>% 
  filter(hour_into_shift > 0 & hour_into_shift < 11) %>% 
  mutate(shift_date = as.Date(shift_start))

# summarize data by shift hour
agg <- df_single %>%
  group_by(officer, shift_date, shift_start, shift, district, date, dow, month, year, hour_into_shift, hour) %>%
  summarise(
    num_stops      = n(),
    total_crim_vio = sum(crim_vio, na.rm = TRUE),
    total_civil_vio = sum(civil_vio, na.rm = TRUE),
    prop_male      = mean(subj_sex == "Male", na.rm = TRUE),
    prop_white     = mean(subj_race == "White", na.rm = TRUE),
    mean_age       = mean(subj_age, na.rm = TRUE)
  ) %>%
  ungroup()

# identify unique officer-shifts in the data
shifts <- df_single %>%
  distinct(officer, shift_date, shift_start, shift, district, date, dow, month, year)

# expand each shift to the full 10 hours
complete_shifts <- shifts %>%
  crossing(tibble(hour_into_shift = 1:10)) %>%
  # compute the actual clock‐hour for each hour_into_shift:
  mutate(
    hour = shift_start + hours(hour_into_shift - 1),
    date = as_date(hour),
    hour = hour(hour)           
  )


# join the summarized stop data with the complete grid
key_cols <- c(
  "officer", "shift_date", "shift_start", "shift", "district",
  "date", "dow", "month", "year", "hour_into_shift", "hour"
)

final_summary <- full_join(complete_shifts, agg, by = key_cols) %>%
  # for any of the agg‐columns that are NA, replace with 0
  mutate(num_stops = ifelse(is.na(num_stops), 0, num_stops)) %>%
  arrange(officer, shift_date, hour_into_shift)

### merge in cfs
final_data <- final_summary %>%
  left_join(calls, by = c("date", "hour"))

# export this for descripitve stats later
saveRDS(final_data, here::here("data", "processed", "descriptives.rds"))

# clean environment
rm(final_data, final_summary, complete_shifts, shifts, agg, df_single)
gc()


#### gof stats pull function
pull_gof <- function(mod, model_name, iter) {
  if (is.null(mod)) {
    return(tibble::tibble(model = character(),
                          param = character(),
                          estimate = numeric(),
                          se = numeric(),
                          iter = integer()))
  }
  
  # N
  N <- tryCatch(stats::nobs(mod), error = function(e) NA_real_)
  
  # Adjusted R2 and Within R2 via fixest::r2()
  AdjR2 <- tryCatch(fixest::r2(mod, type = "ar2"),  error = function(e) NA_real_)
  Wr2   <- tryCatch(fixest::r2(mod, type = "wr2"),  error = function(e) NA_real_)
  
  tibble::tibble(
    model    = model_name,
    param    = c("N", "Adj.R2", "Within.R2"),
    estimate = c(N, AdjR2, Wr2),
    se       = NA_real_,
    iter     = iter
  )
}
# Function 1 for iterative analysis
# this function includes models based on equations 1, 2, and 4
# randomize shift -> build panel dataset-> analysis -> extract relevant coefficients 
run_once <- function(b, base_seed, df_base, calls) {
  set.seed(base_seed + b)
  
  # --- RANDOMIZE ambiguous shift and start times -------------------------------
  df <- df_base %>%
    dplyr::mutate(
      early_late = dplyr::case_when(
        shift == 1 & any_5  ~ "early",
        shift == 2 & any_13 ~ "early",
        shift == 3 & any_20 ~ "early",
        TRUE ~ NA_character_
      ),
      .rand  = runif(dplyr::n()),
      .rand2 = runif(dplyr::n()),
      shift = dplyr::case_when(
        !is.na(shift) ~ shift,
        is.na(shift) & any_5_6 & !any_13_16 & !any_20_23 & !any_0 ~ ifelse(.rand < 0.5, 1L, 3L),
        is.na(shift) & any_13_16 & !any_5_6 & !any_20_23 & !any_0 ~ ifelse(.rand < 0.5, 1L, 2L),
        is.na(shift) & (any_20_23 | any_0_tomorrow) & !any_5_6 & !any_13_16 ~ ifelse(.rand < 0.5, 2L, 3L),
        is.na(shift) & (any_20_23_yesterday | any_0) & !any_5_6 & !any_13_16 ~ ifelse(.rand < 0.5, 2L, 3L),
        TRUE ~ NA_integer_
      ),
      early_late = ifelse(is.na(early_late), ifelse(.rand2 < 0.5, "early", "late"), early_late)
    ) %>%
    dplyr::select(-.rand, -.rand2) %>%
    dplyr::filter(!is.na(shift))
  
  # --- create shift_start + hour_into_shift
  stop_rand <- df %>%
    dplyr::mutate(
      base_dt = lubridate::as_datetime(date),
      
      start_hour = dplyr::case_when(
        shift == 1 & early_late == "early" ~ 5L,
        shift == 1 & early_late == "late"  ~ 6L,
        
        shift == 2 & hour < 12 & early_late == "early" ~ -11L,
        shift == 2 & hour < 12 & early_late == "late"  ~ -10L,
        shift == 2 & hour >= 12 & early_late == "early" ~ 13L,
        shift == 2 & hour >= 12 & early_late == "late"  ~ 14L,
        
        shift == 3 & hour < 10 & early_late == "early" ~ -4L,
        shift == 3 & hour < 10 & early_late == "late"  ~ -3L,
        shift == 3 & hour >= 18 & early_late == "early" ~ 20L,
        shift == 3 & hour >= 18 & early_late == "late"  ~ 21L,
        TRUE ~ NA_integer_
      ),
      
      shift_start = base_dt + lubridate::hours(start_hour),
      
      hour_into_shift = dplyr::case_when(
        shift == 1 & early_late == "early" ~ hour - 4L,
        shift == 1 & early_late == "late"  ~ hour - 5L,
        
        shift == 2 & early_late == "early" & hour >= 12L ~ hour - 12L,
        shift == 2 & early_late == "early" & hour <  12L ~ hour + 12L,
        shift == 2 & early_late == "late"  & hour >= 12L ~ hour - 13L,
        shift == 2 & early_late == "late"  & hour <  12L ~ hour + 11L,
        
        shift == 3 & early_late == "early" & hour >= 18L ~ hour - 19L,
        shift == 3 & early_late == "early" & hour <  10L ~ hour + 5L,
        shift == 3 & early_late == "late"  & hour >= 18L ~ hour - 20L,
        shift == 3 & early_late == "late"  & hour <  10L ~ hour + 4L,
        TRUE ~ NA_real_
      )
    ) %>% # filter data down to hours within shift
    dplyr::filter(hour_into_shift > 0 & hour_into_shift < 11) %>%
    dplyr::mutate(shift_date = as.Date(shift_start)) %>%
    dplyr::select(-base_dt, -start_hour)
  
  # --- BUILD panel dataset with n stops per hour and n violations per shift -------------------------------------
    # identify each unique officer-shift in the data
  shifts <- stop_rand %>%
    dplyr::distinct(officer, shift_date, shift_start, shift, district, dow, month, year)
    # create 10 rows per officer-shift, 1 for each hour
    complete_shifts <- tidyr::crossing(shifts, tibble::tibble(hour_into_shift = 1:10)) %>%
    dplyr::mutate(
      hour_ts = shift_start + lubridate::hours(hour_into_shift - 1L),
      date    = as.Date(hour_ts),
      hour    = lubridate::hour(hour_ts)
    ) %>%
    dplyr::select(-hour_ts)
    # compute stops per officer hour and violations per stop per officer hour values
  agg <- stop_rand %>%
    dplyr::group_by(officer, shift_date, shift_start, shift, district, dow, month, year, hour_into_shift, hour) %>%
    dplyr::summarise(
      num_stops       = dplyr::n(),
      total_civil_vio = sum(civil_vio, na.rm = TRUE),
      total_crim_vio  = sum(crim_vio,  na.rm = TRUE),
      .groups = "drop"
    )
    # merge the panel format and computed variables
  df_full <- complete_shifts %>%
    dplyr::left_join(agg, by = c("officer","shift_date","shift_start","shift","district",
                                 "dow","month","year","hour_into_shift","hour")) %>%
    dplyr::mutate(
      num_stops       = ifelse(is.na(num_stops), 0L, num_stops),
      total_civil_vio = ifelse(is.na(total_civil_vio), 0, total_civil_vio),
      total_crim_vio  = ifelse(is.na(total_crim_vio),  0, total_crim_vio)
    )
    # merge in CFS data by date-hour
  df <- df_full %>%
    dplyr::left_join(calls, by = c("date","hour")) %>%
    dplyr::mutate(
      total          = tidyr::replace_na(total, 0),
      civ_per_stop   = total_civil_vio / ifelse(num_stops == 0, NA_real_, num_stops),
      crim_per_stop  = total_crim_vio  / ifelse(num_stops == 0, NA_real_, num_stops))
    # create fixed effects for modeling
  df <- df %>%
    dplyr::mutate(
      fe_dwh = interaction(district, dow, hour, drop = TRUE),
      fe_dm  = interaction(district, year, month, drop = TRUE),
      fe_dws = interaction(district, shift, dow, drop = TRUE),
      fe_dw  = interaction(district, dow, drop = TRUE)
    )
    # create time counterfacual dataset
  df_overlap <- df %>%
    dplyr::filter(hour %in% c(5L,6L,14L,15L,22L,23L)) %>%
    dplyr::mutate(
      end_shift = as.integer(
        (shift == 1 & hour %in% c(14L,15L)) |
          (shift == 2 & hour %in% c(22L,23L)) |
          (shift == 3 & hour %in% c(5L,6L))
      ),
      early_end = as.integer((shift == 1 & hour == 14L) | (shift == 2 & hour == 22L) | (shift == 3 & hour == 5L)),
      late_end  = as.integer((shift == 1 & hour == 15L) | (shift == 2 & hour == 23L) | (shift == 3 & hour == 6L))
    )
    # create time and officer counterfactual dataset
  df_overlap2 <- df_overlap %>% dplyr::group_by(officer) %>%
    dplyr::filter(dplyr::n_distinct(shift) > 1L) %>% dplyr::ungroup()
    # create early end of shift dataset
  df_early <- df %>%
    dplyr::filter(!(shift == 1 & hour > 14L),
                  !(shift == 2 & hour > 22L & hour < 2L),
                  !(shift == 3 & hour > 5L  & hour < 10L)) %>%
    dplyr::mutate(early_end = as.integer((shift == 1 & hour == 14L) | (shift == 2 & hour == 22L) | (shift == 3 & hour == 5L)))
  # create late end of shift dataset
  df_late <- df %>%
    dplyr::filter(!(shift == 1 & hour > 15L),
                  !(shift == 2 & hour > 23L & hour < 2L),
                  !(shift == 3 & hour > 6L  & hour < 11L)) %>%
    dplyr::mutate(late_end = as.integer((shift == 1 & hour == 15L) | (shift == 2 & hour == 23L) | (shift == 3 & hour == 6L)))

  # --- feols modification, return null in the event of an error ----------------------------------------------
  # this allows the analysis to run despite errors, we can investigate them in post
  safe_feols <- function(formula, data, ...) {
    if (is.null(data) || !is.data.frame(data) || nrow(data) == 0L) return(NULL)
    tryCatch(fixest::feols(formula, data = data, lean = TRUE, ...),
             error = function(e) NULL)
  }
  # function to pull coefficients and SEs from vars of interest
  pull_terms <- function(mod, wanted) {
    if (is.null(mod)) return(tibble::tibble(term = character(), estimate = numeric(), se = numeric()))
    cf <- stats::coef(mod)
    se <- tryCatch(fixest::se(mod), error = function(e) rep(NA_real_, length(cf)))
    keep <- names(cf) %in% wanted
    if (!any(keep)) return(tibble::tibble(term = character(), estimate = numeric(), se = numeric()))
    tibble::tibble(term = names(cf)[keep], estimate = unname(cf[keep]), se = unname(se[keep]))
  }
  
  # --- ANALYSIS -------------------------------------------
  # traffic stop per hour models
    # descriptive
  neg_total    <- safe_feols(num_stops ~ i(hour_into_shift, ref = 5) + total |
                               officer + fe_dwh + fe_dm + fe_dws,
                             data = df, cluster = "officer")
    # early bound final hour
  single_early <- safe_feols(num_stops ~ early_end + total |
                               officer + fe_dw + fe_dm + fe_dws,
                             data = df_early, cluster = "officer")
    # late bound final hour
  
  single_late  <- safe_feols(num_stops ~ late_end + total |
                               officer + fe_dw + fe_dm + fe_dws,
                             data = df_late, cluster = "officer")
    # within time counterfactual
  treat        <- safe_feols(num_stops ~ end_shift + total |
                               officer + fe_dwh + fe_dm + fe_dws,
                             data = df_overlap, cluster = "officer")
    # within time and officer counterfactual
  
  treat2       <- safe_feols(num_stops ~ end_shift + total |
                               officer + fe_dwh + fe_dm + fe_dws,
                             data = df_overlap2, cluster = "officer")
  # civil violations per stop models
    # descriptive
  civ_total <- safe_feols(civ_per_stop ~ i(hour_into_shift, ref = 5) + total |
                            officer + fe_dwh + fe_dm + fe_dws,
                          data = df, cluster = "officer")
    # early bound final hour
  civ_early <- safe_feols(civ_per_stop ~ early_end + total |
                            officer + fe_dw + fe_dm + fe_dws,
                          data = df_early, cluster = "officer")
    # late bound final hour
  civ_late  <- safe_feols(civ_per_stop ~ late_end + total |
                            officer + fe_dw + fe_dm + fe_dws,
                          data = df_late, cluster = "officer")
    # within time counterfactual
  civ2      <- safe_feols(civ_per_stop ~ end_shift + total |
                            officer + fe_dwh + fe_dm + fe_dws,
                          data = df_overlap, cluster = "officer")
    # within time and officer counterfactual
  civ3      <- safe_feols(civ_per_stop ~ end_shift + total |
                            officer + fe_dwh + fe_dm + fe_dws,
                          data = df_overlap2, cluster = "officer")
  # criminal violations per stop models
    # descriptive
  crim_total <- safe_feols(crim_per_stop ~ i(hour_into_shift, ref = 5) + total |
                             officer + fe_dwh + fe_dm + fe_dws,
                           data = df, cluster = "officer")
    # early bound final hour
  crim_early <- safe_feols(crim_per_stop ~ early_end + total |
                             officer + fe_dw + fe_dm + fe_dws,
                           data = df_early, cluster = "officer")
    # late bound final hour
  crim_late  <- safe_feols(crim_per_stop ~ late_end + total |
                             officer + fe_dw + fe_dm + fe_dws,
                           data = df_late, cluster = "officer")
    # within time counterfactual
  crim2      <- safe_feols(crim_per_stop ~ end_shift + total |
                             officer + fe_dwh + fe_dm + fe_dws,
                           data = df_overlap, cluster = "officer")
    # within time counterfactual
  crim3      <- safe_feols(crim_per_stop ~ end_shift + total |
                             officer + fe_dwh + fe_dm + fe_dws,
                           data = df_overlap2, cluster = "officer")
  
  # --- EXTRACT  terms of interest -------------------------------------------------------------
  gof_res <- dplyr::bind_rows(
    pull_gof(neg_total, "neg_total", b),
    pull_gof(single_early, "single_early", b),
    pull_gof(single_late, "single_late",b),
    pull_gof(treat, "treat", b),
    pull_gof(treat2, "treat2", b),
    
    pull_gof(civ_total, "civ_total",b),
    pull_gof(civ_early, "civ_early", b),
    pull_gof(civ_late, "civ_late", b),
    pull_gof(civ2, "civ2",b),
    pull_gof(civ3, "civ3", b),
    
    pull_gof(crim_total, "crim_total",b),
    pull_gof(crim_early, "crim_early", b),
    pull_gof(crim_late, "crim_late", b),
    pull_gof(crim2, "crim2",b),
    pull_gof(crim3, "crim3", b),
  )
  # terms to grab for descripitve outputs
  hrs <- paste0("hour_into_shift::", setdiff(sort(unique(df$hour_into_shift)), 5))
  
  # build dataset with coefficients and SEs from each iteration
  res <- dplyr::bind_rows(
    pull_terms(neg_total,  c(hrs, "total")) %>% dplyr::mutate(model = "neg_total",  param = term),
    pull_terms(single_early, c("early_end", "total")) %>% dplyr::mutate(model = "single_early", param = term),
    pull_terms(single_late,  c("late_end",  "total")) %>% dplyr::mutate(model = "single_late",  param = term),
    pull_terms(treat,       c("end_shift", "total")) %>% dplyr::mutate(model = "treat",  param = term),
    pull_terms(treat2,      c("end_shift", "total")) %>% dplyr::mutate(model = "treat2", param = term),
    
    pull_terms(civ_total,   c(hrs, "total")) %>% dplyr::mutate(model = "civ_total", param = term),
    pull_terms(civ_early,   c("early_end", "total")) %>% dplyr::mutate(model = "civ_early", param = term),
    pull_terms(civ_late,    c("late_end",  "total")) %>% dplyr::mutate(model = "civ_late",  param = term),
    pull_terms(civ2,        c("end_shift", "total")) %>% dplyr::mutate(model = "civ2",     param = term),
    pull_terms(civ3,        c("end_shift", "total")) %>% dplyr::mutate(model = "civ3",     param = term),
    
    pull_terms(crim_total,  c(hrs, "total")) %>% dplyr::mutate(model = "crim_total", param = term),
    pull_terms(crim_early,  c("early_end", "total")) %>% dplyr::mutate(model = "crim_early", param = term),
    pull_terms(crim_late,   c("late_end",  "total")) %>% dplyr::mutate(model = "crim_late",  param = term),
    pull_terms(crim2,       c("end_shift", "total")) %>% dplyr::mutate(model = "crim2",     param = term),
    pull_terms(crim3,       c("end_shift", "total")) %>% dplyr::mutate(model = "crim3",     param = term)
  ) %>%
    dplyr::select(model, param, estimate, se) %>%
    dplyr::mutate(iter = b)
  res <- dplyr::bind_rows(res, gof_res)
}



# Function 2 for iterative analysis
# this function includes models based on equation 3
# this function is identical to the previous one up until line 463
# randomize shift -> build panel dataset-> analysis -> extract relevant coefficients 
run_once_chunk <- function(b, base_seed, df_base, calls) {
  set.seed(base_seed + b)
  
  # --- RANDOMIZE ambiguous shift and start times -------------------------------
  df <- df_base %>%
    dplyr::mutate(
      early_late = dplyr::case_when(
        shift == 1 & any_5  ~ "early",
        shift == 2 & any_13 ~ "early",
        shift == 3 & any_20 ~ "early",
        TRUE ~ NA_character_
      ),
      .rand  = runif(dplyr::n()),
      .rand2 = runif(dplyr::n()),
      shift = dplyr::case_when(
        !is.na(shift) ~ shift,
        is.na(shift) & any_5_6 & !any_13_16 & !any_20_23 & !any_0 ~ ifelse(.rand < 0.5, 1L, 3L),
        is.na(shift) & any_13_16 & !any_5_6 & !any_20_23 & !any_0 ~ ifelse(.rand < 0.5, 1L, 2L),
        is.na(shift) & (any_20_23 | any_0_tomorrow) & !any_5_6 & !any_13_16 ~ ifelse(.rand < 0.5, 2L, 3L),
        is.na(shift) & (any_20_23_yesterday | any_0) & !any_5_6 & !any_13_16 ~ ifelse(.rand < 0.5, 2L, 3L),
        TRUE ~ NA_integer_
      ),
      early_late = ifelse(is.na(early_late), ifelse(.rand2 < 0.5, "early", "late"), early_late)
    ) %>%
    dplyr::select(-.rand, -.rand2) %>%
    dplyr::filter(!is.na(shift))
  
  # --- create shift_start + hour_into_shift
  stop_rand <- df %>%
    dplyr::mutate(
      base_dt = lubridate::as_datetime(date),
      
      start_hour = dplyr::case_when(
        shift == 1 & early_late == "early" ~ 5L,
        shift == 1 & early_late == "late"  ~ 6L,
        
        shift == 2 & hour < 12 & early_late == "early" ~ -11L,
        shift == 2 & hour < 12 & early_late == "late"  ~ -10L,
        shift == 2 & hour >= 12 & early_late == "early" ~ 13L,
        shift == 2 & hour >= 12 & early_late == "late"  ~ 14L,
        
        shift == 3 & hour < 10 & early_late == "early" ~ -4L,
        shift == 3 & hour < 10 & early_late == "late"  ~ -3L,
        shift == 3 & hour >= 18 & early_late == "early" ~ 20L,
        shift == 3 & hour >= 18 & early_late == "late"  ~ 21L,
        TRUE ~ NA_integer_
      ),
      
      shift_start = base_dt + lubridate::hours(start_hour),
      
      hour_into_shift = dplyr::case_when(
        shift == 1 & early_late == "early" ~ hour - 4L,
        shift == 1 & early_late == "late"  ~ hour - 5L,
        
        shift == 2 & early_late == "early" & hour >= 12L ~ hour - 12L,
        shift == 2 & early_late == "early" & hour <  12L ~ hour + 12L,
        shift == 2 & early_late == "late"  & hour >= 12L ~ hour - 13L,
        shift == 2 & early_late == "late"  & hour <  12L ~ hour + 11L,
        
        shift == 3 & early_late == "early" & hour >= 18L ~ hour - 19L,
        shift == 3 & early_late == "early" & hour <  10L ~ hour + 5L,
        shift == 3 & early_late == "late"  & hour >= 18L ~ hour - 20L,
        shift == 3 & early_late == "late"  & hour <  10L ~ hour + 4L,
        TRUE ~ NA_real_
      )
    ) %>% # filter data down to hours within shift
    dplyr::filter(hour_into_shift > 0 & hour_into_shift < 11) %>%
    dplyr::mutate(shift_date = as.Date(shift_start)) %>%
    dplyr::select(-base_dt, -start_hour)
  
  # --- BUILD panel dataset with n stops per hour and n violations per shift -------------------------------------
  # identify each unique officer-shift in the data
  shifts <- stop_rand %>%
    dplyr::distinct(officer, shift_date, shift_start, shift, district, dow, month, year)
  # create 10 rows per officer-shift, 1 for each hour
  complete_shifts <- tidyr::crossing(shifts, tibble::tibble(hour_into_shift = 1:10)) %>%
    dplyr::mutate(
      hour_ts = shift_start + lubridate::hours(hour_into_shift - 1L),
      date    = as.Date(hour_ts),
      hour    = lubridate::hour(hour_ts)
    ) %>%
    dplyr::select(-hour_ts)
  # compute stops per officer hour and violations per stop per officer hour values
  agg <- stop_rand %>%
    dplyr::group_by(officer, shift_date, shift_start, shift, district, dow, month, year, hour_into_shift, hour) %>%
    dplyr::summarise(
      num_stops       = dplyr::n(),
      total_civil_vio = sum(civil_vio, na.rm = TRUE),
      total_crim_vio  = sum(crim_vio,  na.rm = TRUE),
      .groups = "drop"
    )
  # merge the panel format and computed variables
  df_full <- complete_shifts %>%
    dplyr::left_join(agg, by = c("officer","shift_date","shift_start","shift","district",
                                 "dow","month","year","hour_into_shift","hour")) %>%
    dplyr::mutate(
      num_stops       = ifelse(is.na(num_stops), 0L, num_stops),
      total_civil_vio = ifelse(is.na(total_civil_vio), 0, total_civil_vio),
      total_crim_vio  = ifelse(is.na(total_crim_vio),  0, total_crim_vio)
    )
  # merge in CFS data by date-hour
  df <- df_full %>%
    dplyr::left_join(calls, by = c("date","hour")) %>%
    dplyr::mutate(
      total          = tidyr::replace_na(total, 0),
      civ_per_stop   = total_civil_vio / ifelse(num_stops == 0, NA_real_, num_stops),
      crim_per_stop  = total_crim_vio  / ifelse(num_stops == 0, NA_real_, num_stops),
      ## create a shift portion bin for beginning, middle and end
      shift_portion_text = dplyr::case_when(
        hour_into_shift < 4 ~ "beginning",
        hour_into_shift < 8 ~ "middle",
        TRUE                ~ "end"
      )
    )
  # group data by hour bins and officer to create stops per hour and vio per stop
  df_collapsed_big <- df %>% 
    dplyr::group_by(officer, shift_start, shift_portion_text) %>%
    dplyr::mutate(
      total_stops_bin = sum(num_stops),
      bin_hours       = if_else(shift_portion_text == "middle", 4L, 3L),
      stops_per_hour  = total_stops_bin / bin_hours,
      civ_vio  = if (all(is.na(civ_per_stop))) NA_real_ else sum(civ_per_stop, na.rm = TRUE) / bin_hours,
      crim_vio = if (all(is.na(crim_per_stop))) NA_real_ else sum(crim_per_stop, na.rm = TRUE) / bin_hours
      ) %>%
    dplyr::ungroup() %>% 
    # create fixed effect structure
    dplyr::mutate(
      year_month = interaction(year, month),
      fe_dwh    = interaction(district, dow, hour),
      fe_dm     = interaction(district, year_month),
      fe_dws    = interaction(district, shift, dow),
      fe_dw    = interaction(district, dow)
    )
  
  # --- feols modification, return null in the event of an error ----------------------------------------------
  # this allows the analysis to run despite errors, we can investigate them in post
  safe_feols <- function(formula, data, ...) {
    if (is.null(data) || !is.data.frame(data) || nrow(data) == 0L) return(NULL)
    tryCatch(fixest::feols(formula, data = data, lean = TRUE, ...),
             error = function(e) NULL)
  }
  # pull coefficients and SEs from vars of interest
  pull_terms <- function(mod, wanted) {
    if (is.null(mod)) return(tibble::tibble(term = character(), estimate = numeric(), se = numeric()))
    cf <- stats::coef(mod)
    se <- tryCatch(fixest::se(mod), error = function(e) rep(NA_real_, length(cf)))
    keep <- names(cf) %in% wanted
    if (!any(keep)) return(tibble::tibble(term = character(), estimate = numeric(), se = numeric()))
    tibble::tibble(term = names(cf)[keep], estimate = unname(cf[keep]), se = unname(se[keep]))
  }
  
  # --- ANALYSIS -------------------------------------------
  # traffic stops per hour, shift bins
  chunk        <- feols(stops_per_hour ~ i(shift_portion_text, ref = "middle") + total |
                          officer + fe_dwh + fe_dm + fe_dws,
                        data = df_collapsed_big, cluster = "officer")
  # civil violations per stop, shift bins
  civ_chunk <- safe_feols(civ_vio ~ i(shift_portion_text, ref = "middle") + total |
                            officer + fe_dwh + fe_dm + fe_dws,
                          data = df_collapsed_big, cluster = "officer")
  # criminal violations per stop, shift bins
  crim_chunk <- safe_feols(crim_vio ~ i(shift_portion_text, ref = "middle") + total |
                             officer + fe_dwh + fe_dm + fe_dws,
                           data = df_collapsed_big, cluster = "officer")
  
  
  # --- EXTRACT terms of interest -------------------------------------------------------------
  gof_res <- dplyr::bind_rows(
    pull_gof(chunk,     "chunk",     b),
    pull_gof(civ_chunk, "civ_chunk", b),
    pull_gof(crim_chunk,"crim_chunk",b)
  )
  chunk_terms <- c("shift_portion_text::beginning", "shift_portion_text::end", "total")
  
  res <- dplyr::bind_rows(
    pull_terms(chunk,     chunk_terms) %>% dplyr::mutate(model = "chunk",     param = term),
    pull_terms(civ_chunk, chunk_terms) %>% dplyr::mutate(model = "civ_chunk", param = term),
    pull_terms(crim_chunk,chunk_terms) %>% dplyr::mutate(model = "crim_chunk",param = term)
  ) %>%
    dplyr::select(model, param, estimate, se) %>%
    dplyr::mutate(iter = b)
  res <- dplyr::bind_rows(res, gof_res)
}




# run ----------------------------------------------------------
B <- 1000 # set number of iterations
base_seed <- 123 # set seed, this analysis used 123

# parallel processing (optional, but will reduce runtime)
workers <- max(1, min(8, parallel::detectCores() - 1))
future::plan(multisession, workers = workers)

# run full process with 1000 iterations
draws <- future_lapply(
  seq_len(B),
  run_once,
  base_seed = base_seed,
  df_base = df_base,
  calls = calls,
  future.seed = TRUE
)

# make the output into a dataframe format
iter <- bind_rows(draws)

# save out
saveRDS(iter, here::here("data", "processed", "iterations_not_binned.rds")) 


draws_chunk <- future_lapply(
  seq_len(B),
  run_once_chunk,
  base_seed = base_seed,
  df_base = df_base,
  calls = calls,
  future.seed = TRUE
)

# make the output into a dataframe format
iter_chunk <- bind_rows(draws_chunk)

# save out
saveRDS(iter_chunk, here::here("data", "processed", "iterations_binned.rds")) 

# combine outputs to make a final dataframe with every iteration of every model
final <- bind_rows(iter, iter_chunk)

# save out
saveRDS(final, here::here("data", "processed", "all_iters.rds"))




# summary stats -----------------------------------------------------------------

# optional read-in here to skip past iterative procedure
# final <- readRDS(here::here("data", "processed", "all_iters.rds"))

final <- final %>% 
  # group by model and parameter
  group_by(model, param) %>%
  summarise(
    # num of iterations that were not NA (sanity check)
    M = sum(!is.na(estimate) & !is.na(se)),
    
    # simple mean of each estimate
    mean   = mean(estimate, na.rm = TRUE),
    
    # (Rubin, 1976) computations for SEs
    W      = mean(se^2, na.rm = TRUE),  # within-iteration variance = mean(SE^2)
    B      = var(estimate, na.rm = TRUE), # between-iteration variance = var of betas
    T_var  = W + (1 + 1/M) * B, # total variance
    se_total = sqrt(T_var), # our SE value of interest
    nu = ifelse(M > 1 & B > 0, (M - 1) * (1 + W / ((1 + 1/M) * B))^2, Inf), # df

    # 95% CI using SE and df (for plotting)
    ci_low  = mean - qt(0.975, df = nu) * se_total,
    ci_high = mean + qt(0.975, df = nu) * se_total,
    
    # percentile stats
    minimum = min(estimate, na.rm = TRUE),
    p025   = quantile(estimate, 0.025, na.rm = TRUE),
    p050   = quantile(estimate, 0.050, na.rm = TRUE),
    p100   = quantile(estimate, 0.100, na.rm = TRUE),
    p900   = quantile(estimate, 0.900, na.rm = TRUE),
    p950   = quantile(estimate, 0.950, na.rm = TRUE),
    p975   = quantile(estimate, 0.975, na.rm = TRUE),
    maximum = max(estimate, na.rm = TRUE),
    .groups = "drop"
  ) %>% 
  select(., model, param, mean, se_total, minimum, maximum)

# calculate pvals for relevant coefs
final <- final %>% 
  mutate(pval = 2 * (1 - pnorm(abs(mean / se_total))))

# round to five decimals for cleanliness
final <- final %>% 
  mutate(across(where(is.numeric), ~ round(., 5)))

# save out
saveRDS(final, here::here("data", "processed", "final_summary_iters.rds"))
