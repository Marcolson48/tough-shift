library(tidyverse)
library(lubridate)
library(tidyr)
library(arrow)
library(knitr)
library(readr)
library(here)

## this file constructs the figures and tables from the manuscript in chronological order

# read in iterative data
df <- readRDS(here("data", "processed", "final_summary_iters.rds"))

# read in single instance of randomized data for descriptives
# add per stop variables
desc <- readRDS(here("data", "processed", "descriptives.rds")) %>% 
  mutate(crim_per_stop = total_crim_vio / num_stops,
         civ_per_stop = total_civil_vio / num_stops)



# Figure 1 ----------------------------------------------------------------


# filter out CFS coefficients and fit stats
f1 <- df %>% 
  filter(!param %in% c("Adj.R2", "Within.R2", "N", "total"))

# filter to the descriptive model for stops per hour
overall_hourly <- f1 %>% 
  filter(model == "neg_total") %>% 
  select(., -model) %>%
  mutate(hour = parse_number(param)) # extract hour number from each coefficient

# make a row for hour 5 (reference hour) at 0 with SE 0
new_row <- overall_hourly %>%
  slice(1) %>%
  mutate(across(everything(), ~0)) %>% 
  mutate(hour = 5,
         param = "hour_into_shift::5")

# bring hour 5 into the object
overall_hourly <- bind_rows(overall_hourly, new_row)

# plot
ggplot(overall_hourly, aes(x = hour)) +
  geom_hline(yintercept = 0, color = "black", linewidth = 0.4) +
  geom_errorbar(aes(ymin = mean - 1.96 * se_total,
                    ymax = mean + 1.96 * se_total),
                width = 0.25, color = "black")  +
  geom_line(aes(y = mean), color = "black", linewidth = 0.4) +
  geom_point(aes(y = mean),
             shape = 16, size = 2) +
  scale_x_continuous(breaks = sort(unique(overall_hourly$hour))) +
  labs(x = "Shift Hour", y = "Traffic Stop Frequency") +
  theme_bw(base_size = 10, base_family = "serif") +
  theme(
    panel.grid.major = element_line(color = "grey85", linetype = "dotted"),
    panel.grid.minor = element_blank(),
    legend.position = "none",
    plot.title = element_text(hjust = 0.5)
  )





# Table 1 ---------------------------------------------------------------

# data for table 1 is native to our summarized dataset
# print coef values
df %>% filter(model == "single_early" | model == "single_late") %>% 
  filter(!param %in% c("Adj.R2", "Within.R2", "N"))

# print diagnostic stats and Ns
df %>% filter(model == "single_early" | model == "single_late") %>% 
  filter(param %in% c("Adj.R2", "Within.R2", "N")) %>% 
  select(., model, param, mean)



# Figure 2 ----------------------------------------------------------------

# filter out CFS coefficients and fit stats
f2 <- df %>% 
  filter(!param %in% c("Adj.R2", "Within.R2", "N", "total"))

# filter to the binned model
f2 <- f2 %>% 
  filter(model == "chunk")

#  bind the rows with a reference point for middle of shift at 0
f2_plot <- f2 %>%
  bind_rows(tibble(
    model    = "chunk",
    param    = "shift_portion_text::middle",
    mean     = 0,
    se_total = 0,
    minimum  = NA_real_,
    maximum  = NA_real_
  )) %>%
  filter(model == "chunk") %>%
  mutate(
    param = factor(
      param,
      levels = c("shift_portion_text::beginning",
                 "shift_portion_text::middle",
                 "shift_portion_text::end"),
      labels = c("Beginning", "Middle", "End")
    )
  )

# plot
ggplot(f2_plot, aes(x = param)) +
  geom_hline(yintercept = 0, color = "black", linewidth = 0.4) +
  geom_errorbar(
    aes(ymin = mean - 1.96 * se_total,
        ymax = mean + 1.96 * se_total),
    width = 0.25, color = "black"
  ) +
  geom_line(aes(y = mean, group = 1), color = "black", linewidth = 0.4) +
  geom_point(aes(y = mean), shape = 16, size = 2) +
  labs(x = "Shift Portion", y = "Traffic Stop Frequency") +
  theme_bw(base_size = 10, base_family = "serif") +
  theme(
    panel.grid.major = element_line(color = "grey85", linetype = "dotted"),
    panel.grid.minor = element_blank(),
    legend.position  = "none",
    plot.title       = element_text(hjust = 0.5)
  )



# Table 2 -----------------------------------------------------------------

# data for table 2 is native to our summarized dataset
# print coef values
df %>% filter(model == "treat" | model == "treat2") %>% 
  filter(!param %in% c("Adj.R2", "Within.R2", "N"))

# print diagnostic stats and Ns
df %>% filter(model == "treat" | model == "treat2") %>% 
  filter(param %in% c("Adj.R2", "Within.R2", "N")) %>% 
  select(., model, param, mean)





# Figure 3 ----------------------------------------------------------------

# filter out CFS coefficients and fit stats
f3_civ <- df %>% 
  filter(!param %in% c("Adj.R2", "Within.R2", "N", "total"))

# filter to the descriptive model for civil violations per stop
f3_civ <- f3_civ %>% 
  filter(model == "civ_total") %>% 
  select(., -model) %>% 
  mutate(hour = parse_number(param))

# bind with the hour 5 reference we made earlier
f3_civ <- bind_rows(f3_civ, new_row)

# plot (civil portion)
ggplot(f3_civ, aes(x = hour)) +
  geom_hline(yintercept = 0, color = "black", linewidth = 0.4) +
  # CI bars from empirical percentiles
  geom_errorbar(aes(ymin = mean - 1.96 * se_total,
                    ymax = mean + 1.96 * se_total),
                width = 0.25, color = "black")  +
  # mean line
  geom_line(aes(y = mean), color = "black", linewidth = 0.4) +
  # mean points
  geom_point(aes(y = mean),
             shape = 16, size = 2) +
  scale_x_continuous(breaks = sort(unique(overall_hourly$hour))) +
  labs(x = "Shift Hour", y = "Civil Violations per Traffic Stop") +
  theme_bw(base_size = 10, base_family = "serif") +
  theme(
    panel.grid.major = element_line(color = "grey85", linetype = "dotted"),
    panel.grid.minor = element_blank(),
    legend.position = "none",
    plot.title = element_text(hjust = 0.5)
  )


# filter out CFS coefficients and fit stats
f3_crim <- df %>% 
  filter(!param %in% c("Adj.R2", "Within.R2", "N", "total"))

# filter to the descriptive model for criminal violations per stop
f3_crim <- f3_crim %>% 
  filter(model == "crim_total") %>% 
  select(., -model) %>% 
  mutate(hour = parse_number(param))

# bind with the hour 5 reference we made earlier
f3_crim <- bind_rows(f3_crim, new_row)

# plot
ggplot(f3_crim, aes(x = hour)) +
  geom_hline(yintercept = 0, color = "black", linewidth = 0.4) +
  # CI bars from empirical percentiles
  geom_errorbar(aes(ymin = mean - 1.96 * se_total,
                    ymax = mean + 1.96 * se_total),
                width = 0.25, color = "black")  +
  # mean line
  geom_line(aes(y = mean), color = "black", linewidth = 0.4) +
  # mean points
  geom_point(aes(y = mean),
             shape = 16, size = 2) +
  scale_x_continuous(breaks = sort(unique(overall_hourly$hour))) +
  labs(x = "Shift Hour", y = "Criminal Violations per Traffic Stop") +
  theme_bw(base_size = 10, base_family = "serif") +
  theme(
    panel.grid.major = element_line(color = "grey85", linetype = "dotted"),
    panel.grid.minor = element_blank(),
    legend.position = "none",
    plot.title = element_text(hjust = 0.5)
  )



# Table 3 -----------------------------------------------------------------

# data for table 3 is native to our summarized dataset
# print coef values
df %>% filter(model == "civ_early" | model == "civ_late" | 
                model == "crim_early" | model == "crim_late") %>% 
  filter(!param %in% c("Adj.R2", "Within.R2", "N"))

# print diagnostic stats and Ns
df %>% filter(model == "civ_early" | model == "civ_late" | 
                model == "crim_early" | model == "crim_late") %>% 
  filter(param %in% c("Adj.R2", "Within.R2", "N")) %>% 
  select(., model, param, mean)




# Figure 4 ----------------------------------------------------------------

# filter out CFS coefficients and fit stats
f4 <- df %>% 
  filter(!param %in% c("Adj.R2", "Within.R2", "N", "total"))

# filter to the binned model
f4_civ <- f4 %>% 
  filter(model == "civ_chunk")

#  bind the rows with a reference point for middle of shift at 0
f4_civ <- f4_civ %>%
  bind_rows(tibble(
    model    = "civ_chunk",
    param    = "shift_portion_text::middle",
    mean     = 0,
    se_total = 0,
    minimum  = NA_real_,
    maximum  = NA_real_
  )) %>%
  filter(model == "civ_chunk") %>%
  mutate(
    param = factor(
      param,
      levels = c("shift_portion_text::beginning",
                 "shift_portion_text::middle",
                 "shift_portion_text::end"),
      labels = c("Beginning", "Middle", "End")
    )
  )

# plot (civil portion)
ggplot(f4_civ, aes(x = param)) +
  geom_hline(yintercept = 0, color = "black", linewidth = 0.4) +
  geom_errorbar(
    aes(ymin = mean - 1.96 * se_total,
        ymax = mean + 1.96 * se_total),
    width = 0.25, color = "black"
  ) +
  geom_line(aes(y = mean, group = 1), color = "black", linewidth = 0.4) +
  geom_point(aes(y = mean), shape = 16, size = 2) +
  labs(x = "Shift Portion", y = "Civil Violations per Stop") +
  theme_bw(base_size = 10, base_family = "serif") +
  theme(
    panel.grid.major = element_line(color = "grey85", linetype = "dotted"),
    panel.grid.minor = element_blank(),
    legend.position  = "none",
    plot.title       = element_text(hjust = 0.5)
  )



# filter to the binned model
f4_crim <- f4 %>% 
  filter(model == "crim_chunk")

#  bind the rows with a reference point for middle of shift at 0
f4_crim <- f4_crim %>%
  bind_rows(tibble(
    model    = "crim_chunk",
    param    = "shift_portion_text::middle",
    mean     = 0,
    se_total = 0,
    minimum  = NA_real_,
    maximum  = NA_real_
  )) %>%
  filter(model == "crim_chunk") %>%
  mutate(
    param = factor(
      param,
      levels = c("shift_portion_text::beginning",
                 "shift_portion_text::middle",
                 "shift_portion_text::end"),
      labels = c("Beginning", "Middle", "End")
    )
  )

# plot (civil portion)
ggplot(f4_crim, aes(x = param)) +
  geom_hline(yintercept = 0, color = "black", linewidth = 0.4) +
  geom_errorbar(
    aes(ymin = mean - 1.96 * se_total,
        ymax = mean + 1.96 * se_total),
    width = 0.25, color = "black"
  ) +
  geom_line(aes(y = mean, group = 1), color = "black", linewidth = 0.4) +
  geom_point(aes(y = mean), shape = 16, size = 2) +
  labs(x = "Shift Portion", y = "Criminal Violations per Stop") +
  theme_bw(base_size = 10, base_family = "serif") +
  theme(
    panel.grid.major = element_line(color = "grey85", linetype = "dotted"),
    panel.grid.minor = element_blank(),
    legend.position  = "none",
    plot.title       = element_text(hjust = 0.5)
  )




# Table 4 -----------------------------------------------------------------

# data for table 4 is native to our summarized dataset
# print coef values
df %>% filter(model == "civ2" | model == "civ3" | 
                model == "crim2" | model == "crim3") %>% 
  filter(!param %in% c("Adj.R2", "Within.R2", "N"))

# print diagnostic stats and Ns
df %>% filter(model == "civ2" | model == "civ3" | 
                model == "crim2" | model == "crim3") %>% 
  filter(param %in% c("Adj.R2", "Within.R2", "N")) %>% 
  select(., model, param, mean)





# Appendix ----------------------------------------------------------------

desc_summary <- desc %>% 
  group_by(hour) %>% 
  summarize(
    stops     = sum(num_stops, na.rm = TRUE),
    crim_per  = mean(crim_per_stop, na.rm = TRUE),
    civil_per = mean(civ_per_stop, na.rm = TRUE)
  )

# plot (figure A1)
ggplot(desc_summary) +
  geom_rect(
    aes(xmin = hour,
        xmax = hour + 0.95,
        ymin = 0,
        ymax = stops),
    fill = "grey40", color = "black"
  ) +
  scale_x_continuous(limits = c(0, 24), breaks = seq(0, 24, 2)) +
  scale_y_continuous(limits = c(0, 28000),
                     breaks = seq(0, 30000, 5000)) +
  labs(
    title = "Hourly Totals: Traffic Stops",
    x = "Hour of Day",
    y = "Number of Stops"
  ) +
  theme_minimal(base_size = 12, base_family = "serif") +
  theme(
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_blank(),
    panel.grid.major.y = element_line(color = "grey80", size = 0.3),
    axis.ticks = element_line(color = "black", size = 0.3),
    axis.line = element_line(color = "black", size = 0.4),
    plot.title = element_text(hjust = 0.5, face = "bold")
  )


# table A1
# overall totals for stops
overall_summary <- desc %>%
  summarize(
    officers = as.numeric(n_distinct(officer)),
    stops    = sum(num_stops),
    stops_per_officer = mean(num_stops, na.rm = T),
    stops_per_officer_sd = sd(num_stops, na.rm = T),
    crim_per = sum(total_crim_vio, na.rm = T) / sum(num_stops),
    civil_per= sum(total_civil_vio, na.rm = T) / sum(num_stops)
  ) %>%
  # Create an "hour" column so the overall row can be identified and combined with the hourly summary
  mutate(hour = 999) %>%
  select(hour, everything())

desc <- desc %>%  
  mutate(officer = as.character(officer))

# hourly totals for stops
hourly_summary <- desc %>%
  group_by(hour) %>% 
  summarize(
    # n_hours = n(),
    officers = as.numeric(n_distinct(officer)),
    stops    = sum(num_stops, na.rm = T),
    stops_per_officer = mean(num_stops, na.rm = T),
    stops_per_officer_sd = sd(num_stops, na.rm = T),
    crim_per = sum(total_crim_vio, na.rm = T) / sum(num_stops),
    civil_per= sum(total_civil_vio, na.rm = T) / sum(num_stops)
  )

hourly_summary <- bind_rows(hourly_summary, overall_summary)

# cfs totals
cfs <- desc %>%
  distinct(date, hour, .keep_all = TRUE) %>%
  group_by(hour) %>%
  summarize(cfs_tot = sum(total, na.rm = TRUE))

all_cfs <- desc %>% 
  distinct(date, hour, .keep_all = TRUE) %>%
  summarize(cfs_tot = sum(total, na.rm = TRUE)) %>% 
  mutate(hour = 999)

cfs <- bind_rows(cfs, all_cfs)

# bind the two
append_table <- left_join(hourly_summary, cfs, by = "hour") %>% 
  select(., hour, cfs_tot, everything())

# display the table as it appears in the manuscript
# hour = 999 refers to the total
print(append_table, n = 25)



# shift assignment visual, Appendix B


#  raw schedules (hours since midnight)
phpd_sched <- tibble(
  shift = rep(c("Day", "Swing", "Night"), each = 3),
  start = c(5.5, 6, 6.5, 13.5, 14, 14.5, 20, 20.5, 21),
  end   = c(15.5, 16, 16.5, 23.5, 0, 0.5, 6, 6.5, 7))


# function to split intervals that wrap past midnight
expand_wrap <- function(df) {
  df %>% rowwise() %>% do({
    if (.$end > .$start) {
      tibble(shift = .$shift, start = .$start, end = .$end)
    } else {
      tibble(
        shift = rep(.$shift, 2),
        start = c(.$start, 0),
        end   = c(24, .$end)
      )
    }
  }) %>% ungroup()
}

# build and vertically offset each sub‐shift
phpd_seg <- expand_wrap(phpd_sched) %>% 
  group_by(shift) %>%
  mutate(
    base_y = case_when(
      shift == "Day" ~ 1,
      shift == "Swing" ~ 2,
      shift == "Night" ~ 3
    )) %>%
  ungroup()

phpd_seg <- phpd_seg[-6, ]
phpd_seg$y <- c(0.6, 0.8, 1, 1.6, 1.8, 2, 2, 2.6, 2.6, 2.8, 2.8, 3, 3)

# hour ticks
ticks <- tibble(hour = 0:24)

# build example stops
phpd_officers <- tibble(
  officer = factor(rep(1:3, times = c(2, 2, 3))),
  x       = c(6.78, 11.95,  # Officer 1
              6.78, 15.02,      # Officer 2
              5.76, 11.28, 13.72)) %>%
  mutate(
    y_off = case_when(
      officer == "1" ~ 3.5,
      officer == "2" ~ 4,
      officer == "3" ~ 4.5
    ))

plot_shifts <- function(seg_df, dept_name, stops_df, ticks) {
  # one label per shift (placed at segment midpoint)
  label_df <- seg_df |>
    dplyr::group_by(shift) |>
    dplyr::slice_max(start, n = 1, with_ties = FALSE) |>
    dplyr::ungroup() |>
    dplyr::mutate(xmid = (start + end) / 2)
  
  ggplot() +
    geom_segment(aes(x = 0, xend = 24, y = 0, yend = 0), linewidth = 0.4, color = "black") +
    geom_segment(data = ticks,
                 aes(x = hour, xend = hour, y = 0, yend = 0.2),
                 linewidth = 0.3, color = "black") +
    
    geom_vline(data = stops_df,
               aes(xintercept = x), linetype = "dashed",
               color = "black", linewidth = 0.25) +
    
    geom_point(
      data = stops_df,
      aes(x = x, y = y_off, shape = officer, fill = officer),
      size = 2.4, color = "black", stroke = 0.3
    ) +
    
    geom_segment(
      data = seg_df,
      aes(x = start, xend = end, y = y, yend = y, linetype = shift),
      linewidth = 0.7, color = "black", lineend = "butt"
    ) +
    
    geom_segment(data = seg_df,
                 aes(x = start, xend = start, y = y, yend = y - 0.12),
                 linewidth = 0.5, color = "black") +
    geom_segment(data = seg_df,
                 aes(x = end, xend = end, y = y, yend = y - 0.12),
                 linewidth = 0.5, color = "black") +
    
    geom_text(
      data = label_df,
      aes(x = xmid, y = y + 0.18, label = shift),
      size = 3, family = "serif", vjust = 0
    ) +
    
    # scales & guides
    scale_x_continuous(limits = c(0, 24), breaks = seq(0, 24, 2), expand = expansion(add = 0)) +
    scale_shape_manual(
      name = "Officer",
      values = c(21, 22, 24)
    ) +
    scale_fill_manual(
      name = "Officer",
      values = c("white", "grey60", "black")
    ) +
    scale_linetype_manual(
      name = NULL,  # hide legend title for shifts
      values = c("solid", "dashed", "dotdash", "longdash", "twodash")
    ) +
    
    # labels
    labs(x = "Hour of Day", y = NULL, title = dept_name) +
    
    theme_classic(base_size = 11, base_family = "serif") +
    theme(
      panel.grid.major.y = element_line(color = "grey85", linewidth = 0.3),
      panel.grid.minor = element_blank(),
      axis.title.y = element_blank(),
      axis.text.y  = element_blank(),
      axis.ticks.y = element_blank(),
      legend.position = "bottom",
      legend.box = "vertical",
      plot.title = element_text(hjust = 0.5, face = "bold")
    )
}

ticks <- data.frame(hour = 0:24)
plot_shifts(phpd_seg, "", phpd_officers, ticks = ticks)



# binned models, tabular outputs, Appendix C

# C2
# print coef values
df %>% filter(model == "chunk") %>% 
  filter(!param %in% c("Adj.R2", "Within.R2", "N"))

# print diagnostic stats and Ns
df %>% filter(model == "chunk") %>% 
  filter(param %in% c("Adj.R2", "Within.R2", "N")) %>% 
  select(., model, param, mean)



# C3
# print coef values
df %>% filter(model == "civ_chunk" | model == "crim_chunk") %>% 
  filter(!param %in% c("Adj.R2", "Within.R2", "N"))

# print diagnostic stats and Ns
df %>% filter(model == "civ_chunk" | model == "crim_chunk") %>% 
  filter(param %in% c("Adj.R2", "Within.R2", "N")) %>% 
  select(., model, param, mean)
