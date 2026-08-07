library(tidyverse)
library(ltc)
library(purrr)

alger = ltc('alger')
bird(alger)

make_names_pretty = function(df) {
  df = df %>%
    mutate(scenario = case_when(scenario == 'ssp370' ~ 'SSP3-7.0',
                        scenario == 'ssp585' ~ 'SSP5-8.5'),
           climate_future = case_when(climate_future == 'central' ~ 'Central',
                                      climate_future == 'hot-dry' ~ 'Hot-Dry',
                                      climate_future == 'warm-wet' ~ 'Warm-Wet',
                                      climate_future == 'hot-wet' ~ 'Hot-Wet',
                                      climate_future == 'warm-dry' ~ 'Warm-Dry'))
  return(df)
}

add_color_scheme = function() {
  alger = ltc('alger')
  scale_colour_manual(name = 'Climate Future',
                      values = c('Central' = alger[1], 
                                 'Hot-Dry' = alger[5],
                                 'Warm-Wet' = alger[2],
                                 'Hot-Wet' = alger[4],
                                 'Warm-Dry' = alger[3])) 
}

add_fill_scheme = function() {
  alger = ltc('alger')
    scale_fill_manual(name = 'Climate Future',
                      values = c('Central' = alger[1], 
                                 'Hot-Dry' = alger[5],
                                 'Warm-Wet' = alger[2],
                                 'Hot-Wet' = alger[4],
                                 'Warm-Dry' = alger[3]))
}

read_data = function(park) {
  climate_futures_park = read_csv(paste0("outputs/climate_futures_", park, ".csv")) %>%
    mutate('Park' = park) %>%
    make_names_pretty()
  
  climate_futures_quantiles_park = read_csv(paste0("outputs/climate_futures_quantiles_", park, ".csv")) %>%
    mutate('Park' = park) %>%
    rename(quantile = '...1')
  
  return(list(climate_futures_park, climate_futures_quantiles_park))
}

climate_futures = bind_rows(read_data('jotr')[[1]], read_data('deva')[[1]])
climate_futures_quantiles = bind_rows(read_data('jotr')[[2]], read_data('deva')[[2]])

ggplot() + theme_classic() +
  facet_wrap(~Park) +
  geom_hline(yintercept = climate_futures_quantiles[climate_futures_quantiles$quantile %in% c(0.5),]$pr, 
             linetype = "dashed", color = "grey") +
  geom_vline(xintercept = climate_futures_quantiles[climate_futures_quantiles$quantile %in% c(0.5),]$tas, 
             linetype = "dashed", color = "grey") +
  geom_rect(data = climate_futures_quantiles[climate_futures_quantiles$quantile %in% c(0.25, 0.75),], 
            aes(xmin = min(tas), xmax = max(tas), ymin = min(pr), ymax = max(pr)), 
            fill = 'white', alpha = .25, color = 'grey') +
  geom_point(data = climate_futures_park, size = 1.75, shape = 1, stroke = 1, alpha = 1,
             aes(x = tas, y = pr, color = climate_future, fill = climate_future,
                 alpha = scenario)) +
  geom_point(data = climate_futures_park, size = 1.75, shape = 21, stroke = 1,
             aes(x = tas, y = pr, color = climate_future, fill = climate_future,
                 alpha = scenario)) +
  scale_alpha_manual(values = c('SSP3-7.0' = 0, 'SSP5-8.5' = 1),
                     name = 'Scenario') +
  add_color_scheme() +
  add_fill_scheme() +
  scale_x_continuous(name = 'Temperature (°C)', 
                     breaks = round(climate_futures_quantiles[climate_futures_quantiles$quantile %in% c(0.25, 0.75),]$tas, 2)) +
  scale_y_continuous(name = 'Precipitation (mm)', 
                     breaks = round(climate_futures_quantiles[climate_futures_quantiles$quantile %in% c(0.25, 0.75),]$pr, 7))

ggplot() + theme_classic() +
  geom_line(data = climate_futures, linewidth = .5,
             aes(x = tas, y = pr, group = interaction(scenario, model))) +
  geom_point(data = climate_futures, size = 1.75, stroke = 1,
             aes(x = tas, y = pr, color = climate_future, fill = climate_future,
                 shape = Park)) +
  add_color_scheme() +
  add_fill_scheme() +
  scale_x_continuous(name = 'Temperature (°C)', 
                     breaks = round(climate_futures_quantiles_park[climate_futures_quantiles_park$quantile %in% c(0.25, 0.75),]$tas, 2)) +
  scale_y_continuous(name = 'Precipitation (mm)', 
                     breaks = round(climate_futures_quantiles_park[climate_futures_quantiles_park$quantile %in% c(0.25, 0.75),]$pr, 7))
