library(tidyverse)
library(ltc)
library(purrr)

setwd("~/Desktop/NPS/climate_futures")
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

add_reference_box = function(park, linetype, color, linewidth) {
  climate_futures_quantiles_park = read_csv(paste0("outputs/climate_futures_quantiles_", park, ".csv")) %>%
    mutate('Park' = park) %>%
    rename(quantile = '...1')
  
  list(geom_hline(yintercept = climate_futures_quantiles_park[climate_futures_quantiles_park$quantile %in% c(0.5),]$pr, 
             linetype = linetype, color = color, linewidth = linewidth),
    geom_vline(xintercept = climate_futures_quantiles_park[climate_futures_quantiles_park$quantile %in% c(0.5),]$tas, 
               linetype = linetype, color = color, linewidth = linewidth),
    geom_rect(data = climate_futures_quantiles_park[climate_futures_quantiles_park$quantile %in% c(0.25, 0.75),], 
              aes(xmin = min(tas), xmax = max(tas), ymin = min(pr), ymax = max(pr)), 
              fill = 'white', alpha = .25, color = color, linewidth = linewidth, linetype = linetype))
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

plot_climate_futures = function(park) {
  data = read_data(park)
  climate_futures_park = data[[1]]
 
  
  ggplot() + theme_classic() +
    ggtitle(paste0('Climate Futures for ', toupper(park))) +
    add_reference_box(park, 'solid', 'grey', .5) +
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
                       breaks = round(climate_futures_quantiles_park[climate_futures_quantiles_park$quantile %in% c(0.25, 0.75),]$tas, 2)) +
    scale_y_continuous(name = 'Precipitation (mm)', 
                       breaks = round(climate_futures_quantiles_park[climate_futures_quantiles_park$quantile %in% c(0.25, 0.75),]$pr, 2))
}



combine_two_parks = function(parks = c('jotr', 'deva'), shapes = c(21, 1), sizes = c(1.75, 1)) {
  
  climate_futures = bind_rows(read_data(parks[1])[[1]], read_data(parks[2])[[1]])
  
  ggplot() + theme_classic() +
    ggtitle(paste0('Climate Futures for ', toupper(parks[1]), ' and ', toupper(parks[2]))) +
    add_reference_box(parks[1], 'dashed', 'grey', .25) +
    add_reference_box(parks[2], 'solid', 'grey', .5) +
    geom_line(data = climate_futures, linewidth = .25, alpha = .5,
              aes(x = tas, y = pr, group = interaction(scenario, model))) +
    geom_point(data = climate_futures,
               aes(x = tas, y = pr, color = climate_future, fill = climate_future,
                   shape = Park, size = Park)) +
    add_color_scheme() +
    add_fill_scheme() +
    scale_shape_manual(values = setNames(shapes, parks), name = "Park") +
    scale_size_manual( values = setNames(sizes,  parks), name = "Park") +
    guides(shape = guide_legend(override.aes = list(fill = "black")))
  
}

combine_two_parks()

plot_climate_futures('jotr')
plot_climate_futures('deva')




