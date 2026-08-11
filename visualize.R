library(tidyverse)
library(ltc)
library(purrr)
library(cowplot)

default_colors = adjust_ltc('dora', amount = -20)
bird(default_colors)

make_names_pretty = function(df) {
  
  if ('scenario' %in% names(df)) { df = df %>%
      mutate(scenario = case_when(scenario == 'ssp370' ~ 'SSP3-7.0',
                                  scenario == 'ssp585' ~ 'SSP5-8.5',
                                  scenario == 'historical' ~ 'Historical'))
  }
  
  if ('climate_future' %in% names(df)) { df = df %>%
    mutate(climate_future = case_when(climate_future == 'central' ~ 'Central',
                                        climate_future == 'hot-dry' ~ 'Hot-Dry',
                                        climate_future == 'hot-wet' ~ 'Hot-Wet',
                                        climate_future == 'warm-wet' ~ 'Warm-Wet',
                                        climate_future == 'warm-dry' ~ 'Warm-Dry',
                                        climate_future == 'historical' ~ 'Historical'))
  }
  
  if ('variable' %in% names(df)) {df = df %>%
      mutate(variable = case_when(variable == 'tas' ~ 'Annual temperature in K',
                                  variable == 'pr' ~ 'Annual precipitation in mm'))
  }
  
  return(df)
}

add_color_scheme = function(colors = default_colors) {
  scale_colour_manual(name = 'Climate Future',
                      values = c('Central' = 'black', 
                                 'Historical' = 'black',
                                 'Hot-Dry' = colors[3],
                                 'Warm-Wet' = colors[5],
                                 'Hot-Wet' = colors[2],
                                 'Warm-Dry' = colors[4])) 
}

add_fill_scheme = function(colors = default_colors) {
    scale_fill_manual(name = 'Climate Future',
                      values = c('Central' = 'black', 
                                 'Hot-Dry' = colors[3],
                                 'Warm-Wet' = colors[5],
                                 'Hot-Wet' = colors[2],
                                 'Warm-Dry' = colors[4]))
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
              fill = 'white', alpha = .25, color = color, linewidth = linewidth, linetype = linetype),
    scale_x_continuous(name = 'Temperature Anomaly (K)', 
                         breaks = round(climate_futures_quantiles_park[climate_futures_quantiles_park$quantile %in% c(0.25, 0.75),]$tas, 2)),
      scale_y_continuous(name = 'Precipitation Anomaly (mm)', 
                         breaks = round(climate_futures_quantiles_park[climate_futures_quantiles_park$quantile %in% c(0.25, 0.75),]$pr, 2)))
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
    add_fill_scheme() 
}

plot_model_overview = function(park) {
  climate_future_levels = c('Hot-Dry', 'Hot-Wet', 'Central', 'Warm-Dry', 'Warm-Wet' )
  
  data = read_data(park)[[1]]
  
  model_order = data %>%
    distinct(model, scenario, climate_future) %>%
    pivot_wider(names_from = scenario, values_from = climate_future) %>%
    arrange(!is.na(`SSP3-7.0`),
            -match(`SSP3-7.0`, climate_future_levels),
            -match(`SSP5-8.5`, climate_future_levels), desc(model))
  
  climate_futures_park = data %>%
    mutate(model = factor(model, levels = union(model_order$model, model)))
  
  ggplot() + theme_classic() +
    geom_tile(data = climate_futures_park, color = 'black',
              aes(y = model, x = scenario, fill = climate_future)) +
    add_fill_scheme() +
    scale_x_discrete(name = 'Scenario', expand = c(0, 0)) +
    scale_y_discrete(name = 'Model', expand = c(0, 0)) +
    theme(panel.border = element_rect(color = 'black', fill = NA,
                                      linewidth = .75))

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

load_timeseries = function(park, variable) {
  df = read_csv(paste0("outputs/ensemble_", variable, "_", park, ".csv")) %>%
    mutate(year = year(time),
           across(any_of('pr'), ~ .x * 86400)) %>%
    group_by(year, scenario, model, climate_future) %>%
    summarize(across(any_of("tas"), mean, .names = "value"),
              across(any_of("pr"),  sum,  .names = "value"),
              .groups = "drop") %>%
    mutate(park = toupper(park),
           variable = variable) %>%
    filter(!is.na(value))
  
  return(df)
}

filter_to_selected_futures = function(df, futures) {
  selected_futures = df %>%
    filter(climate_future %in% futures)
  
  historical = df %>%
    filter(climate_future == 'Historical') %>%
    semi_join(selected_futures, by = c('park', 'model'))
  
  return(bind_rows(selected_futures, historical))
}

plot_climate_futures_ensemble = function(parks, futures = c('Warm-Wet', 'Hot-Dry')) {
  df = expand_grid(park = parks, variable = c('tas', 'pr')) %>%
    pmap(load_timeseries) %>%
    list_rbind() %>%
    make_names_pretty() %>%
    filter_to_selected_futures(futures)
  
  ggplot() + theme_classic() +
    facet_grid(cols = vars(park), rows = vars(variable), scales = 'free_y') +
    geom_line(data = df, aes(x = year, y = value, 
                             group = interaction(scenario, model, climate_future), 
                             color = climate_future)) +
    add_color_scheme() +
    scale_y_continuous(name = '')
}

plot_climate_futures('jotr')
plot_climate_futures('deva')

plot_model_overview('jotr')
plot_model_overview('deva')

combine_two_parks()

plot_climate_futures_ensemble(park = c('jotr', 'deva'))


