suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
  library(purrr)
  library(tidyr)
  library(glue)
})

library(here)

# 1. Carga carpeta con fotos (3) ------------------------------------------
DATA_FOLDER <- here::here("data")

print(glue::glue('Cargando datos de carpeta {DATA_FOLDER}'))

# Obtiene ruta carpeta de condiciones de cultivo
culpture_conditions_folders <-
  list.dirs(DATA_FOLDER, recursive = FALSE)

print(glue::glue('Cantidad de condiciones de cultivo cargadas: {length(culpture_conditions_folders)}'))

# Identifica todos los archivos .csv asociados a las carpetas de condiciones de cultivo
culpture_conditions_files <-
  list.files(culpture_conditions_folders, full.names = TRUE)

print(glue('Procesando condiciones de cultivo...'))
suppressMessages(
culpture_conditions_tbl <-
  dplyr::tibble(filepath = culpture_conditions_files) %>%
  dplyr::mutate(
    condicion = purrr::map_chr(filepath, function(x) {
      splited_filepath <- unlist(strsplit(x, split = "/"))
      
      splited_filepath[[length(splited_filepath) - 1]]
      
    }),
    image = purrr::map_chr(filepath, function(x)
    {
      sub('\\.csv$', '', sapply(strsplit(x, split = "/"), tail, 1), -3)
      
    }),
    data_raw = purrr::map(filepath, function(x) {
      raw_data <- readr::read_csv(x)
      
      observations_tbl <- raw_data %>%
        dplyr::filter(is.na(Label)) %>%
        dplyr::select(-c('...1', 'Label'))
      
      statistics_tbl <- raw_data %>%
        dplyr::filter(!is.na(Label)) %>%
        dplyr::select(-'...1')
      
      list(observations_tbl, statistics_tbl)
    })
  ) %>%
  dplyr::mutate(observations = purrr::map(data_raw,  ~ .x[[1]]),
         statistics = purrr::map(data_raw,  ~ .x[[2]])) %>%
  dplyr::select(-data_raw)
)

if (!dir.exists(here::here("processed"))) {
  dir.create(here::here("processed"),
             recursive = TRUE)
}

print(glue('Guardando archivo consolidado en {here("processed", "observations.csv")} ...'))

culpture_conditions_tbl %>%
  dplyr::select(condicion, image, observations) %>%
  tidyr::unnest(observations) %>%
  readr::write_excel_csv(here("processed", "observations.csv"))

print(glue('Guardando archivo de estadisticas en {here("processed", "statistics.csv")} ...'))
culpture_conditions_tbl %>%
  dplyr::select(condicion, image, statistics) %>%
  tidyr::unnest(statistics) %>%
  readr::write_excel_csv(here("processed", "statistics.csv"))

culpture_conditions_tbl %>%
  dplyr::select(condicion, observations) %>%
  tidyr::unnest(observations) %>%
  dplyr::group_by(condicion) %>%
  tidyr::nest(data = -condicion) %>%
  dplyr::ungroup() %>%
  purrr::walk2(.x = .$condicion,
               .y = .$data,
               .f = ~ {
                 print(glue::glue('Guardando condicion: {.x}'))
                 .y %>%
                   readr::write_excel_csv(here("processed", paste0(.x, ".csv")))
               })
