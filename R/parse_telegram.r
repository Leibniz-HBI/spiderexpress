parse_telegram_messages <-  function(html_source, file) {

  messages <- html_source %>% 
    html_nodes(xpath = "//div[@class='tgme_widget_message_bubble']")
  
  rel_xpaths <- list(
    post_id = "../@data-post",
    views = ".//span[@class='tgme_widget_message_views']",
    datetime = ".//time/@datetime",
    user = ".//a[@class='tgme_widget_message_owner_name']",
    from_author =".//span[@class='tgme_widget_message_from_author']",
    text = "./div[contains(@class, 'tgme_widget_message_text')]",
    link = ".//div[contains(@class, 'tgme_widget_message_text')]//a/@href",
    reply_to_user = ".//a[@class='tgme_widget_message_reply']//span[@class='tgme_widget_message_author_name']",
    reply_to_text = ".//a[@class='tgme_widget_message_reply']//div[@class='tgme_widget_message_text']",
    reply_to_link = ".//a[@class='tgme_widget_message_reply']/@href",
    image_url = "./a[@class='tgme_widget_message_photo_wrap']/@style",
    forwarded_message_url = ".//a[@class='tgme_widget_message_forwarded_from_name']/@href",
    forwarded_message_user = ".//a[@class='tgme_widget_message_forwarded_from_name']",
    poll_question = ".//div[@class='tgme_widget_message_poll_question']",
    poll_options_text = ".//div[@class='tgme_widget_message_poll_option_value']",
    poll_options_percent = ".//div[@class='tgme_widget_message_poll_option_percent']",
    video_url = ".//video[contains(@class, 'tgme_widget_message_video')]/@src",
    video_duration = ".//time[contains(@class, 'message_video_duration')]"
  )
  
  df <- messages %>%
    map(function(.x) {
      .list <- map(rel_xpaths, function(.xpath) {
        html_nodes(.x, xpath = .xpath) %>% 
          html_text(trim = T) %>% {
            if (length(.) == 0) {
              list(NA_character_)
            } else list(.)
          }
      })
      
      as_tibble(
        .list
      )
    }) %>% 
    bind_rows() %>% 
    tidyr::unnest(cols = -c(link, poll_options_text, poll_options_percent, video_url, video_duration)) %>%
    tidyr::extract(post_id, c("handle", "post_id"), '(\\w+)\\/(\\d+)') %>% 
    mutate(
      views = stringr::str_replace(views, "K", "00") %>%
        stringr::str_remove("\\.") %>%
        as.integer(),
      datetime = lubridate::as_datetime(datetime),
      image_url = stringr::str_extract(image_url, "(?<=url\\(').+(?=')"),
      handle = stringr::str_to_lower(handle)
    )
  
  return(df)
}

parse_telegram_user <- function(html_source) {
  
  xpaths <- list(
    handle = '//div[@class="tgme_channel_info_header_username"]/a',
    name = '//div[@class="tgme_channel_info_header_title"]',
    url = '//div[@class="tgme_channel_info_header_username"]/a/@href',
    subscriber_count = '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "subscribers"]/preceding-sibling::span',
    photos_count = '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "photos"]/preceding-sibling::span',
    videos_count = '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "videos"]/preceding-sibling::span',
    files_count = '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "files"]/preceding-sibling::span',
    links_count = '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "links"]/preceding-sibling::span'
  )
  
  map(xpaths, function(.xpath) {
    html_nodes(html_source, xpath = .xpath) %>% 
      html_text(trim = T) %>% {
        if (length(.) == 0) {
          list(NA_character_)
        } else list(.)
      }
  }) %>% 
    as_tibble() %>% 
    mutate(
      across(everything(), as.character),
      # across(ends_with('count'), as.integer),
      handle = stringr::str_remove(handle, '@') %>% stringr::str_to_lower()
    ) %>% 
    return()
  # 
  # html_source %>%
  #   map(function(.x) {
  #     .list <- 
  #     
  #     as_tibble(
  #       .list
  #     )
  #   }) %>% 
  #   return()
}
