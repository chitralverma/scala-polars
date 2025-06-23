package com.github.chitralverma.polars.config

object TableFormats extends Enumeration {
  type TableFormat = Value

  val NOTHING, ASCII_FULL, ASCII_FULL_CONDENSED, ASCII_NO_BORDERS, ASCII_BORDERS_ONLY,
      ASCII_BORDERS_ONLY_CONDENSED, ASCII_HORIZONTAL_ONLY, ASCII_MARKDOWN, UTF8_FULL,
      UTF8_FULL_CONDENSED, UTF8_NO_BORDERS, UTF8_BORDERS_ONLY, UTF8_HORIZONTAL_ONLY = Value
}

object UniqueKeepStrategies extends Enumeration {
  type UniqueKeepStrategy = Value

  val first, last, any, none = Value
}
