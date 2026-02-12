terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ------------------------------------------------------------------------------
# S3 Bucket - Data Lake
# ------------------------------------------------------------------------------

resource "aws_s3_bucket" "pipeline" {
  bucket = var.bucket_name
}

# ------------------------------------------------------------------------------
# Glue Catalog Database (used by Athena)
# ------------------------------------------------------------------------------

resource "aws_glue_catalog_database" "portfolio" {
  name = "portfolio"
}

# ------------------------------------------------------------------------------
# Glue Table: stocks
# ------------------------------------------------------------------------------

resource "aws_glue_catalog_table" "stocks" {
  database_name = aws_glue_catalog_database.portfolio.name
  name          = "stocks"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "json"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_name}/stocks/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "symbol"
      type = "string"
    }
    columns {
      name = "price"
      type = "double"
    }
    columns {
      name = "previous_close"
      type = "double"
    }
    columns {
      name = "change"
      type = "double"
    }
    columns {
      name = "change_percent"
      type = "double"
    }
    columns {
      name = "volume"
      type = "bigint"
    }
    columns {
      name = "trading_day"
      type = "string"
    }
    columns {
      name = "extracted_at"
      type = "string"
    }
  }
}

# ------------------------------------------------------------------------------
# Glue Table: crypto
# ------------------------------------------------------------------------------

resource "aws_glue_catalog_table" "crypto" {
  database_name = aws_glue_catalog_database.portfolio.name
  name          = "crypto"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "json"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_name}/crypto/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "symbol"
      type = "string"
    }
    columns {
      name = "price"
      type = "double"
    }
    columns {
      name = "currency"
      type = "string"
    }
    columns {
      name = "timestamp"
      type = "string"
    }
    columns {
      name = "extracted_at"
      type = "string"
    }
  }
}

# ------------------------------------------------------------------------------
# Glue Table: weather
# ------------------------------------------------------------------------------

resource "aws_glue_catalog_table" "weather" {
  database_name = aws_glue_catalog_database.portfolio.name
  name          = "weather"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "json"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_name}/raw/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "city"
      type = "string"
    }
    columns {
      name = "temperature"
      type = "double"
    }
    columns {
      name = "feels_like"
      type = "double"
    }
    columns {
      name = "humidity"
      type = "int"
    }
    columns {
      name = "weather"
      type = "string"
    }
    columns {
      name = "wind_speed"
      type = "double"
    }
    columns {
      name = "timestamp"
      type = "string"
    }
  }
}

# ------------------------------------------------------------------------------
# Athena Workgroup
# ------------------------------------------------------------------------------

resource "aws_athena_workgroup" "portfolio" {
  name = "portfolio-workgroup"

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_name}/athena-results/"
    }
  }
}
