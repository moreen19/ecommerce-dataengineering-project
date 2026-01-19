terraform {
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

resource "aws_glue_catalog_database" "default_db" {
  name = "default" 
}

terraform {
  backend "s3" {
    bucket         = "moeandy-terraform-state" # The name of the NEW bucket
    key            = "state/terraform.tfstate"
    region         = "us-east-2"
    encrypt        = true
  }
}