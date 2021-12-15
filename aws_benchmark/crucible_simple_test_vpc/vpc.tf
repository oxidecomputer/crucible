data "aws_availability_zones" "available" {
  state = "available"
}

resource "random_string" "random" {
  length  = 32
  special = false
  lower   = true
  upper   = true
  number  = true
}

module "vpc" {
  source = "terraform-aws-modules/vpc/aws"

  name = "crucible-benchmark-vpc-${random_string.random.id}"
  cidr = "10.0.0.0/16"

  azs             = slice(data.aws_availability_zones.available.names, 0, 3)
  private_subnets = ["10.0.103.0/24", "10.0.104.0/24", "10.0.105.0/24"]
  public_subnets  = ["10.0.100.0/24", "10.0.101.0/24", "10.0.102.0/24"]

  enable_nat_gateway   = true
  enable_vpn_gateway   = false
  enable_dns_hostnames = true
  enable_dns_support   = true

  # TODO: IPv6 testing
  #enable_ipv6                                    = true
  #assign_ipv6_address_on_creation                = true
  #private_subnet_assign_ipv6_address_on_creation = false
  #public_subnet_ipv6_prefixes                    = [0, 1]
  #private_subnet_ipv6_prefixes                   = [2, 3]
  #database_subnet_ipv6_prefixes                  = [4, 5]

  tags = {
    Terraform   = "true"
    Environment = "dev"
  }
}

resource "aws_route53_zone" "private" {
  name = "private.lan"

  vpc {
    vpc_id = module.vpc.vpc_id
  }
}

