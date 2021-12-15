module "upstairs_sg" {
  source = "terraform-aws-modules/security-group/aws"

  name   = "crucible-upstairs"
  vpc_id = module.vpc.vpc_id

  ingress_with_cidr_blocks = [
    {
      rule        = "ssh-tcp",
      cidr_blocks = "0.0.0.0/0"
    },
  ]

  egress_with_cidr_blocks = [
    {
      rule        = "all-all",
      cidr_blocks = "0.0.0.0/0",
    }
  ]
}

module "downstairs_sg" {
  source = "terraform-aws-modules/security-group/aws"

  name   = "crucible-downstairs"
  vpc_id = module.vpc.vpc_id

  ingress_with_cidr_blocks = [
    {
      rule        = "ssh-tcp",
      cidr_blocks = "0.0.0.0/0"
    },
    {
      from_port                = 3801,
      to_port                  = 3801,
      protocol                 = "tcp",
      description              = "crucible traffic",
      source_security_group_id = module.upstairs_sg.security_group_id,
      cidr_blocks              = module.vpc.vpc_cidr_block
    },
  ]

  egress_with_cidr_blocks = [
    {
      rule        = "all-all",
      cidr_blocks = "0.0.0.0/0",
    }
  ]
}
