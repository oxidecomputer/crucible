resource "aws_key_pair" "temp" {
  key_name_prefix = "crucible-benchmarking-"
  public_key      = file(pathexpand("~/.ssh/id_ed25519.pub"))
}

module "upstairs" {
  source  = "terraform-aws-modules/ec2-instance/aws"
  version = "~> 6.0"

  name = "upstairs"

  ami                         = var.ami_id
  instance_type               = var.instance_type
  key_name                    = aws_key_pair.temp.id
  monitoring                  = true
  vpc_security_group_ids      = [module.upstairs_sg.security_group_id]
  associate_public_ip_address = true
  subnet_id                   = module.vpc.public_subnets[0]
  user_data                   = var.user_data_path != null ? file("${path.module}/${var.user_data_path}") : null

  root_block_device = [
    {
      name        = "/dev/sda1"
      volume_type = "gp3"
      iops        = 5000,
      volume_size = 50
    },
  ]

  tags = {
    Terraform   = "true"
    Environment = "dev"
  }
}

resource "aws_route53_record" "upstairs" {
  zone_id = aws_route53_zone.private.zone_id
  name    = "upstairs.${aws_route53_zone.private.name}"
  type    = "CNAME"
  ttl     = "60"
  records = [module.upstairs.private_dns]
}

resource "aws_ebs_volume" "downstairs" {
  for_each = toset(["0", "1", "2"])

  availability_zone = module.vpc.azs[each.key]

  # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-volume-types.html
  type = "io1"
  size = 100
  iops = 5000
}

resource "aws_volume_attachment" "downstairs" {
  device_name = "/dev/sdh"
  volume_id   = aws_ebs_volume.downstairs["${each.key}"].id
  instance_id = module.downstairs["${each.key}"].id

  for_each = toset(["0", "1", "2"])
}

module "downstairs" {
  source  = "terraform-aws-modules/ec2-instance/aws"
  version = "~> 6.0"

  for_each = toset(["0", "1", "2"])

  name = "downstairs-${each.key}"

  ami                         = var.ami_id
  instance_type               = var.instance_type
  key_name                    = aws_key_pair.temp.id
  monitoring                  = true
  vpc_security_group_ids      = [module.downstairs_sg.security_group_id]
  associate_public_ip_address = true
  subnet_id                   = module.vpc.public_subnets["${each.key}"]
  user_data                   = var.user_data_path != null ? file("${path.module}/${var.user_data_path}") : null

  root_block_device = [
    {
      name        = "/dev/sda1"
      volume_type = "gp3"
      iops        = 5000,
      volume_size = 50
    },
  ]

  tags = {
    Terraform   = "true"
    Environment = "dev"
  }
}

resource "aws_route53_record" "private" {
  for_each = toset(["0", "1", "2"])

  zone_id = aws_route53_zone.private.zone_id
  name    = "downstairs${each.key}.${aws_route53_zone.private.name}"
  type    = "CNAME"
  ttl     = "60"
  records = [module.downstairs["${each.key}"].private_dns]
}
