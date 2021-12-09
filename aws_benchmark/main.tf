variable "ami_id" {
  type = string
}

variable "instance_type" {
  type = string
}

variable "user_data_path" {
  type = string
}

module "crucible_simple_test" {
  source         = "./crucible_simple_test_vpc/"
  ami_id         = var.ami_id
  instance_type  = var.instance_type
  user_data_path = var.user_data_path
}

output "upstairs_ip" {
  value = module.crucible_simple_test.upstairs_ip
}
output "upstairs_id" {
  value = module.crucible_simple_test.upstairs_id
}

output "downstairs_ips" {
  value = module.crucible_simple_test.downstairs_ips
}
output "downstairs_ids" {
  value = module.crucible_simple_test.downstairs_ids
}

