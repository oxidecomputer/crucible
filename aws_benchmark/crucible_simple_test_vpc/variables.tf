variable "ami_id" {
  type = string
}

variable "instance_type" {
  type = string
}

variable "user_data_path" {
  type    = string
  default = null
}
