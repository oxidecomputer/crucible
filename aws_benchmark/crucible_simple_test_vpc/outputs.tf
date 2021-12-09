output "upstairs_ip" {
  value = module.upstairs.public_ip
}
output "upstairs_id" {
  value = module.upstairs.id
}

output "downstairs_ips" {
  value = [
    module.downstairs["0"].public_ip,
    module.downstairs["1"].public_ip,
    module.downstairs["2"].public_ip,
   ]
}
output "downstairs_ids" {
  value = [
    module.downstairs["0"].id,
    module.downstairs["1"].id,
    module.downstairs["2"].id,
   ]
}
