#!/bin/bash

rm inventory

echo '[upstairs]' >> inventory
terraform output -raw upstairs_ip >> inventory
echo '' >> inventory
echo '' >> inventory

echo '[downstairs]' >> inventory
terraform output -json downstairs_ips 2>&1 | jq -r .[0] >> inventory
terraform output -json downstairs_ips 2>&1 | jq -r .[1] >> inventory
terraform output -json downstairs_ips 2>&1 | jq -r .[2] >> inventory

echo '' >> inventory

