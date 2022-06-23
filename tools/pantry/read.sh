#!/bin/bash

curl -sSf -X GET http://localhost:9999/crucible/pantry/0/volume/hello/get/0 |
    od -c
