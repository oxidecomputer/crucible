#!/bin/bash

curl -d '
	{
		"gen": 1000,
		"read_only":false,
		"volume_construction_request": {
			"type":"region",
			"block_size": 512,
			"gen": 1000,
			"opts": {
				"lossy": false,
				"target": [
					"127.0.0.1:3801",
					"127.0.0.1:3802",
					"127.0.0.1:3803"
				]
			}
		}
	}' -X PUT http://localhost:9999/crucible/pantry/0/volume/hello

