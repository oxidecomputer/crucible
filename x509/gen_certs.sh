#!/bin/bash
set -e

#cfssl print-defaults config > ca-config.json
#cfssl print-defaults csr > ca-csr.json
cat > ca-config.json <<EOF
{
    "signing": {
        "default": {
            "expiry": "168h"
        },
        "profiles": {
            "www": {
                "expiry": "8760h",
                "usages": [
                    "signing",
                    "key encipherment",
                    "server auth"
                ]
            },
            "client": {
                "expiry": "8760h",
                "usages": [
                    "signing",
                    "key encipherment",
                    "client auth"
                ]
            }
        }
    }
}
EOF

cat > ca-csr.json <<EOF
{
    "CN": "crucible trust root",
    "hosts": [
    ],
    "key": {
        "algo": "rsa",
        "size": 2048
    },
    "names": [
        {
            "C": "CA",
            "ST": "ON",
            "L": "Fonthill"
        }
    ]
}
EOF

cfssl gencert -initca ca-csr.json | cfssljson -bare ca -

for i in {0..2};
do
    echo ${i}
    cat > downstairs${i}-csr.json <<EOF
{
    "CN": "downstairs${i}",
    "hosts": [
        "downstairs${i}"
    ],
    "key": {
        "algo": "rsa",
        "size": 2048
    }
}
EOF

    cat downstairs${i}-csr.json

    cfssl genkey downstairs${i}-csr.json | cfssljson -bare downstairs${i} -
    cfssl sign -ca ca.pem -ca-key ca-key.pem downstairs${i}.csr | cfssljson -bare downstairs${i} -
done

cat > upstairs-csr.json <<EOF
{
    "CN": "upstairs",
    "hosts": [
        "upstairs"
    ],
    "key": {
        "algo": "rsa",
        "size": 2048
    }
}
EOF

cat upstairs-csr.json

cfssl genkey upstairs-csr.json | cfssljson -bare upstairs -
cfssl sign -ca ca.pem -ca-key ca-key.pem upstairs.csr | cfssljson -bare upstairs -

# self signed should be rejected
cat > selfsigned-csr.json <<EOF
{
    "CN": "self signed",
    "hosts": [
    ],
    "key": {
        "algo": "rsa",
        "size": 2048
    },
    "names": [
        {
            "C": "CA",
            "ST": "ON",
            "L": "Fonthill"
        }
    ]
}
EOF

cfssl gencert -initca selfsigned-csr.json | cfssljson -bare selfsigned -

cat > bad-upstairs-csr.json <<EOF
{
    "CN": "badupstairs",
    "hosts": [
        "badupstairs"
    ],
    "key": {
        "algo": "rsa",
        "size": 2048
    }
}
EOF

cat bad-upstairs-csr.json

cfssl genkey bad-upstairs-csr.json | cfssljson -bare bad-upstairs -
cfssl sign -ca selfsigned.pem -ca-key selfsigned-key.pem bad-upstairs.csr | cfssljson -bare bad-upstairs -
