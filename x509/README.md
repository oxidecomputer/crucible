# How to enable TLS between the Upstairs and Downstairs #

Generate the appropriate X509 keys and certificates by running `make` in this
directory.

Create a downstairs like normal, but run with new arguments:

    cargo run --release -q -p crucible-downstairs -- \
        run -p "44100" -d "disks/d0/" \
        --cert-pem x509/downstairs0.pem \
        --key-pem x509/downstairs0-key.pem \
        --root-cert-pem x509/ca.pem
        
    cargo run --release -q -p crucible-downstairs -- \
        run -p "44101" -d "disks/d0/" \
        --cert-pem x509/downstairs1.pem \
        --key-pem x509/downstairs1-key.pem \
        --root-cert-pem x509/ca.pem
        
    cargo run --release -q -p crucible-downstairs -- \
        run -p "44102" -d "disks/d0/" \
        --cert-pem x509/downstairs2.pem \
        --key-pem x509/downstairs2-key.pem \
        --root-cert-pem x509/ca.pem

Run the upstairs with similar arguments:

    cargo run --bin=crucible-client -- \
        -q \
        -t 127.0.0.1:44100 \
        -t 127.0.0.1:44101 \
        -t 127.0.0.1:44102 \
        --key "HLyo7ZhAf/E9IdX2DDHPHJO0dLgrRxZabWiTlnoKZXc=" \
        --cert-pem x509/upstairs.pem \
        --key-pem x509/upstairs-key.pem \
        --root-cert-pem x509/ca.pem \
        --workload one

Note that the Downstairs at port 44100 is running with the key and certificate
for downstairs0, the Downstairs at port 44101 is running for downstairs1, etc.
This is important because the Upstairs will attempt connecting to the clients in
its list with server name "downstairs${i}", where `i` is the number of the
client in its list:

        -t 127.0.0.1:44100 => downstairs0
        -t 127.0.0.1:44101 => downstairs1
        -t 127.0.0.1:44102 => downstairs2

To test that certificates not signed by x509/ca.pem are rejected, try using
x509/selfsigned.pem and x509/selfsigned-key.pem in the client connection. The
client should fail with:

    error: InvalidCertificateData("invalid peer certificate: UnknownIssuer")

And the Downstairs should say:

    rejecting connection from 127.0.0.1:49462: Custom { kind: InvalidData, error: AlertReceived(BadCertificate) }

If you try to connect without certificates in the client, it should fail, and
the Downstairs should say:

    rejecting connection from 127.0.0.1:38116: Custom { kind: InvalidData, error: CorruptMessage }

