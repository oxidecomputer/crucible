# pantest - Crucible Pantry Test Tool

A CLI tool for testing the Crucible pantry outside of the full
control plane. Similar to how crutest exercises the upstairs
directly, pantest exercises the pantry through its HTTP API.

## Usage

Requires a pantry and (for most commands) a dsc server running
separately.

```
# Get pantry status
pantest -p 127.0.0.1:9999 status

# Attach a volume (builds VCR from dsc region info)
pantest -p 127.0.0.1:9999 attach --dsc 127.0.0.1:9998

# Attach with a specific volume ID and generation number
pantest -p 127.0.0.1:9999 attach --dsc 127.0.0.1:9998 \
    --volume-id <uuid> --generation 5

# Get volume status
pantest -p 127.0.0.1:9999 volume-status <volume-uuid>

# Detach a volume
pantest -p 127.0.0.1:9999 detach <volume-uuid>
```

The `attach` command queries dsc for region info, port numbers,
and read-only status, then constructs a
VolumeConstructionRequest and sends it to the pantry. A volume
ID is auto-generated if not provided.
