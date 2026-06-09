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
pantest -p 127.0.0.1:9999 -d 127.0.0.1:9998 attach

# Attach with a specific generation number
pantest -p 127.0.0.1:9999 -d 127.0.0.1:9998 attach --generation 5

# Get volume status
pantest -p 127.0.0.1:9999 volume-status <volume-uuid>

# Detach a volume
pantest -p 127.0.0.1:9999 detach <volume-uuid>
```

The `attach` command queries dsc for region info, port numbers,
and the volume UUID (from downstairs client ID 0), then
constructs a VolumeConstructionRequest and sends it to the
pantry.
