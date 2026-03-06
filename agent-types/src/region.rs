// Copyright 2026 Oxide Computer Company

pub use crucible_agent_types_versions::latest::region::*;

use std::path::Path;

use crucible_smf::scf_type_t::*;

use crate::smf::SmfProperty;

/// Given a root directory, return a list of SMF properties to ensure for the
/// corresponding running downstairs instance.
pub fn region_smf_properties<'a>(
    region: &'a Region,
    dir: &Path,
) -> Vec<SmfProperty<'a>> {
    let mut results = vec![
        SmfProperty {
            name: "directory",
            typ: SCF_TYPE_ASTRING,
            val: dir.to_str().unwrap().to_string(),
        },
        SmfProperty {
            name: "port",
            typ: SCF_TYPE_COUNT,
            val: region.port_number.to_string(),
        },
    ];

    if region.cert_pem.is_some() {
        let mut path = dir.to_path_buf();
        path.push("cert.pem");
        let path = path.into_os_string().into_string().unwrap();

        results.push(SmfProperty {
            name: "cert_pem_path",
            typ: SCF_TYPE_ASTRING,
            val: path,
        });
    }

    if region.key_pem.is_some() {
        let mut path = dir.to_path_buf();
        path.push("key.pem");
        let path = path.into_os_string().into_string().unwrap();

        results.push(SmfProperty {
            name: "key_pem_path",
            typ: SCF_TYPE_ASTRING,
            val: path,
        });
    }

    if region.root_pem.is_some() {
        let mut path = dir.to_path_buf();
        path.push("root.pem");
        let path = path.into_os_string().into_string().unwrap();

        results.push(SmfProperty {
            name: "root_pem_path",
            typ: SCF_TYPE_ASTRING,
            val: path,
        });
    }

    results
}

/// Check if a `CreateRegion` request mismatches an existing `Region`. Returns
/// `Some` with a description of the mismatch, or `None` if they match.
pub fn create_region_mismatch(
    create: &CreateRegion,
    r: &Region,
) -> Option<String> {
    if create.block_size != r.block_size {
        Some(format!(
            "block size {} instead of requested {}",
            create.block_size, r.block_size
        ))
    } else if create.extent_size != r.extent_size {
        Some(format!(
            "extent size {} instead of requested {}",
            create.extent_size, r.extent_size
        ))
    } else if create.extent_count != r.extent_count {
        Some(format!(
            "extent count {} instead of requested {}",
            create.extent_count, r.extent_count
        ))
    } else if create.encrypted != r.encrypted {
        Some(format!(
            "encrypted {} instead of requested {}",
            create.encrypted, r.encrypted
        ))
    } else if create.cert_pem != r.cert_pem {
        Some(format!(
            "cert_pem {:?} instead of requested {:?}",
            create.cert_pem, r.cert_pem
        ))
    } else if create.key_pem != r.key_pem {
        Some(format!(
            "key_pem {:?} instead of requested {:?}",
            create.key_pem, r.key_pem
        ))
    } else if create.root_pem != r.root_pem {
        Some(format!(
            "root_pem {:?} instead of requested {:?}",
            create.root_pem, r.root_pem
        ))
    } else if create.source != r.source {
        Some(format!(
            "source {:?} instead of requested {:?}",
            create.source, r.source
        ))
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic() {
        let r = Region {
            id: RegionId("abc".to_string()),
            port_number: 1701,
            state: State::Requested,
            block_size: 4096,
            extent_size: 4096,
            extent_count: 100,
            encrypted: false,
            cert_pem: None,
            key_pem: None,
            root_pem: None,
            source: None,
            read_only: false,
        };

        let s = serde_json::to_string(&r).expect("serialised");
        println!("{}", s);

        let recons: Region = serde_json::from_str(&s).expect("deserialised");

        assert_eq!(r, recons);
    }
}
