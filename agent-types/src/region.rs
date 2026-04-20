// Copyright 2026 Oxide Computer Company

pub use crucible_agent_types_versions::latest::region::*;

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

        let s = serde_json::to_string(&r).expect("serialise");
        println!("{}", s);

        let recons: Region = serde_json::from_str(&s).expect("deserialise");

        assert_eq!(r, recons);
    }
}
