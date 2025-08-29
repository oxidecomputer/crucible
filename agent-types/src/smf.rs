// Copyright 2025 Oxide Computer Company

use crucible_smf::scf_type_t;

pub struct SmfProperty<'a> {
    pub name: &'a str,
    pub typ: scf_type_t,
    pub val: String,
}
