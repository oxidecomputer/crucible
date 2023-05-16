// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company
use vergen::EmitBuilder;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    EmitBuilder::builder()
        .all_cargo()
        .all_git()
        .all_rustc()
        .emit()
        .unwrap();
}
