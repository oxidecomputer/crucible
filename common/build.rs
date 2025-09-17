// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company
use vergen::CargoBuilder;
use vergen::Emitter;
use vergen::RustcBuilder;
use vergen_git2::Git2Builder;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    let cargo = CargoBuilder::all_cargo().expect("valid builder");
    let rustc = RustcBuilder::all_rustc().expect("valid builder");
    let git = Git2Builder::all_git().expect("valid builder");
    Emitter::default()
        .add_instructions(&cargo)
        .expect("valid instructions")
        .add_instructions(&git)
        .expect("valid instructions")
        .add_instructions(&rustc)
        .expect("valid instructions")
        .emit()
        .expect("emitted version info");
}
