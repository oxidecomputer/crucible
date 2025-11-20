// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::process::ExitCode;

use anyhow::{Context, anyhow};
use camino::Utf8PathBuf;
use clap::Parser;
use crucible_agent_api::*;
use crucible_downstairs_api::*;
use crucible_pantry_api::*;
use dropshot_api_manager::{Environment, ManagedApiConfig, ManagedApis};
use dropshot_api_manager_types::{
    ManagedApiMetadata, ValidationContext, Versions,
};
use openapiv3::OpenAPI;

pub fn environment() -> anyhow::Result<Environment> {
    // The workspace root is one level up from this crate's directory.
    let workspace_root = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();
    let env = Environment::new(
        // This is the command used to run the OpenAPI manager.
        "cargo xtask openapi",
        workspace_root,
        // This is the location within the workspace root where the OpenAPI
        // documents are stored.
        "openapi",
    )?;
    Ok(env)
}

/// The list of APIs managed by the OpenAPI manager.
pub fn all_apis() -> anyhow::Result<ManagedApis> {
    let apis = vec![
        ManagedApiConfig {
            ident: "crucible-agent",
            versions: Versions::Versioned {
                supported_versions: crucible_agent_api::supported_versions(),
            },
            title: "Crucible Agent",
            metadata: ManagedApiMetadata {
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                ..Default::default()
            },
            api_description: crucible_agent_api_mod::stub_api_description,
            extra_validation: None,
        },
        ManagedApiConfig {
            ident: "crucible-pantry",
            versions: Versions::Versioned {
                supported_versions: crucible_pantry_api::supported_versions(),
            },
            title: "Crucible Pantry",
            metadata: ManagedApiMetadata {
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                ..Default::default()
            },
            api_description: crucible_pantry_api_mod::stub_api_description,
            extra_validation: None,
        },
        ManagedApiConfig {
            ident: "downstairs-repair",
            versions: Versions::Versioned {
                supported_versions: crucible_downstairs_api::supported_versions(
                ),
            },
            title: "Downstairs Repair",
            metadata: ManagedApiMetadata {
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                ..Default::default()
            },
            api_description:
                crucible_downstairs_repair_api_mod::stub_api_description,
            extra_validation: None,
        },
    ];

    let apis = ManagedApis::new(apis)
        .context("error creating ManagedApis")?
        .with_unknown_apis(["crucible-control", "dsc-control"])
        .with_validation(validate);
    Ok(apis)
}

fn validate(doc: &OpenAPI, mut cx: ValidationContext<'_>) {
    let errors = openapi_lint::validate(doc);
    for error in errors {
        cx.report_error(anyhow!(error));
    }
}

fn main() -> anyhow::Result<ExitCode> {
    let app = dropshot_api_manager::App::parse();
    let env = environment()?;
    let apis = all_apis()?;

    Ok(app.exec(&env, &apis))
}

#[cfg(test)]
mod test {
    use dropshot_api_manager::test_util::check_apis_up_to_date;

    use super::*;

    // Also recommended: a test which ensures documents are up-to-date. The
    // OpenAPI manager comes with a helper function for this, called
    // `check_apis_up_to_date`.
    #[test]
    fn test_apis_up_to_date() -> anyhow::Result<ExitCode> {
        let env = environment()?;
        let apis = all_apis()?;

        let result = check_apis_up_to_date(&env, &apis)?;
        Ok(result.to_exit_code())
    }
}
