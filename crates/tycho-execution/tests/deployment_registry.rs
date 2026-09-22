use std::{fs, path::PathBuf};

use serde_json::{Map, Value};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("tycho-execution must live under crates/")
        .to_path_buf()
}

fn read_json(path: &str) -> Value {
    let full_path = repository_root().join(path);
    let content = fs::read_to_string(&full_path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", full_path.display()));
    serde_json::from_str(&content)
        .unwrap_or_else(|error| panic!("failed to parse {}: {error}", full_path.display()))
}

fn object_at<'a>(value: &'a Value, key: &str) -> &'a Map<String, Value> {
    value[key]
        .as_object()
        .unwrap_or_else(|| panic!("{key} must be a JSON object"))
}

#[test]
fn active_registry_matches_runtime_address_configs() {
    let registry = read_json("crates/tycho-execution/config/deployment_registry.json");
    let routers = read_json("crates/tycho-execution/config/router_addresses.json");
    let executors = read_json("crates/tycho-execution/config/executor_addresses.json");
    let chains = object_at(&registry, "chains");
    let routers = routers
        .as_object()
        .expect("router config must be a JSON object");

    assert_eq!(registry["schema_version"], 1);
    assert_eq!(registry["designation"], "Fynd License 1.0");
    assert_eq!(registry["legal_contact"], "legal@propellerheads.xyz");
    assert_eq!(chains.len(), routers.len());

    for (chain_name, router_address) in routers {
        let chain = &chains[chain_name];
        assert_eq!(chain["status"], "active", "{chain_name} status");
        assert!(chain.get("effective_at").is_some(), "{chain_name} effective_at");
        assert!(
            chain
                .get("notice_published_at")
                .is_some(),
            "{chain_name} notice"
        );
        assert!(
            chain
                .get("migration_deadline")
                .is_some(),
            "{chain_name} deadline"
        );
        assert_eq!(chain["router"]["address"], *router_address, "{chain_name} router");
        assert_eq!(chain["executors"], executors[chain_name], "{chain_name} executors");
        assert_eq!(chain["superseded"][0]["status"], "superseded");

        let fee_calculator = chain["fee_calculator"]["address"]
            .as_str()
            .unwrap_or_else(|| panic!("{chain_name} fee calculator must be a string"));
        assert!(fee_calculator.starts_with("0x") && fee_calculator.len() == 42);
    }
}

#[test]
fn registry_addresses_are_visible_in_contract_address_docs() {
    let registry = read_json("crates/tycho-execution/config/deployment_registry.json");
    let docs_path = repository_root().join("docs/for-solvers/execution/contract-addresses.md");
    let docs = fs::read_to_string(&docs_path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", docs_path.display()));

    assert!(docs.contains("deployment_registry.json"));
    assert!(docs.contains("legal@propellerheads.xyz"));
    assert!(docs.contains("1 September 2026"));

    for (chain_name, chain) in object_at(&registry, "chains") {
        let router = chain["router"]["address"]
            .as_str()
            .unwrap();
        let fee_calculator = chain["fee_calculator"]["address"]
            .as_str()
            .unwrap();
        assert!(docs.contains(router), "docs missing {chain_name} router {router}");
        assert!(
            docs.contains(fee_calculator),
            "docs missing {chain_name} FeeCalculator {fee_calculator}"
        );

        for address in object_at(chain, "executors").values() {
            let address = address.as_str().unwrap();
            assert!(docs.contains(address), "docs missing {chain_name} executor {address}");
        }
    }
}
