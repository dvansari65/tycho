//! Executor address to protocol name resolution.
use crate::executors_table::EXECUTORS;

/// Protocol names served by the executor at `address`, joined with `,`. Empty when unknown.
pub fn protocol_for(address: &[u8]) -> String {
    let needle = format!("0x{}", hex::encode(address));
    for (addr, protocols) in EXECUTORS {
        if *addr == needle {
            return protocols.join(",");
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_known_executor() {
        let addr = hex::decode("0017c84f2B3414514B67Bfc9a63830c8E0E690d0").unwrap();
        assert_eq!(protocol_for(&addr), "sushiswap_v2,uniswap_v2");
    }

    #[test]
    fn unknown_executor_is_empty() {
        assert_eq!(protocol_for(&[0u8; 20]), "");
    }

    #[test]
    fn table_addresses_are_lowercase_and_unique() {
        let mut seen = std::collections::HashSet::new();
        for (addr, protocols) in EXECUTORS {
            assert_eq!(*addr, addr.to_lowercase(), "{addr} not lowercase");
            assert_eq!(addr.len(), 42, "{addr} malformed");
            assert!(seen.insert(*addr), "{addr} duplicated");
            assert!(!protocols.is_empty(), "{addr} has no protocols");
        }
    }
}
