//! Shared persistence for the Node and Hub.

#[cfg(test)]
mod tests {
    #[test]
    fn crate_is_registered() {
        assert_eq!(env!("CARGO_PKG_NAME"), "cooplet-storage");
    }
}
