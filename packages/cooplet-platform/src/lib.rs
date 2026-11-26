//! Operating-system primitives shared by Cooplet processes.

#[cfg(test)]
mod tests {
    #[test]
    fn crate_is_registered() {
        assert_eq!(env!("CARGO_PKG_NAME"), "cooplet-platform");
    }
}
