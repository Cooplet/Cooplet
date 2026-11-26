use std::process::ExitCode;

fn main() -> ExitCode {
    if rustls::crypto::ring::default_provider()
        .install_default()
        .is_err()
    {
        eprintln!("Failed to install the Rustls ring crypto provider");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::{ExitCode, main};
    use rustls::crypto::CryptoProvider;

    #[test]
    fn startup_installs_the_process_crypto_provider() {
        assert!(CryptoProvider::get_default().is_none());
        assert_eq!(main(), ExitCode::SUCCESS);
        assert!(CryptoProvider::get_default().is_some());
    }
}
