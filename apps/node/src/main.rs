use std::process::ExitCode;

use clap::Parser;

mod cli;
mod logging;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = match cli::Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let code = if error.use_stderr() {
                ExitCode::from(2)
            } else {
                ExitCode::SUCCESS
            };
            let _ = error.print();
            return code;
        }
    };
    if rustls::crypto::ring::default_provider()
        .install_default()
        .is_err()
    {
        eprintln!("Failed to install the Rustls ring crypto provider");
        return ExitCode::FAILURE;
    }

    logging::install();
    match cli.run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
