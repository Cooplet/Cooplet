use std::{error::Error, fmt::Write, process::ExitCode};

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
            eprintln!("{}", format_error(error.as_ref()));
            ExitCode::FAILURE
        }
    }
}

fn format_error(error: &(dyn Error + 'static)) -> String {
    let mut message = error.to_string();
    if let Some(cooplet_client::ClientError::Problem(problem)) = error.downcast_ref() {
        for (field, value) in problem.recovery_fields() {
            let _ = write!(message, " {field}={value}");
        }
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;
    use cooplet_client::ClientError;
    use cooplet_protocol::{ErrorCode, ProblemDetails};

    #[test]
    fn thread_errors_show_typed_recovery_fields() {
        let conflict = ProblemDetails::new(ErrorCode::PinConflict, "private detail")
            .with_extension("current_version", 3);
        assert_eq!(
            format_error(&ClientError::Problem(Box::new(conflict))),
            "API problem (409): PinConflict current_version=3",
        );

        let expired = ProblemDetails::new(ErrorCode::ResyncRequired, "private detail")
            .with_extension("floor_seq", 4)
            .with_extension("head_seq", 12)
            .with_extension("existing_answer_id", "invalid")
            .with_extension("unexpected", "private detail");
        assert_eq!(
            format_error(&ClientError::Problem(Box::new(expired))),
            "API problem (410): ResyncRequired floor_seq=4 head_seq=12",
        );
    }
}
