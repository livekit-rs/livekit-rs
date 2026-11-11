//! `livecloud` / `livekit-server` entry point. Parses the official CLI contract
//! (lk-config), then runs the requested action. Errors are printed to stdout and
//! exit with status 1, like upstream `main`.

mod cli_nodes;
mod https;
mod interfaces;
mod logging;
mod metrics;
mod profiling;
mod server;
mod turn;

use std::ffi::OsString;
use std::process::ExitCode;

use lk_config::{EntryAction, Env, ProgramInfo};

// Delegates to System; disabled sampler unless the real --memprofile owner starts.
#[global_allocator]
static PROFILE_ALLOCATOR: rpprof::alloc::AllocProfiler = rpprof::alloc::AllocProfiler::system();

/// Upstream release this build tracks (SPEC-0001 §3.1.1).
pub const COMPAT_VERSION: &str = "1.13.10";

pub fn run_cli() -> ExitCode {
    let args: Vec<OsString> = std::env::args_os().collect();
    let binary_name = args
        .first()
        .and_then(|a| std::path::Path::new(a).file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("livecloud")
        .to_string();
    let info = ProgramInfo {
        binary_name,
        product_version: env!("CARGO_PKG_VERSION").to_string(),
        compat_version: COMPAT_VERSION,
    };
    let env = Env::from_process();

    let action = match lk_config::parse(args, &env, &info) {
        Ok(a) => a,
        Err(e) => {
            use std::io::Write;
            let mut diagnostic = e.stdout_bytes();
            diagnostic.push(b'\n');
            if e.diagnostic_to_stderr() {
                let _ = std::io::stderr().lock().write_all(&diagnostic);
            } else {
                let _ = std::io::stdout().lock().write_all(&diagnostic);
            }
            return ExitCode::from(e.exit_code());
        }
    };

    match action {
        EntryAction::PrintBytes {
            stdout,
            stderr,
            exit_code,
        } => {
            use std::io::Write;
            let _ = std::io::stdout().lock().write_all(&stdout);
            let _ = std::io::stderr().lock().write_all(&stderr);
            ExitCode::from(exit_code.clamp(0, 255) as u8)
        }
        EntryAction::PrintHelp {
            text,
            to_stderr,
            exit_code,
        } => {
            let (stdout, stderr) = lk_config::help_output(&text, to_stderr);
            print!("{stdout}");
            eprint!("{stderr}");
            ExitCode::from(exit_code.clamp(0, 255) as u8)
        }
        EntryAction::PrintVersion(line) => {
            println!("{line}");
            ExitCode::SUCCESS
        }
        EntryAction::GenerateKeys => {
            print!("{}", lk_config::generate_keys_output());
            ExitCode::SUCCESS
        }
        EntryAction::Ports(cfg) => {
            print!("{}", lk_config::ports_output(&cfg));
            ExitCode::SUCCESS
        }
        EntryAction::ListNodes(cfg) => {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    println!("could not start runtime: {error}");
                    return ExitCode::from(1);
                }
            };
            match runtime.block_on(cli_nodes::list_nodes(&cfg)) {
                Ok(output) => {
                    print!("{output}");
                    ExitCode::SUCCESS
                }
                Err(cli_nodes::NodeListError::FatalNilStats) => {
                    eprintln!("{}", cli_nodes::NodeListError::FatalNilStats);
                    ExitCode::from(2)
                }
                Err(error) => {
                    println!("{error}");
                    ExitCode::from(1)
                }
            }
        }
        EntryAction::CreateJoinToken {
            config,
            room,
            identity,
            recorder,
        } => match server::create_join_token(&config, &room, &identity, recorder) {
            Ok(token) => {
                println!("Token: {token}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                println!("{e}");
                ExitCode::from(1)
            }
        },
        EntryAction::RunServer(cfg) => {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    println!("could not start runtime: {e}");
                    return ExitCode::from(1);
                }
            };
            match runtime.block_on(server::run(*cfg)) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    println!("{e}");
                    ExitCode::from(1)
                }
            }
        }
    }
}

#[cfg(test)]
mod cli_redis_address_tests;
