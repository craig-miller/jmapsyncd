use clap::Parser;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = jmapsyncd::args::Args::parse();
    let command = args.command.unwrap_or_default();
    jmapsyncd::logging::init(args.log_level);

    let overrides: jmapsyncd::config::Overrides = args.overrides.into();
    let config = match jmapsyncd::config::Config::load(args.config_file.as_deref(), &overrides) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e:#}");
            return std::process::ExitCode::from(1);
        }
    };

    match command {
        jmapsyncd::args::Command::Sync { account } => {
            match account {
                None => log::info!("syncing all accounts"),
                Some(account) => log::info!("syncing account: {:?}", account),
            }
            if args.dry_run {
                log::info!("dry-run mode, no changes will be applied");
            }
            // Sync one-shot handled in Phase C.4; stub for now.
            std::process::ExitCode::SUCCESS
        }
        jmapsyncd::args::Command::Daemon => {
            if args.dry_run {
                log::warn!("--dry-run has no effect in daemon mode");
            }
            match jmapsyncd::daemon::run_daemon(config).await {
                Ok(()) => std::process::ExitCode::SUCCESS,
                Err(e) => {
                    log::error!("daemon exited with error: {e:#}");
                    std::process::ExitCode::from(1)
                }
            }
        }
    }
}
