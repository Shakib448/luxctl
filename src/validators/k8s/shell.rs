use crate::validators::k8s::executor::KubernetesExecutor;
use std::io::{self, Write};

pub async fn run_shell() -> Result<(), kube::Error> {
    println!("luxctl k8s shell — type 'help' for commands, 'exit' to quit.");
    println!("Start with: use <kubeconfig_path>  (or 'use default' for ~/.kube/config)");

    let mut executor: Option<KubernetesExecutor> = None;

    loop {
        print!("luxctl> ");
        io::stdout().flush().ok();

        let mut line = String::new();
        if io::stdin().read_line(&mut line).is_err() {
            break;
        }

        let input = line.trim();

        if input.is_empty() {
            continue;
        }

        if input == "exit" || input == "quit" {
            println!("bye");
            break;
        }

        let parts: Vec<&str> = input.split_whitespace().collect();

        // Handle 'use' command before requiring an executor
        const DEFAULT: &str = "default";
        if parts[0] == "use" {
            let path = parts.get(1).unwrap_or(&DEFAULT);
            let kubeconfig = if path == &DEFAULT {
                None
            } else {
                Some(path.to_string())
            };

            match KubernetesExecutor::new(kubeconfig, None).await {
                Ok(ex) => {
                    println!("Connected.");
                    executor = Some(ex);
                }
                Err(e) => eprintln!("Failed to connect: {}", e),
            }
            continue;
        }

        // Everything else needs an executor
        let exec = match &executor {
            Some(e) => e,
            None => {
                eprintln!("No cluster connected. Type 'use <kubeconfig>' or 'use default'");
                continue;
            }
        };

        if let Err(e) = dispatch(exec, &parts).await {
            eprintln!("Error: {}", e);
        }
    }

    Ok(())
}

async fn dispatch(executor: &KubernetesExecutor, parts: &[&str]) -> Result<(), kube::Error> {
    let (args, override_ns) = parse_ns(parts);

    let exec = match override_ns {
        Some(ns) => executor.with_namespace(ns.to_string()),
        None => executor.clone(),
    };

    match args.as_slice() {
        ["help"] => {
            print_help();
            Ok(())
        }

        ["ns"] | ["namespace"] => {
            println!("Current namespace: {:?}", executor.namespace());
            Ok(())
        }

        ["get", "pods"] | ["get", "po"] => exec.list_pods().await,
        ["get", "deployments"] | ["get", "deploy"] => exec.list_deployments().await,
        ["get", "svc"] | ["get", "services"] => exec.list_svc().await,

        ["logs", pod] => exec.logs_pod(pod, false).await,
        ["logs", pod, "-f"] => exec.logs_pod(pod, true).await,

        _ => {
            eprintln!("Unknown command: '{}'. Type 'help'.", parts.join(" "));
            Ok(())
        }
    }
}

fn parse_ns<'a>(parts: &'a [&'a str]) -> (Vec<&'a str>, Option<&'a str>) {
    let mut args = Vec::with_capacity(parts.len());
    let mut ns = None;
    let mut i = 0;

    while i < parts.len() {
        if (parts[i] == "-n" || parts[i] == "--namespace") && i + 1 < parts.len() {
            ns = Some(parts[i + 1]);
            i += 2;
        } else {
            args.push(parts[i]);
            i += 1;
        }
    }

    (args, ns)
}

fn print_help() {
    println!("Available commands:");
    println!("  use <path> | use default     connect to cluster");
    println!("  get pods|deployments|svc [-n <ns>]");
    println!("  logs <pod> [-f] [-n <ns>]");
    println!("  ns                           show current namespace");
    println!("  help");
    println!("  exit | quit");
}
