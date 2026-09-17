use std::{fs, process::Command};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::HostConfig;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HostGpu {
    pub identity: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HostInventory {
    pub detected_cpu_threads: u32,
    pub cpu_threads: u32,
    pub detected_memory_bytes: u64,
    pub memory_bytes: u64,
    pub max_concurrent_jobs: u32,
    pub gpus: Vec<HostGpu>,
    pub gpu_inventory_authoritative: bool,
    pub named_resources: Vec<String>,
}

#[derive(Debug, Error)]
pub enum InventoryError {
    #[error("cannot read /proc/meminfo: {0}")]
    ReadMemory(#[source] std::io::Error),
    #[error("cannot parse MemTotal from /proc/meminfo: {0}")]
    InvalidMemory(String),
}

pub fn discover_host_inventory(config: &HostConfig) -> Result<HostInventory, InventoryError> {
    let detected_cpu_threads = std::thread::available_parallelism()
        .ok()
        .and_then(|threads| u32::try_from(threads.get()).ok())
        .unwrap_or(1);
    let meminfo = fs::read_to_string("/proc/meminfo").map_err(InventoryError::ReadMemory)?;
    let detected_memory_bytes = parse_meminfo(&meminfo)?;
    let detected_gpus = if config.gpus.is_empty() && config.discover_gpus {
        discover_nvidia_gpus()
    } else {
        None
    };
    Ok(inventory_from(
        config,
        detected_cpu_threads,
        detected_memory_bytes,
        detected_gpus,
    ))
}

fn inventory_from(
    config: &HostConfig,
    detected_cpu_threads: u32,
    detected_memory_bytes: u64,
    detected_gpus: Option<Vec<HostGpu>>,
) -> HostInventory {
    let (gpus, gpu_inventory_authoritative) = if !config.gpus.is_empty() {
        (
            config
                .gpus
                .iter()
                .map(|identity| HostGpu {
                    identity: identity.clone(),
                    display_name: None,
                })
                .collect(),
            true,
        )
    } else if !config.discover_gpus {
        (Vec::new(), true)
    } else {
        match detected_gpus {
            Some(gpus) => (gpus, true),
            None => (Vec::new(), false),
        }
    };
    HostInventory {
        detected_cpu_threads,
        cpu_threads: config.cpu_threads.map_or(detected_cpu_threads, |limit| {
            detected_cpu_threads.min(limit)
        }),
        detected_memory_bytes,
        memory_bytes: config.memory_bytes.map_or(detected_memory_bytes, |limit| {
            detected_memory_bytes.min(limit)
        }),
        max_concurrent_jobs: config.max_concurrent_jobs,
        gpus,
        gpu_inventory_authoritative,
        named_resources: config.named_resources.clone(),
    }
}

fn parse_meminfo(input: &str) -> Result<u64, InventoryError> {
    let line = input
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))
        .ok_or_else(|| InventoryError::InvalidMemory("MemTotal is missing".into()))?;
    let mut fields = line.split_whitespace();
    let kilobytes = fields
        .next()
        .ok_or_else(|| InventoryError::InvalidMemory("MemTotal has no value".into()))?
        .parse::<u64>()
        .map_err(|error| InventoryError::InvalidMemory(error.to_string()))?;
    if fields.next() != Some("kB") {
        return Err(InventoryError::InvalidMemory(
            "MemTotal must be expressed in kB".into(),
        ));
    }
    kilobytes
        .checked_mul(1024)
        .ok_or_else(|| InventoryError::InvalidMemory("MemTotal overflows bytes".into()))
}

fn discover_nvidia_gpus() -> Option<Vec<HostGpu>> {
    let output = match Command::new("nvidia-smi")
        .args(["--query-gpu=uuid,name", "--format=csv,noheader,nounits"])
        .output()
    {
        Ok(output) if output.status.success() => output,
        Ok(output) => {
            tracing::warn!(status = %output.status, "NVIDIA GPU discovery failed");
            return None;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!("nvidia-smi is not installed; no NVIDIA GPUs discovered");
            return None;
        }
        Err(error) => {
            tracing::warn!(%error, "cannot run nvidia-smi for GPU discovery");
            return None;
        }
    };
    let Ok(output) = String::from_utf8(output.stdout) else {
        tracing::warn!("nvidia-smi returned non-UTF-8 output");
        return None;
    };
    parse_nvidia_smi(&output).or_else(|| {
        tracing::warn!("nvidia-smi returned an unsupported GPU inventory format");
        None
    })
}

fn parse_nvidia_smi(input: &str) -> Option<Vec<HostGpu>> {
    input
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let (identity, display_name) = line.split_once(',')?;
            let identity = identity.trim();
            if identity.is_empty() {
                return None;
            }
            let display_name = display_name.trim();
            Some(HostGpu {
                identity: identity.into(),
                display_name: (!display_name.is_empty()).then(|| display_name.into()),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_linux_memory() -> Result<(), InventoryError> {
        assert_eq!(parse_meminfo("MemTotal: 16384 kB\n")?, 16_777_216);
        assert!(parse_meminfo("MemFree: 1 kB\n").is_err());
        Ok(())
    }

    #[test]
    fn parses_nvidia_inventory() {
        let parsed = parse_nvidia_smi("GPU-one, RTX 4090\nGPU-two, RTX 3090\n");
        assert_eq!(parsed.as_ref().map(Vec::len), Some(2));
        assert_eq!(
            parsed.and_then(|gpus| gpus.first().cloned()),
            Some(HostGpu {
                identity: "GPU-one".into(),
                display_name: Some("RTX 4090".into()),
            })
        );
        assert!(parse_nvidia_smi("invalid").is_none());
    }

    #[test]
    fn configured_limits_cap_scheduling_capacity() {
        let config = HostConfig {
            cpu_threads: Some(6),
            memory_bytes: Some(12_000),
            max_concurrent_jobs: 2,
            ..HostConfig::default()
        };
        let inventory = inventory_from(&config, 12, 24_000, Some(Vec::new()));
        assert_eq!(inventory.detected_cpu_threads, 12);
        assert_eq!(inventory.cpu_threads, 6);
        assert_eq!(inventory.detected_memory_bytes, 24_000);
        assert_eq!(inventory.memory_bytes, 12_000);
        assert_eq!(inventory.max_concurrent_jobs, 2);
    }

    #[test]
    fn discovery_does_not_reduce_default_cpu_or_memory_capacity() {
        let inventory = inventory_from(&HostConfig::default(), 12, 24_000, Some(Vec::new()));
        assert_eq!(inventory.cpu_threads, inventory.detected_cpu_threads);
        assert_eq!(inventory.memory_bytes, inventory.detected_memory_bytes);
    }

    #[test]
    fn configured_gpus_override_discovery() {
        let config = HostConfig {
            gpus: vec!["GPU-allowed".into()],
            ..HostConfig::default()
        };
        let inventory = inventory_from(
            &config,
            1,
            1,
            Some(vec![HostGpu {
                identity: "GPU-detected".into(),
                display_name: None,
            }]),
        );
        assert_eq!(inventory.gpus[0].identity, "GPU-allowed");
    }

    #[test]
    fn gpu_discovery_can_be_disabled() {
        let config = HostConfig {
            discover_gpus: false,
            ..HostConfig::default()
        };
        let inventory = inventory_from(&config, 1, 1, None);
        assert!(inventory.gpus.is_empty());
        assert!(inventory.gpu_inventory_authoritative);
    }

    #[test]
    fn failed_gpu_discovery_is_not_authoritative() {
        let inventory = inventory_from(&HostConfig::default(), 1, 1, None);
        assert!(inventory.gpus.is_empty());
        assert!(!inventory.gpu_inventory_authoritative);
    }
}
