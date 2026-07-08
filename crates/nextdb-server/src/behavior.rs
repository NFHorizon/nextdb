use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use tokio::{fs, sync::RwLock};
use wasmtime::{Config, Engine, Linker, Memory, Module, PoolingAllocationConfig, Store, TypedFunc};

use crate::{
    connection::ConnectionTransport, model::BehaviorPublishedManifest, schema::FieldSchema,
};

const DEFAULT_BEHAVIOR_INSTANCE_POOL_MAX: usize = 4;
const DEFAULT_BEHAVIOR_MAX_FUEL: u64 = 10_000_000;
const BEHAVIOR_EPOCH_TICK_MS: u64 = 1;
const BEHAVIOR_EPOCH_FUEL_PER_TICK: u64 = 1_000;
const BEHAVIOR_EPOCH_DEADLINE_MAX_TICKS: u64 = 100_000;
const DEFAULT_BEHAVIOR_POOL_TOTAL_CORE_INSTANCES: u32 = 128;
const DEFAULT_BEHAVIOR_POOL_TOTAL_MEMORIES: u32 = 128;
const DEFAULT_BEHAVIOR_POOL_TOTAL_TABLES: u32 = 128;

#[derive(Clone)]
pub struct BehaviorRuntime {
    engine: Engine,
    behaviors: Arc<RwLock<HashMap<String, LoadedBehavior>>>,
    epoch: Arc<AtomicU64>,
    _epoch_ticker: Arc<BehaviorEpochTicker>,
    config: BehaviorRuntimeConfig,
}

#[derive(Clone)]
struct LoadedBehavior {
    manifest: BehaviorManifest,
    module: Arc<Module>,
    epoch: u64,
    fuel_enabled: bool,
    pool: Arc<Mutex<Vec<BehaviorGuestInstance>>>,
    counters: Arc<BehaviorRuntimeCounters>,
}

#[derive(Default)]
struct BehaviorRuntimeCounters {
    invocations: AtomicU64,
    handle_message_invocations: AtomicU64,
    unknown_message_invocations: AtomicU64,
    successes: AtomicU64,
    guest_errors: AtomicU64,
    command_rejections: AtomicU64,
    instance_create_errors: AtomicU64,
    instances_created: AtomicU64,
    instances_reused: AtomicU64,
    instances_returned: AtomicU64,
    instances_discarded: AtomicU64,
    pool_errors: AtomicU64,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorRuntimeCounterSnapshot {
    pub invocations: u64,
    pub handle_message_invocations: u64,
    pub unknown_message_invocations: u64,
    pub successes: u64,
    pub guest_errors: u64,
    pub command_rejections: u64,
    pub instance_create_errors: u64,
    pub instances_created: u64,
    pub instances_reused: u64,
    pub instances_returned: u64,
    pub instances_discarded: u64,
    pub pool_errors: u64,
}

struct BehaviorGuestInstance {
    store: Store<()>,
    memory: Memory,
    alloc: TypedFunc<u32, u32>,
    dealloc: TypedFunc<(u32, u32), ()>,
    invoke: Option<TypedFunc<(u32, u32), u64>>,
    handle_message: Option<TypedFunc<(u32, u32), u64>>,
    on_unknown_message: Option<TypedFunc<(u32, u32), u64>>,
    on_deactivate: Option<TypedFunc<(), ()>>,
}

struct BehaviorEpochTicker {
    stop: Arc<AtomicBool>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorRuntimeConfig {
    pub fuel_enabled: bool,
    pub instance_pool_max: usize,
    pub pooling_total_core_instances: u32,
    pub pooling_total_memories: u32,
    pub pooling_total_tables: u32,
}

impl BehaviorRuntimeConfig {
    fn from_env() -> Self {
        Self {
            fuel_enabled: env_bool("NEXTDB_BEHAVIOR_FUEL_ENABLED", true),
            instance_pool_max: env_usize(
                "NEXTDB_BEHAVIOR_INSTANCE_POOL_MAX",
                DEFAULT_BEHAVIOR_INSTANCE_POOL_MAX,
            ),
            pooling_total_core_instances: env_u32(
                "NEXTDB_BEHAVIOR_POOL_TOTAL_CORE_INSTANCES",
                DEFAULT_BEHAVIOR_POOL_TOTAL_CORE_INSTANCES,
            )
            .max(1),
            pooling_total_memories: env_u32(
                "NEXTDB_BEHAVIOR_POOL_TOTAL_MEMORIES",
                DEFAULT_BEHAVIOR_POOL_TOTAL_MEMORIES,
            )
            .max(1),
            pooling_total_tables: env_u32(
                "NEXTDB_BEHAVIOR_POOL_TOTAL_TABLES",
                DEFAULT_BEHAVIOR_POOL_TOTAL_TABLES,
            )
            .max(1),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorRuntimeStatus {
    pub epoch: u64,
    pub behavior_count: usize,
    pub fuel_enabled: bool,
    pub instance_pool_max: usize,
    pub pooled_instances: usize,
    pub counters: BehaviorRuntimeCounterSnapshot,
    pub config: BehaviorRuntimeConfig,
    pub behaviors: Vec<BehaviorRuntimeBehaviorStatus>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorRuntimeBehaviorStatus {
    pub name: String,
    pub version: String,
    pub epoch: u64,
    pub pooled_instances: usize,
    pub instance_pool_max: usize,
    pub max_fuel: u64,
    pub fuel_enabled: bool,
    pub abi_encoding: BehaviorAbiEncoding,
    pub counters: BehaviorRuntimeCounterSnapshot,
}

#[derive(Clone, Copy)]
enum BehaviorGuestEntrypoint {
    HandleMessage,
    OnUnknownMessage,
}

impl BehaviorRuntimeCounters {
    fn snapshot(&self) -> BehaviorRuntimeCounterSnapshot {
        BehaviorRuntimeCounterSnapshot {
            invocations: self.invocations.load(Ordering::Relaxed),
            handle_message_invocations: self.handle_message_invocations.load(Ordering::Relaxed),
            unknown_message_invocations: self.unknown_message_invocations.load(Ordering::Relaxed),
            successes: self.successes.load(Ordering::Relaxed),
            guest_errors: self.guest_errors.load(Ordering::Relaxed),
            command_rejections: self.command_rejections.load(Ordering::Relaxed),
            instance_create_errors: self.instance_create_errors.load(Ordering::Relaxed),
            instances_created: self.instances_created.load(Ordering::Relaxed),
            instances_reused: self.instances_reused.load(Ordering::Relaxed),
            instances_returned: self.instances_returned.load(Ordering::Relaxed),
            instances_discarded: self.instances_discarded.load(Ordering::Relaxed),
            pool_errors: self.pool_errors.load(Ordering::Relaxed),
        }
    }
}

impl BehaviorRuntimeCounterSnapshot {
    fn add_assign(&mut self, other: &BehaviorRuntimeCounterSnapshot) {
        self.invocations += other.invocations;
        self.handle_message_invocations += other.handle_message_invocations;
        self.unknown_message_invocations += other.unknown_message_invocations;
        self.successes += other.successes;
        self.guest_errors += other.guest_errors;
        self.command_rejections += other.command_rejections;
        self.instance_create_errors += other.instance_create_errors;
        self.instances_created += other.instances_created;
        self.instances_reused += other.instances_reused;
        self.instances_returned += other.instances_returned;
        self.instances_discarded += other.instances_discarded;
        self.pool_errors += other.pool_errors;
    }
}

pub(crate) struct BehaviorReloadPlan {
    loaded: HashMap<String, LoadedBehavior>,
    manifests: Vec<BehaviorPublishedManifest>,
    epoch: u64,
}

impl BehaviorReloadPlan {
    pub(crate) fn loaded_count(&self) -> usize {
        self.loaded.len()
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }

    pub(crate) fn manifests(&self) -> &[BehaviorPublishedManifest] {
        &self.manifests
    }
}

impl LoadedBehavior {
    fn take_guest_instance(
        &self,
        engine: &Engine,
        manifest: &BehaviorManifest,
    ) -> Result<BehaviorGuestInstance> {
        if let Some(guest) = self
            .pool
            .lock()
            .map_err(|_| anyhow!("behavior instance pool poisoned"))?
            .pop()
        {
            self.counters
                .instances_reused
                .fetch_add(1, Ordering::Relaxed);
            return Ok(guest);
        }
        match BehaviorGuestInstance::instantiate(
            engine,
            self.module.as_ref(),
            manifest,
            self.fuel_enabled,
        ) {
            Ok(guest) => {
                self.counters
                    .instances_created
                    .fetch_add(1, Ordering::Relaxed);
                Ok(guest)
            }
            Err(err) => {
                self.counters
                    .instance_create_errors
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    fn return_guest_instance(
        &self,
        mut guest: BehaviorGuestInstance,
        pool_max: usize,
    ) -> Result<()> {
        let mut pool = self
            .pool
            .lock()
            .map_err(|_| anyhow!("behavior instance pool poisoned"))?;
        if pool.len() < pool_max {
            pool.push(guest);
            self.counters
                .instances_returned
                .fetch_add(1, Ordering::Relaxed);
        } else {
            let _ = guest.deactivate(
                self.manifest.max_fuel.unwrap_or(DEFAULT_BEHAVIOR_MAX_FUEL),
                self.fuel_enabled,
            );
            self.counters
                .instances_discarded
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn deactivate_pooled_instances(&self) {
        let mut pool = match self.pool.lock() {
            Ok(pool) => pool,
            Err(poisoned) => poisoned.into_inner(),
        };
        for mut guest in pool.drain(..) {
            let _ = guest.deactivate(
                self.manifest.max_fuel.unwrap_or(DEFAULT_BEHAVIOR_MAX_FUEL),
                self.fuel_enabled,
            );
        }
    }
}

impl Drop for LoadedBehavior {
    fn drop(&mut self) {
        if Arc::strong_count(&self.pool) == 1 {
            self.deactivate_pooled_instances();
        }
    }
}

impl BehaviorEpochTicker {
    fn spawn(engine: Engine) -> Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let handle = thread::Builder::new()
            .name("nextdb-behavior-epoch".to_string())
            .spawn(move || {
                while !thread_stop.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(BEHAVIOR_EPOCH_TICK_MS));
                    engine.increment_epoch();
                }
            })
            .context("spawn behavior epoch ticker")?;
        Ok(Self {
            stop,
            handle: Mutex::new(Some(handle)),
        })
    }
}

impl Drop for BehaviorEpochTicker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Ok(mut handle) = self.handle.lock() {
            if let Some(handle) = handle.take() {
                let _ = handle.join();
            }
        }
    }
}

impl BehaviorGuestInstance {
    fn instantiate(
        engine: &Engine,
        module: &Module,
        manifest: &BehaviorManifest,
        fuel_enabled: bool,
    ) -> Result<Self> {
        let mut store = Store::new(engine, ());
        reset_behavior_limits(
            &mut store,
            manifest.max_fuel.unwrap_or(DEFAULT_BEHAVIOR_MAX_FUEL),
            fuel_enabled,
        )?;
        let mut linker = Linker::new(engine);
        linker.func_wrap(
            "env",
            "abort",
            |_message: i32, _file: i32, _line: i32, _column: i32| {},
        )?;
        let instance = linker.instantiate(&mut store, module)?;
        if let Ok(on_activate) = instance.get_typed_func::<(), ()>(&mut store, "on_activate") {
            on_activate.call(&mut store, ())?;
        }
        let memory = instance
            .get_memory(&mut store, "memory")
            .context("behavior module must export memory")?;
        let alloc = instance.get_typed_func::<u32, u32>(&mut store, "alloc")?;
        let dealloc = instance.get_typed_func::<(u32, u32), ()>(&mut store, "dealloc")?;
        let invoke = instance
            .get_typed_func::<(u32, u32), u64>(&mut store, "invoke")
            .ok();
        let handle_message = instance
            .get_typed_func::<(u32, u32), u64>(&mut store, "handle_message")
            .ok();
        let on_unknown_message = instance
            .get_typed_func::<(u32, u32), u64>(&mut store, "on_unknown_message")
            .ok();
        let on_deactivate = instance
            .get_typed_func::<(), ()>(&mut store, "on_deactivate")
            .ok();
        Ok(Self {
            store,
            memory,
            alloc,
            dealloc,
            invoke,
            handle_message,
            on_unknown_message,
            on_deactivate,
        })
    }

    #[cfg(test)]
    fn invoke(
        &mut self,
        request: &BehaviorInvokeRequest,
        max_fuel: u64,
    ) -> Result<BehaviorInvokeOutput> {
        self.call(
            request,
            max_fuel,
            BehaviorGuestEntrypoint::HandleMessage,
            BehaviorAbiEncoding::Json,
            true,
        )
    }

    fn call(
        &mut self,
        request: &BehaviorInvokeRequest,
        max_fuel: u64,
        entrypoint: BehaviorGuestEntrypoint,
        abi_encoding: BehaviorAbiEncoding,
        fuel_enabled: bool,
    ) -> Result<BehaviorInvokeOutput> {
        reset_behavior_limits(&mut self.store, max_fuel, fuel_enabled)?;
        let input = encode_behavior_guest_input(request, abi_encoding)?;
        let input_ptr = self.alloc.call(&mut self.store, input.len() as u32)?;
        write_guest(&mut self.store, &self.memory, input_ptr, &input)?;

        let entrypoint = match entrypoint {
            BehaviorGuestEntrypoint::HandleMessage => self
                .handle_message
                .as_ref()
                .or(self.invoke.as_ref())
                .cloned()
                .context("behavior module must export handle_message or invoke")?,
            BehaviorGuestEntrypoint::OnUnknownMessage => self
                .on_unknown_message
                .as_ref()
                .cloned()
                .context("behavior module does not export on_unknown_message")?,
        };
        let packed = entrypoint.call(&mut self.store, (input_ptr, input.len() as u32))?;
        self.dealloc
            .call(&mut self.store, (input_ptr, input.len() as u32))?;

        let output_ptr = (packed >> 32) as u32;
        let output_len = (packed & 0xffff_ffff) as u32;
        let output = read_guest(&mut self.store, &self.memory, output_ptr, output_len)?;
        self.dealloc
            .call(&mut self.store, (output_ptr, output_len))?;

        decode_behavior_guest_output(&output, abi_encoding)
    }

    fn deactivate(&mut self, max_fuel: u64, fuel_enabled: bool) -> Result<()> {
        if let Some(on_deactivate) = &self.on_deactivate {
            reset_behavior_limits(&mut self.store, max_fuel, fuel_enabled)?;
            on_deactivate.call(&mut self.store, ())?;
        }
        Ok(())
    }
}

fn configure_behavior_engine(runtime_config: BehaviorRuntimeConfig) -> Result<Engine> {
    let mut config = Config::new();
    config.consume_fuel(runtime_config.fuel_enabled);
    config.epoch_interruption(true);

    let mut pooling = PoolingAllocationConfig::new();
    pooling.total_core_instances(runtime_config.pooling_total_core_instances);
    pooling.total_memories(runtime_config.pooling_total_memories);
    pooling.total_tables(runtime_config.pooling_total_tables);
    config.allocation_strategy(pooling);

    Engine::new(&config).map_err(|err| anyhow!("create wasm engine: {err}"))
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn env_u32(name: &str, default: u32) -> u32 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn reset_behavior_limits(store: &mut Store<()>, max_fuel: u64, fuel_enabled: bool) -> Result<()> {
    if fuel_enabled {
        store.set_fuel(max_fuel)?;
    }
    store.set_epoch_deadline(behavior_epoch_deadline_ticks(max_fuel));
    Ok(())
}

fn behavior_epoch_deadline_ticks(max_fuel: u64) -> u64 {
    max_fuel
        .saturating_div(BEHAVIOR_EPOCH_FUEL_PER_TICK)
        .max(1)
        .min(BEHAVIOR_EPOCH_DEADLINE_MAX_TICKS)
}

impl BehaviorRuntime {
    pub async fn load_checked<F>(root: PathBuf, validate: F) -> Result<Self>
    where
        F: Fn(&BehaviorManifest) -> Result<()>,
    {
        let config = BehaviorRuntimeConfig::from_env();
        Self::load_checked_with_config(root, validate, config).await
    }

    async fn load_checked_with_config<F>(
        root: PathBuf,
        validate: F,
        config: BehaviorRuntimeConfig,
    ) -> Result<Self>
    where
        F: Fn(&BehaviorManifest) -> Result<()>,
    {
        let engine = configure_behavior_engine(config)?;
        let runtime = Self {
            engine: engine.clone(),
            behaviors: Arc::new(RwLock::new(HashMap::new())),
            epoch: Arc::new(AtomicU64::new(0)),
            _epoch_ticker: Arc::new(BehaviorEpochTicker::spawn(engine)?),
            config,
        };
        let plan = runtime.prepare_reload_checked(root, validate).await?;
        runtime.commit_reload(plan).await;
        Ok(runtime)
    }

    pub(crate) async fn prepare_reload_checked<F>(
        &self,
        root: PathBuf,
        validate: F,
    ) -> Result<BehaviorReloadPlan>
    where
        F: Fn(&BehaviorManifest) -> Result<()>,
    {
        let next_epoch = self.epoch.load(Ordering::Acquire).saturating_add(1);
        let mut loaded = HashMap::new();
        if root.exists() {
            let mut entries = fs::read_dir(&root).await?;
            while let Some(entry) = entries.next_entry().await? {
                if !entry.file_type().await?.is_dir() {
                    continue;
                }
                let manifest_path = entry.path().join("nextdb.behavior.json");
                if !manifest_path.exists() {
                    continue;
                }
                let manifest_bytes = fs::read(&manifest_path).await?;
                let mut manifest: BehaviorManifest = serde_json::from_slice(&manifest_bytes)
                    .with_context(|| format!("parse {}", manifest_path.display()))?;
                if manifest.module_path.is_relative() {
                    manifest.module_path = entry.path().join(&manifest.module_path);
                }
                validate_manifest(&manifest)
                    .with_context(|| format!("validate {}", manifest_path.display()))?;
                validate(&manifest)
                    .with_context(|| format!("validate {}", manifest_path.display()))?;
                if loaded.contains_key(&manifest.name) {
                    bail!("duplicate behavior manifest '{}'", manifest.name);
                }
                let module =
                    Module::from_file(&self.engine, &manifest.module_path).map_err(|err| {
                        anyhow!(
                            "compile wasm module for behavior '{}' at {}: {err}",
                            manifest.name,
                            manifest.module_path.display()
                        )
                    })?;
                loaded.insert(
                    manifest.name.clone(),
                    LoadedBehavior {
                        manifest,
                        module: Arc::new(module),
                        epoch: next_epoch,
                        fuel_enabled: self.config.fuel_enabled,
                        pool: Arc::new(Mutex::new(Vec::new())),
                        counters: Arc::new(BehaviorRuntimeCounters::default()),
                    },
                );
            }
        }

        let mut manifests = loaded
            .values()
            .map(|behavior| BehaviorPublishedManifest {
                name: behavior.manifest.name.clone(),
                version: behavior.manifest.version.clone(),
                module_path: behavior.manifest.module_path.display().to_string(),
                mutations: behavior.manifest.mutations.clone(),
            })
            .collect::<Vec<_>>();
        manifests.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(BehaviorReloadPlan {
            loaded,
            manifests,
            epoch: next_epoch,
        })
    }

    pub(crate) async fn commit_reload(&self, plan: BehaviorReloadPlan) {
        let old_behaviors = {
            let mut behaviors = self.behaviors.write().await;
            std::mem::replace(&mut *behaviors, plan.loaded)
        };
        self.epoch.store(plan.epoch, Ordering::Release);
        drop(old_behaviors);
    }

    #[cfg(test)]
    pub async fn reload_checked<F>(&self, root: PathBuf, validate: F) -> Result<usize>
    where
        F: Fn(&BehaviorManifest) -> Result<()>,
    {
        let plan = self.prepare_reload_checked(root, validate).await?;
        let count = plan.loaded_count();
        self.commit_reload(plan).await;
        Ok(count)
    }

    #[cfg(test)]
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    pub async fn list(&self) -> Vec<BehaviorManifest> {
        let mut behaviors: Vec<_> = self
            .behaviors
            .read()
            .await
            .values()
            .map(|behavior| behavior.manifest.clone())
            .collect();
        behaviors.sort_by(|left, right| left.name.cmp(&right.name));
        behaviors
    }

    pub async fn status(&self) -> BehaviorRuntimeStatus {
        let behaviors = self.behaviors.read().await;
        let mut behavior_statuses = behaviors
            .values()
            .map(|behavior| {
                let pooled_instances = behavior
                    .pool
                    .lock()
                    .map(|pool| pool.len())
                    .unwrap_or_else(|poisoned| poisoned.into_inner().len());
                let counters = behavior.counters.snapshot();
                BehaviorRuntimeBehaviorStatus {
                    name: behavior.manifest.name.clone(),
                    version: behavior.manifest.version.clone(),
                    epoch: behavior.epoch,
                    pooled_instances,
                    instance_pool_max: self.config.instance_pool_max,
                    max_fuel: behavior
                        .manifest
                        .max_fuel
                        .unwrap_or(DEFAULT_BEHAVIOR_MAX_FUEL),
                    fuel_enabled: behavior.fuel_enabled,
                    abi_encoding: behavior.manifest.abi_encoding,
                    counters,
                }
            })
            .collect::<Vec<_>>();
        behavior_statuses.sort_by(|left, right| left.name.cmp(&right.name));
        let pooled_instances = behavior_statuses
            .iter()
            .map(|behavior| behavior.pooled_instances)
            .sum();
        let mut counters = BehaviorRuntimeCounterSnapshot::default();
        for behavior in &behavior_statuses {
            counters.add_assign(&behavior.counters);
        }
        BehaviorRuntimeStatus {
            epoch: self.epoch.load(Ordering::Acquire),
            behavior_count: behavior_statuses.len(),
            fuel_enabled: self.config.fuel_enabled,
            instance_pool_max: self.config.instance_pool_max,
            pooled_instances,
            counters,
            config: self.config,
            behaviors: behavior_statuses,
        }
    }

    pub async fn validate_read_capabilities(
        &self,
        behavior_name: &str,
        read: &BehaviorReadPlan,
    ) -> Result<()> {
        let manifest = {
            let behaviors = self.behaviors.read().await;
            behaviors
                .get(behavior_name)
                .map(|behavior| behavior.manifest.clone())
                .with_context(|| format!("behavior '{}' is not loaded", behavior_name))?
        };
        validate_read_capabilities(&manifest, read)
    }

    pub async fn validate_command_scopes(
        &self,
        behavior_name: &str,
        commands: &[BehaviorCommand],
    ) -> Result<()> {
        let manifest = {
            let behaviors = self.behaviors.read().await;
            behaviors
                .get(behavior_name)
                .map(|behavior| behavior.manifest.clone())
                .with_context(|| format!("behavior '{}' is not loaded", behavior_name))?
        };
        validate_record_scopes_for_commands(&manifest, commands)?;
        validate_object_scopes_for_commands(&manifest, commands)?;
        validate_realtime_scopes_for_commands(&manifest, commands)?;
        validate_connection_scopes_for_commands(&manifest, commands)?;
        validate_user_scopes_for_commands(&manifest, commands)?;
        validate_event_scopes_for_commands(&manifest, commands)?;
        validate_host_http_scopes_for_commands(&manifest, commands)
    }

    pub async fn invoke(&self, request: BehaviorInvokeRequest) -> Result<BehaviorInvokeResult> {
        let loaded = {
            let behaviors = self.behaviors.read().await;
            behaviors
                .get(&request.behavior)
                .cloned()
                .with_context(|| format!("behavior '{}' is not loaded", request.behavior))?
        };
        let manifest = &loaded.manifest;

        let entrypoint = if manifest
            .mutations
            .iter()
            .any(|mutation| mutation == &request.mutation)
        {
            BehaviorGuestEntrypoint::HandleMessage
        } else {
            BehaviorGuestEntrypoint::OnUnknownMessage
        };
        loaded.counters.invocations.fetch_add(1, Ordering::Relaxed);
        match entrypoint {
            BehaviorGuestEntrypoint::HandleMessage => {
                loaded
                    .counters
                    .handle_message_invocations
                    .fetch_add(1, Ordering::Relaxed);
            }
            BehaviorGuestEntrypoint::OnUnknownMessage => {
                loaded
                    .counters
                    .unknown_message_invocations
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        let mut guest = loaded.take_guest_instance(&self.engine, manifest)?;
        let output = match guest.call(
            &request,
            manifest.max_fuel.unwrap_or(DEFAULT_BEHAVIOR_MAX_FUEL),
            entrypoint,
            manifest.abi_encoding,
            loaded.fuel_enabled,
        ) {
            Ok(output) => output,
            Err(err) => {
                loaded.counters.guest_errors.fetch_add(1, Ordering::Relaxed);
                return Err(err);
            }
        };
        if let Err(err) = validate_command_capabilities(manifest, &output.commands) {
            loaded
                .counters
                .command_rejections
                .fetch_add(1, Ordering::Relaxed);
            let _ = guest.deactivate(
                manifest.max_fuel.unwrap_or(DEFAULT_BEHAVIOR_MAX_FUEL),
                loaded.fuel_enabled,
            );
            loaded
                .counters
                .instances_discarded
                .fetch_add(1, Ordering::Relaxed);
            return Err(err);
        }
        if let Err(err) = loaded.return_guest_instance(guest, self.config.instance_pool_max) {
            loaded.counters.pool_errors.fetch_add(1, Ordering::Relaxed);
            return Err(err);
        }
        loaded.counters.successes.fetch_add(1, Ordering::Relaxed);
        Ok(BehaviorInvokeResult {
            output,
            metadata: BehaviorInvocationMetadata {
                behavior: manifest.name.clone(),
                behavior_version: manifest.version.clone(),
                epoch: loaded.epoch,
            },
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorInvocationMetadata {
    pub behavior: String,
    pub behavior_version: String,
    pub epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorInvokeResult {
    pub output: BehaviorInvokeOutput,
    pub metadata: BehaviorInvocationMetadata,
}

fn write_guest(store: &mut Store<()>, memory: &Memory, ptr: u32, bytes: &[u8]) -> Result<()> {
    let start = ptr as usize;
    let end = start + bytes.len();
    let data = memory.data_mut(store);
    if end > data.len() {
        bail!("guest memory write out of bounds");
    }
    data[start..end].copy_from_slice(bytes);
    Ok(())
}

fn read_guest(store: &mut Store<()>, memory: &Memory, ptr: u32, len: u32) -> Result<Vec<u8>> {
    let start = ptr as usize;
    let end = start + len as usize;
    let data = memory.data(store);
    if end > data.len() {
        bail!("guest memory read out of bounds");
    }
    Ok(data[start..end].to_vec())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorManifest {
    pub name: String,
    pub version: String,
    pub module_path: PathBuf,
    #[serde(default)]
    pub abi_encoding: BehaviorAbiEncoding,
    #[serde(default)]
    pub mutations: Vec<String>,
    #[serde(default)]
    pub inputs: BTreeMap<String, FieldSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reads: Option<Vec<BehaviorReadCapability>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_scopes: Option<BehaviorRecordScopes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_scopes: Option<BehaviorObjectScopes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realtime_scopes: Option<BehaviorRealtimeScopes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_scopes: Option<BehaviorConnectionScopes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_scopes: Option<BehaviorUserScopes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_scopes: Option<BehaviorEventScopes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_http_scopes: Option<BehaviorHostHttpScopes>,
    #[serde(default)]
    pub commands: Vec<BehaviorCommandCapability>,
    pub max_fuel: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BehaviorAbiEncoding {
    #[default]
    Json,
    Postcard,
    PostcardTypedSchema,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BehaviorPostcardFrame {
    encoding: BehaviorPostcardPayloadEncoding,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum BehaviorPostcardPayloadEncoding {
    Json,
    TypedSchema,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyBehaviorPostcardFrame {
    json: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TypedBehaviorInvokeOutput {
    #[serde(default)]
    commands: Vec<PostcardJsonValue>,
    result: PostcardJsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TypedBehaviorInvokeRequest {
    behavior: String,
    mutation: String,
    user_id: Option<String>,
    client_mutation_id: Option<String>,
    input: PostcardJsonValue,
    #[serde(default)]
    read: BehaviorReadPlan,
    #[serde(default)]
    context: PostcardJsonValue,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum PostcardJsonValue {
    #[default]
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Array(Vec<PostcardJsonValue>),
    Object(BTreeMap<String, PostcardJsonValue>),
}

impl BehaviorPostcardFrame {
    fn json(json: Vec<u8>) -> Self {
        Self {
            encoding: BehaviorPostcardPayloadEncoding::Json,
            payload: json,
        }
    }

    fn typed_schema(payload: Vec<u8>) -> Self {
        Self {
            encoding: BehaviorPostcardPayloadEncoding::TypedSchema,
            payload,
        }
    }
}

enum DecodedBehaviorPostcardPayload {
    Json(Vec<u8>),
    TypedSchema(Vec<u8>),
}

fn decode_behavior_postcard_payload(frame: &[u8]) -> Result<DecodedBehaviorPostcardPayload> {
    match postcard::from_bytes::<BehaviorPostcardFrame>(frame) {
        Ok(frame) => match frame.encoding {
            BehaviorPostcardPayloadEncoding::Json => Ok(DecodedBehaviorPostcardPayload::Json(frame.payload)),
            BehaviorPostcardPayloadEncoding::TypedSchema => {
                Ok(DecodedBehaviorPostcardPayload::TypedSchema(frame.payload))
            }
        },
        Err(frame_err) => postcard::from_bytes::<LegacyBehaviorPostcardFrame>(frame)
            .map(|legacy| DecodedBehaviorPostcardPayload::Json(legacy.json))
            .map_err(|legacy_err| {
                anyhow::anyhow!(
                    "invalid behavior postcard frame: typed frame error: {frame_err}; legacy frame error: {legacy_err}"
                )
            }),
    }
}

fn encode_behavior_guest_input(
    request: &BehaviorInvokeRequest,
    abi_encoding: BehaviorAbiEncoding,
) -> Result<Vec<u8>> {
    let json = serde_json::to_vec(request)?;
    match abi_encoding {
        BehaviorAbiEncoding::Json => Ok(json),
        BehaviorAbiEncoding::Postcard => {
            postcard::to_allocvec(&BehaviorPostcardFrame::json(json)).map_err(Into::into)
        }
        BehaviorAbiEncoding::PostcardTypedSchema => {
            let payload = postcard::to_allocvec(&TypedBehaviorInvokeRequest {
                behavior: request.behavior.clone(),
                mutation: request.mutation.clone(),
                user_id: request.user_id.clone(),
                client_mutation_id: request.client_mutation_id.clone(),
                input: PostcardJsonValue::try_from(request.input.clone())?,
                read: request.read.clone(),
                context: PostcardJsonValue::try_from(request.context.clone())?,
            })?;
            postcard::to_allocvec(&BehaviorPostcardFrame::typed_schema(payload)).map_err(Into::into)
        }
    }
}

fn decode_behavior_guest_output(
    output: &[u8],
    abi_encoding: BehaviorAbiEncoding,
) -> Result<BehaviorInvokeOutput> {
    match abi_encoding {
        BehaviorAbiEncoding::Json => serde_json::from_slice(output).map_err(Into::into),
        BehaviorAbiEncoding::Postcard | BehaviorAbiEncoding::PostcardTypedSchema => {
            match decode_behavior_postcard_payload(output)? {
                DecodedBehaviorPostcardPayload::Json(json) => {
                    serde_json::from_slice(&json).map_err(Into::into)
                }
                DecodedBehaviorPostcardPayload::TypedSchema(payload) => {
                    decode_typed_behavior_output(&payload)
                }
            }
        }
    }
}

fn decode_typed_behavior_output(payload: &[u8]) -> Result<BehaviorInvokeOutput> {
    let typed = postcard::from_bytes::<TypedBehaviorInvokeOutput>(payload)?;
    let mut commands = Vec::with_capacity(typed.commands.len());
    for command in typed.commands {
        commands.push(serde_json::from_value(Value::from(command))?);
    }
    Ok(BehaviorInvokeOutput {
        commands,
        result: Value::from(typed.result),
    })
}

impl TryFrom<Value> for PostcardJsonValue {
    type Error = anyhow::Error;

    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Null => Ok(Self::Null),
            Value::Bool(value) => Ok(Self::Bool(value)),
            Value::Number(value) => {
                if let Some(value) = value.as_i64() {
                    Ok(Self::I64(value))
                } else if let Some(value) = value.as_u64() {
                    Ok(Self::U64(value))
                } else if let Some(value) = value.as_f64() {
                    Ok(Self::F64(value))
                } else {
                    anyhow::bail!("unsupported JSON number in typed behavior postcard payload")
                }
            }
            Value::String(value) => Ok(Self::String(value)),
            Value::Array(values) => values
                .into_iter()
                .map(PostcardJsonValue::try_from)
                .collect::<Result<Vec<_>>>()
                .map(Self::Array),
            Value::Object(values) => values
                .into_iter()
                .map(|(key, value)| Ok((key, PostcardJsonValue::try_from(value)?)))
                .collect::<Result<BTreeMap<_, _>>>()
                .map(Self::Object),
        }
    }
}

impl From<PostcardJsonValue> for Value {
    fn from(value: PostcardJsonValue) -> Self {
        match value {
            PostcardJsonValue::Null => Value::Null,
            PostcardJsonValue::Bool(value) => Value::Bool(value),
            PostcardJsonValue::I64(value) => Value::Number(Number::from(value)),
            PostcardJsonValue::U64(value) => Value::Number(Number::from(value)),
            PostcardJsonValue::F64(value) => Number::from_f64(value)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            PostcardJsonValue::String(value) => Value::String(value),
            PostcardJsonValue::Array(values) => {
                Value::Array(values.into_iter().map(Value::from).collect())
            }
            PostcardJsonValue::Object(values) => Value::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, Value::from(value)))
                    .collect(),
            ),
        }
    }
}

fn validate_manifest(manifest: &BehaviorManifest) -> Result<()> {
    if manifest.name.trim().is_empty() {
        bail!("behavior manifest name must not be empty");
    }
    if manifest.version.trim().is_empty() {
        bail!("behavior manifest version must not be empty");
    }
    if manifest.mutations.is_empty() {
        bail!("behavior manifest mutations must not be empty");
    }
    let mut seen = HashSet::new();
    for mutation in &manifest.mutations {
        if mutation.trim().is_empty() {
            bail!("behavior manifest mutations must not contain empty names");
        }
        if !seen.insert(mutation.as_str()) {
            bail!("behavior manifest mutation '{}' is duplicated", mutation);
        }
    }
    for mutation in manifest.inputs.keys() {
        if !seen.contains(mutation.as_str()) {
            bail!(
                "behavior manifest input '{}' is not listed in mutations",
                mutation
            );
        }
    }
    if let Some(manifest_reads) = &manifest.reads {
        let mut reads = HashSet::new();
        for read in manifest_reads {
            if !reads.insert(*read) {
                bail!("behavior manifest read '{read:?}' is duplicated");
            }
        }
    }
    if let Some(record_scopes) = &manifest.record_scopes {
        record_scopes.validate()?;
    }
    if let Some(object_scopes) = &manifest.object_scopes {
        object_scopes.validate()?;
    }
    if let Some(realtime_scopes) = &manifest.realtime_scopes {
        realtime_scopes.validate()?;
    }
    if let Some(connection_scopes) = &manifest.connection_scopes {
        connection_scopes.validate()?;
    }
    if let Some(user_scopes) = &manifest.user_scopes {
        user_scopes.validate()?;
    }
    if let Some(event_scopes) = &manifest.event_scopes {
        event_scopes.validate()?;
    }
    if let Some(host_http_scopes) = &manifest.host_http_scopes {
        host_http_scopes.validate()?;
    }
    let mut commands = HashSet::new();
    for command in &manifest.commands {
        if !commands.insert(*command) {
            bail!("behavior manifest command '{command:?}' is duplicated");
        }
    }
    Ok(())
}

fn validate_read_capabilities(manifest: &BehaviorManifest, read: &BehaviorReadPlan) -> Result<()> {
    if let Some(manifest_reads) = &manifest.reads {
        let allowed: HashSet<BehaviorReadCapability> = manifest_reads.iter().copied().collect();
        for capability in read.capabilities() {
            if !allowed.contains(&capability) {
                bail!(
                    "behavior '{}' is not allowed to use read plan {:?}",
                    manifest.name,
                    capability
                );
            }
        }
    }
    validate_record_scopes_for_read(manifest, read)?;
    validate_object_scopes_for_read(manifest, read)?;
    validate_realtime_scopes_for_read(manifest, read)?;
    validate_connection_scopes_for_read(manifest, read)?;
    validate_user_scopes_for_read(manifest, read)?;
    Ok(())
}

fn validate_record_scopes_for_read(
    manifest: &BehaviorManifest,
    read: &BehaviorReadPlan,
) -> Result<()> {
    let Some(scopes) = &manifest.record_scopes else {
        return Ok(());
    };
    for item in &read.records {
        scopes.ensure_table_read(&manifest.name, &item.table)?;
    }
    for item in &read.nested_records {
        scopes.ensure_nested_read(
            &manifest.name,
            &nested_scope_name(&item.table, &item.nested),
        )?;
    }
    for _ in &read.latest_messages {
        scopes.ensure_nested_read(&manifest.name, "rooms.messages")?;
    }
    for item in &read.audit_traces {
        match item.kind {
            BehaviorAuditTraceKind::Room => {
                scopes.ensure_table_read(&manifest.name, "rooms")?;
                scopes.ensure_nested_read(&manifest.name, "rooms.messages")?;
            }
            BehaviorAuditTraceKind::Record => {
                let table = required_behavior_audit_field(
                    item.table.as_deref(),
                    "auditTraces record table is required",
                )?;
                scopes.ensure_table_read(&manifest.name, table)?;
            }
            BehaviorAuditTraceKind::NestedRecord => {
                let table = required_behavior_audit_field(
                    item.table.as_deref(),
                    "auditTraces nestedRecord table is required",
                )?;
                let nested = required_behavior_audit_field(
                    item.nested.as_deref(),
                    "auditTraces nestedRecord nested is required",
                )?;
                scopes.ensure_nested_read(&manifest.name, &nested_scope_name(table, nested))?;
            }
            BehaviorAuditTraceKind::User
            | BehaviorAuditTraceKind::Object
            | BehaviorAuditTraceKind::Path
            | BehaviorAuditTraceKind::ClientMutation => {}
        }
    }
    for item in &read.audit_replays {
        match item.kind {
            BehaviorAuditReplayKind::Record => {
                let table = required_behavior_audit_field(
                    item.table.as_deref(),
                    "auditReplays record table is required",
                )?;
                scopes.ensure_table_read(&manifest.name, table)?;
            }
            BehaviorAuditReplayKind::NestedRecord => {
                let table = required_behavior_audit_field(
                    item.table.as_deref(),
                    "auditReplays nestedRecord table is required",
                )?;
                let nested = required_behavior_audit_field(
                    item.nested.as_deref(),
                    "auditReplays nestedRecord nested is required",
                )?;
                scopes.ensure_nested_read(&manifest.name, &nested_scope_name(table, nested))?;
            }
            BehaviorAuditReplayKind::User | BehaviorAuditReplayKind::Object => {}
        }
    }
    Ok(())
}

fn validate_record_scopes_for_commands(
    manifest: &BehaviorManifest,
    commands: &[BehaviorCommand],
) -> Result<()> {
    let Some(scopes) = &manifest.record_scopes else {
        return Ok(());
    };
    for command in commands {
        match command {
            BehaviorCommand::SendMessage { .. } => {
                scopes.ensure_nested_write(&manifest.name, "rooms.messages")?;
            }
            BehaviorCommand::UpsertRecord { table, .. }
            | BehaviorCommand::DeleteRecord { table, .. } => {
                scopes.ensure_table_write(&manifest.name, table)?;
            }
            BehaviorCommand::RecordTransaction { operations, .. } => {
                for operation in operations {
                    match operation {
                        BehaviorRecordTransactionOperation::Upsert { table, .. }
                        | BehaviorRecordTransactionOperation::Delete { table, .. } => {
                            scopes.ensure_table_write(&manifest.name, table)?;
                        }
                        BehaviorRecordTransactionOperation::NestedUpsert {
                            table, nested, ..
                        }
                        | BehaviorRecordTransactionOperation::NestedDelete {
                            table, nested, ..
                        } => {
                            scopes.ensure_nested_write(
                                &manifest.name,
                                &nested_scope_name(table, nested),
                            )?;
                        }
                    }
                }
            }
            BehaviorCommand::ActivateRuntimeRecords { table, .. } => {
                scopes.ensure_table_read(&manifest.name, table)?;
            }
            BehaviorCommand::EvictRuntimeRecords { table, .. } => {
                scopes.ensure_table_write(&manifest.name, table)?;
            }
            BehaviorCommand::ActivateRuntimeRoom { .. } => {
                scopes.ensure_nested_read(&manifest.name, "rooms.messages")?;
            }
            BehaviorCommand::EvictRuntimeRoom { .. } => {
                scopes.ensure_nested_write(&manifest.name, "rooms.messages")?;
            }
            BehaviorCommand::PublishVolatile { .. }
            | BehaviorCommand::PublishUserVolatile { .. }
            | BehaviorCommand::PublishUserEvent { .. }
            | BehaviorCommand::BroadcastRealtimeChannel { .. }
            | BehaviorCommand::UpdateRealtimePresence { .. }
            | BehaviorCommand::UpdateRealtimeChannelState { .. }
            | BehaviorCommand::PutObject { .. }
            | BehaviorCommand::DeleteObject { .. }
            | BehaviorCommand::DisconnectConnections { .. }
            | BehaviorCommand::ScheduleActorReminder { .. }
            | BehaviorCommand::RequestHostHttp { .. } => {}
        }
    }
    Ok(())
}

fn validate_object_scopes_for_read(
    manifest: &BehaviorManifest,
    read: &BehaviorReadPlan,
) -> Result<()> {
    let Some(scopes) = &manifest.object_scopes else {
        return Ok(());
    };
    for item in &read.objects {
        scopes.ensure_object_read(&manifest.name, &item.object_id)?;
    }
    for item in &read.object_bodies {
        scopes.ensure_object_read(&manifest.name, &item.object_id)?;
    }
    for item in &read.audit_traces {
        if item.kind == BehaviorAuditTraceKind::Object {
            let object_id = required_behavior_audit_field(
                item.id.as_deref(),
                "auditTraces object id is required",
            )?;
            scopes.ensure_object_read(&manifest.name, object_id)?;
        }
    }
    for item in &read.audit_replays {
        if item.kind == BehaviorAuditReplayKind::Object {
            let object_id = required_behavior_audit_field(
                item.id.as_deref(),
                "auditReplays object id is required",
            )?;
            scopes.ensure_object_read(&manifest.name, object_id)?;
        }
    }
    Ok(())
}

fn validate_object_scopes_for_commands(
    manifest: &BehaviorManifest,
    commands: &[BehaviorCommand],
) -> Result<()> {
    let Some(scopes) = &manifest.object_scopes else {
        return Ok(());
    };
    for command in commands {
        match command {
            BehaviorCommand::PutObject { object_id, .. } => {
                scopes.ensure_object_write_optional(&manifest.name, object_id.as_deref())?;
            }
            BehaviorCommand::DeleteObject { object_id, .. } => {
                scopes.ensure_object_write(&manifest.name, object_id)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_realtime_scopes_for_read(
    manifest: &BehaviorManifest,
    read: &BehaviorReadPlan,
) -> Result<()> {
    let Some(scopes) = &manifest.realtime_scopes else {
        return Ok(());
    };
    for item in &read.realtime_channel_members {
        scopes.ensure_channel_read(&manifest.name, &item.channel_id)?;
    }
    for item in &read.realtime_channel_states {
        scopes.ensure_channel_read(&manifest.name, &item.channel_id)?;
    }
    Ok(())
}

fn validate_realtime_scopes_for_commands(
    manifest: &BehaviorManifest,
    commands: &[BehaviorCommand],
) -> Result<()> {
    let Some(scopes) = &manifest.realtime_scopes else {
        return Ok(());
    };
    for command in commands {
        match command {
            BehaviorCommand::BroadcastRealtimeChannel { channel_id, .. }
            | BehaviorCommand::UpdateRealtimePresence { channel_id, .. }
            | BehaviorCommand::UpdateRealtimeChannelState { channel_id, .. } => {
                scopes.ensure_channel_write(&manifest.name, channel_id)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_connection_scopes_for_read(
    manifest: &BehaviorManifest,
    read: &BehaviorReadPlan,
) -> Result<()> {
    let Some(scopes) = &manifest.connection_scopes else {
        return Ok(());
    };
    for item in &read.connection_sessions {
        scopes.ensure_user_read(&manifest.name, item.user_id.as_deref())?;
    }
    Ok(())
}

fn validate_connection_scopes_for_commands(
    manifest: &BehaviorManifest,
    commands: &[BehaviorCommand],
) -> Result<()> {
    let Some(scopes) = &manifest.connection_scopes else {
        return Ok(());
    };
    for command in commands {
        if let BehaviorCommand::DisconnectConnections { user_id, .. } = command {
            scopes.ensure_user_write(&manifest.name, user_id.as_deref())?;
        }
    }
    Ok(())
}

fn validate_event_scopes_for_commands(
    manifest: &BehaviorManifest,
    commands: &[BehaviorCommand],
) -> Result<()> {
    let Some(scopes) = &manifest.event_scopes else {
        return Ok(());
    };
    for command in commands {
        match command {
            BehaviorCommand::PublishVolatile { name, .. }
            | BehaviorCommand::PublishUserVolatile { name, .. }
            | BehaviorCommand::PublishUserEvent { name, .. } => {
                scopes.ensure_publish(&manifest.name, name)?;
            }
            BehaviorCommand::BroadcastRealtimeChannel { kind, .. } => {
                scopes.ensure_realtime_broadcast(&manifest.name, kind)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_user_scopes_for_commands(
    manifest: &BehaviorManifest,
    commands: &[BehaviorCommand],
) -> Result<()> {
    let Some(scopes) = &manifest.user_scopes else {
        return Ok(());
    };
    for command in commands {
        if let BehaviorCommand::PublishUserEvent { user_id, .. }
        | BehaviorCommand::PublishUserVolatile { user_id, .. } = command
        {
            scopes.ensure_publish(&manifest.name, user_id)?;
        }
    }
    Ok(())
}

fn validate_host_http_scopes_for_commands(
    manifest: &BehaviorManifest,
    commands: &[BehaviorCommand],
) -> Result<()> {
    for command in commands {
        if let BehaviorCommand::RequestHostHttp { url, .. } = command {
            let Some(scopes) = &manifest.host_http_scopes else {
                bail!(
                    "behavior '{}' is not allowed to request host HTTP without hostHttpScopes",
                    manifest.name
                );
            };
            scopes.ensure_url_allowed(&manifest.name, url)?;
        }
    }
    Ok(())
}

fn validate_user_scopes_for_read(
    manifest: &BehaviorManifest,
    read: &BehaviorReadPlan,
) -> Result<()> {
    let Some(scopes) = &manifest.user_scopes else {
        return Ok(());
    };
    for item in &read.audit_traces {
        if item.kind == BehaviorAuditTraceKind::User {
            let user_id = required_behavior_audit_field(
                item.id.as_deref(),
                "auditTraces user id is required",
            )?;
            scopes.ensure_read(&manifest.name, user_id)?;
        }
    }
    for item in &read.audit_replays {
        if item.kind == BehaviorAuditReplayKind::User {
            let user_id = required_behavior_audit_field(
                item.id.as_deref(),
                "auditReplays user id is required",
            )?;
            scopes.ensure_read(&manifest.name, user_id)?;
        }
    }
    Ok(())
}

fn required_behavior_audit_field<'a>(
    value: Option<&'a str>,
    message: &'static str,
) -> Result<&'a str> {
    let Some(value) = value else {
        bail!("{message}");
    };
    if value.trim().is_empty() {
        bail!("{message}");
    }
    Ok(value)
}

fn nested_scope_name(table: &str, nested: &str) -> String {
    format!("{table}.{nested}")
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorRecordScopes {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub read: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub write: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nested_read: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nested_write: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorObjectScopes {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub read: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub write: Vec<String>,
}

impl BehaviorObjectScopes {
    fn validate(&self) -> Result<()> {
        validate_wildcard_scope_list("objectScopes.read", &self.read)?;
        validate_wildcard_scope_list("objectScopes.write", &self.write)?;
        Ok(())
    }

    fn ensure_object_read(&self, behavior: &str, object_id: &str) -> Result<()> {
        ensure_wildcard_scope_contains(behavior, "read object", object_id, &self.read)
    }

    fn ensure_object_write(&self, behavior: &str, object_id: &str) -> Result<()> {
        ensure_wildcard_scope_contains(behavior, "write object", object_id, &self.write)
    }

    fn ensure_object_write_optional(&self, behavior: &str, object_id: Option<&str>) -> Result<()> {
        match object_id {
            Some(object_id) => self.ensure_object_write(behavior, object_id),
            None if self.write.iter().any(|scope| scope == "*") => Ok(()),
            None => bail!(
                "behavior '{behavior}' is not allowed to write generated object ids without objectScopes.write '*'"
            ),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorRealtimeScopes {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub read: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub write: Vec<String>,
}

impl BehaviorRealtimeScopes {
    fn validate(&self) -> Result<()> {
        validate_wildcard_scope_list("realtimeScopes.read", &self.read)?;
        validate_wildcard_scope_list("realtimeScopes.write", &self.write)?;
        Ok(())
    }

    fn ensure_channel_read(&self, behavior: &str, channel_id: &str) -> Result<()> {
        ensure_wildcard_scope_contains(behavior, "read realtime channel", channel_id, &self.read)
    }

    fn ensure_channel_write(&self, behavior: &str, channel_id: &str) -> Result<()> {
        ensure_wildcard_scope_contains(behavior, "write realtime channel", channel_id, &self.write)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorConnectionScopes {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub read: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub write: Vec<String>,
}

impl BehaviorConnectionScopes {
    fn validate(&self) -> Result<()> {
        validate_wildcard_scope_list("connectionScopes.read", &self.read)?;
        validate_wildcard_scope_list("connectionScopes.write", &self.write)?;
        Ok(())
    }

    fn ensure_user_read(&self, behavior: &str, user_id: Option<&str>) -> Result<()> {
        match user_id {
            Some(user_id) => ensure_wildcard_scope_contains(
                behavior,
                "read connection sessions for user",
                user_id,
                &self.read,
            ),
            None if self.read.iter().any(|scope| scope == "*") => Ok(()),
            None => bail!(
                "behavior '{behavior}' is not allowed to read all connection sessions without connectionScopes.read '*'"
            ),
        }
    }

    fn ensure_user_write(&self, behavior: &str, user_id: Option<&str>) -> Result<()> {
        match user_id {
            Some(user_id) => ensure_wildcard_scope_contains(
                behavior,
                "write connection sessions for user",
                user_id,
                &self.write,
            ),
            None if self.write.iter().any(|scope| scope == "*") => Ok(()),
            None => bail!(
                "behavior '{behavior}' is not allowed to write all connection sessions without connectionScopes.write '*'"
            ),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorUserScopes {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub read: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub publish: Vec<String>,
}

impl BehaviorUserScopes {
    fn validate(&self) -> Result<()> {
        validate_wildcard_scope_list("userScopes.read", &self.read)?;
        validate_wildcard_scope_list("userScopes.publish", &self.publish)?;
        Ok(())
    }

    fn ensure_read(&self, behavior: &str, user_id: &str) -> Result<()> {
        ensure_wildcard_scope_contains(behavior, "read user audit", user_id, &self.read)
    }

    fn ensure_publish(&self, behavior: &str, user_id: &str) -> Result<()> {
        ensure_wildcard_scope_contains(
            behavior,
            "publish user event to user",
            user_id,
            &self.publish,
        )
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorEventScopes {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub publish: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub realtime_broadcast: Vec<String>,
}

impl BehaviorEventScopes {
    fn validate(&self) -> Result<()> {
        validate_wildcard_scope_list("eventScopes.publish", &self.publish)?;
        validate_wildcard_scope_list("eventScopes.realtimeBroadcast", &self.realtime_broadcast)?;
        Ok(())
    }

    fn ensure_publish(&self, behavior: &str, name: &str) -> Result<()> {
        ensure_wildcard_scope_contains(behavior, "publish event", name, &self.publish)
    }

    fn ensure_realtime_broadcast(&self, behavior: &str, kind: &str) -> Result<()> {
        ensure_wildcard_scope_contains(
            behavior,
            "broadcast realtime event",
            kind,
            &self.realtime_broadcast,
        )
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorHostHttpScopes {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow_url_prefixes: Vec<String>,
}

impl BehaviorHostHttpScopes {
    fn validate(&self) -> Result<()> {
        if self.allow_url_prefixes.is_empty() {
            bail!("behavior manifest hostHttpScopes.allowUrlPrefixes must not be empty");
        }
        let mut seen = HashSet::new();
        for prefix in &self.allow_url_prefixes {
            if prefix.trim().is_empty() {
                bail!(
                    "behavior manifest hostHttpScopes.allowUrlPrefixes must not contain empty values"
                );
            }
            if !prefix.starts_with("https://") && !prefix.starts_with("http://") {
                bail!(
                    "behavior manifest hostHttpScopes.allowUrlPrefixes value '{prefix}' must start with http:// or https://"
                );
            }
            if !seen.insert(prefix.as_str()) {
                bail!(
                    "behavior manifest hostHttpScopes.allowUrlPrefixes contains duplicate value '{prefix}'"
                );
            }
        }
        Ok(())
    }

    fn ensure_url_allowed(&self, behavior: &str, url: &str) -> Result<()> {
        if self
            .allow_url_prefixes
            .iter()
            .any(|prefix| url.starts_with(prefix))
        {
            return Ok(());
        }
        bail!("behavior '{behavior}' is not allowed to request host HTTP URL '{url}'")
    }
}

impl BehaviorRecordScopes {
    fn validate(&self) -> Result<()> {
        validate_scope_list("recordScopes.read", &self.read)?;
        validate_scope_list("recordScopes.write", &self.write)?;
        validate_scope_list("recordScopes.nestedRead", &self.nested_read)?;
        validate_scope_list("recordScopes.nestedWrite", &self.nested_write)?;
        Ok(())
    }

    fn ensure_table_read(&self, behavior: &str, table: &str) -> Result<()> {
        ensure_scope_contains(behavior, "read table", table, &self.read)
    }

    fn ensure_table_write(&self, behavior: &str, table: &str) -> Result<()> {
        ensure_scope_contains(behavior, "write table", table, &self.write)
    }

    fn ensure_nested_read(&self, behavior: &str, nested: &str) -> Result<()> {
        ensure_scope_contains(behavior, "read nested table", nested, &self.nested_read)
    }

    fn ensure_nested_write(&self, behavior: &str, nested: &str) -> Result<()> {
        ensure_scope_contains(behavior, "write nested table", nested, &self.nested_write)
    }
}

fn validate_scope_list(label: &str, values: &[String]) -> Result<()> {
    let mut seen = HashSet::new();
    for value in values {
        if value.trim().is_empty() {
            bail!("behavior manifest {label} must not contain empty values");
        }
        if !seen.insert(value.as_str()) {
            bail!("behavior manifest {label} contains duplicate value '{value}'");
        }
    }
    Ok(())
}

fn validate_wildcard_scope_list(label: &str, values: &[String]) -> Result<()> {
    let mut seen = HashSet::new();
    for value in values {
        if value.trim().is_empty() {
            bail!("behavior manifest {label} must not contain empty values");
        }
        let wildcard_count = value.matches('*').count();
        if wildcard_count > 0 && (value != "*" && (wildcard_count != 1 || !value.ends_with('*'))) {
            bail!("behavior manifest {label} wildcard must be '*' or a trailing prefix wildcard");
        }
        if value != "*" && value.ends_with('*') && value.len() == 1 {
            bail!("behavior manifest {label} wildcard prefix must not be empty");
        }
        if !seen.insert(value.as_str()) {
            bail!("behavior manifest {label} contains duplicate value '{value}'");
        }
    }
    Ok(())
}

fn ensure_scope_contains(
    behavior: &str,
    label: &str,
    value: &str,
    allowed: &[String],
) -> Result<()> {
    if allowed.iter().any(|allowed| allowed == value) {
        return Ok(());
    }
    bail!("behavior '{behavior}' is not allowed to {label} '{value}'")
}

fn ensure_wildcard_scope_contains(
    behavior: &str,
    label: &str,
    value: &str,
    allowed: &[String],
) -> Result<()> {
    if allowed
        .iter()
        .any(|scope| wildcard_scope_matches(scope, value))
    {
        return Ok(());
    }
    bail!("behavior '{behavior}' is not allowed to {label} '{value}'")
}

fn wildcard_scope_matches(scope: &str, value: &str) -> bool {
    if scope == "*" || scope == value {
        return true;
    }
    scope
        .strip_suffix('*')
        .is_some_and(|prefix| !prefix.is_empty() && value.starts_with(prefix))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BehaviorReadCapability {
    Records,
    NestedRecords,
    LatestMessages,
    Objects,
    ObjectBodies,
    RealtimeChannelMembers,
    RealtimeChannelStates,
    ConnectionSessions,
    AuditTraces,
    AuditReplays,
}

fn validate_command_capabilities(
    manifest: &BehaviorManifest,
    commands: &[BehaviorCommand],
) -> Result<()> {
    if manifest.commands.is_empty() {
        return Ok(());
    }
    let allowed: HashSet<BehaviorCommandCapability> = manifest.commands.iter().copied().collect();
    for command in commands {
        let capability = command.capability();
        if !allowed.contains(&capability) {
            bail!(
                "behavior '{}' is not allowed to return host command {:?}",
                manifest.name,
                capability
            );
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BehaviorCommandCapability {
    SendMessage,
    PublishVolatile,
    PublishUserVolatile,
    PublishUserEvent,
    PutObject,
    DeleteObject,
    UpsertRecord,
    DeleteRecord,
    RecordTransaction,
    BroadcastRealtimeChannel,
    UpdateRealtimeChannelState,
    UpdateRealtimePresence,
    DisconnectConnections,
    ActivateRuntimeRecords,
    EvictRuntimeRecords,
    ActivateRuntimeRoom,
    EvictRuntimeRoom,
    ScheduleActorReminder,
    RequestHostHttp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorInvokeRequest {
    pub behavior: String,
    pub mutation: String,
    pub user_id: Option<String>,
    pub client_mutation_id: Option<String>,
    pub input: Value,
    #[serde(default)]
    pub read: BehaviorReadPlan,
    #[serde(default)]
    pub context: Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorReadPlan {
    #[serde(default)]
    pub records: Vec<BehaviorRecordRead>,
    #[serde(default)]
    pub nested_records: Vec<BehaviorNestedRecordRead>,
    #[serde(default)]
    pub latest_messages: Vec<BehaviorLatestMessagesRead>,
    #[serde(default)]
    pub objects: Vec<BehaviorObjectRead>,
    #[serde(default)]
    pub object_bodies: Vec<BehaviorObjectRead>,
    #[serde(default)]
    pub realtime_channel_members: Vec<BehaviorRealtimeChannelMembersRead>,
    #[serde(default)]
    pub realtime_channel_states: Vec<BehaviorRealtimeChannelStateRead>,
    #[serde(default)]
    pub connection_sessions: Vec<BehaviorConnectionSessionsRead>,
    #[serde(default)]
    pub audit_traces: Vec<BehaviorAuditTraceRead>,
    #[serde(default)]
    pub audit_replays: Vec<BehaviorAuditReplayRead>,
}

impl BehaviorReadPlan {
    fn capabilities(&self) -> Vec<BehaviorReadCapability> {
        let mut capabilities = Vec::new();
        if !self.records.is_empty() {
            capabilities.push(BehaviorReadCapability::Records);
        }
        if !self.nested_records.is_empty() {
            capabilities.push(BehaviorReadCapability::NestedRecords);
        }
        if !self.latest_messages.is_empty() {
            capabilities.push(BehaviorReadCapability::LatestMessages);
        }
        if !self.objects.is_empty() {
            capabilities.push(BehaviorReadCapability::Objects);
        }
        if !self.object_bodies.is_empty() {
            capabilities.push(BehaviorReadCapability::ObjectBodies);
        }
        if !self.realtime_channel_members.is_empty() {
            capabilities.push(BehaviorReadCapability::RealtimeChannelMembers);
        }
        if !self.realtime_channel_states.is_empty() {
            capabilities.push(BehaviorReadCapability::RealtimeChannelStates);
        }
        if !self.connection_sessions.is_empty() {
            capabilities.push(BehaviorReadCapability::ConnectionSessions);
        }
        if !self.audit_traces.is_empty() {
            capabilities.push(BehaviorReadCapability::AuditTraces);
        }
        if !self.audit_replays.is_empty() {
            capabilities.push(BehaviorReadCapability::AuditReplays);
        }
        capabilities
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BehaviorAuditTraceKind {
    Room,
    User,
    Object,
    Record,
    NestedRecord,
    Path,
    ClientMutation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorAuditTraceRead {
    pub kind: BehaviorAuditTraceKind,
    pub id: Option<String>,
    pub table: Option<String>,
    pub record_key: Option<String>,
    pub parent_key: Option<String>,
    pub nested: Option<String>,
    pub nested_key: Option<String>,
    pub path: Option<String>,
    pub client_mutation_id: Option<String>,
    pub after_lsn: Option<u64>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BehaviorAuditReplayKind {
    User,
    Object,
    Record,
    NestedRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorAuditReplayRead {
    pub kind: BehaviorAuditReplayKind,
    pub id: Option<String>,
    pub table: Option<String>,
    pub record_key: Option<String>,
    pub parent_key: Option<String>,
    pub nested: Option<String>,
    pub nested_key: Option<String>,
    pub at_lsn: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorRecordRead {
    pub table: String,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorNestedRecordRead {
    pub table: String,
    pub parent_key: String,
    pub nested: String,
    pub nested_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorLatestMessagesRead {
    pub room_id: String,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorObjectRead {
    pub object_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorRealtimeChannelStateRead {
    pub channel_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorRealtimeChannelMembersRead {
    pub channel_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorConnectionSessionsRead {
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub transport: Option<ConnectionTransport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorInvokeOutput {
    #[serde(default)]
    pub commands: Vec<BehaviorCommand>,
    pub result: Value,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BehaviorCommand {
    SendMessage {
        room_id: String,
        body: String,
        #[serde(default)]
        attachments: Vec<String>,
        #[serde(default)]
        durability: crate::model::Durability,
    },
    PublishVolatile {
        room_id: String,
        name: String,
        payload: Value,
    },
    PublishUserVolatile {
        user_id: String,
        name: String,
        payload: Value,
    },
    PublishUserEvent {
        user_id: String,
        name: String,
        payload: Value,
        #[serde(default)]
        durability: crate::model::Durability,
        client_mutation_id: Option<String>,
    },
    PutObject {
        body_base64: String,
        content_type: String,
        object_id: Option<String>,
        client_mutation_id: Option<String>,
    },
    DeleteObject {
        object_id: String,
        force: Option<bool>,
        client_mutation_id: Option<String>,
    },
    UpsertRecord {
        table: String,
        key: String,
        value: Value,
        #[serde(default)]
        durability: crate::model::Durability,
        expected_lsn: Option<u64>,
    },
    DeleteRecord {
        table: String,
        key: String,
        #[serde(default)]
        durability: crate::model::Durability,
        expected_lsn: Option<u64>,
    },
    RecordTransaction {
        operations: Vec<BehaviorRecordTransactionOperation>,
        #[serde(default)]
        durability: crate::model::Durability,
    },
    BroadcastRealtimeChannel {
        channel_id: String,
        kind: String,
        payload: Value,
        include_self: Option<bool>,
    },
    UpdateRealtimeChannelState {
        channel_id: String,
        state: Value,
        expected_version: Option<u64>,
    },
    UpdateRealtimePresence {
        channel_id: String,
        metadata: Value,
        session_id: Option<String>,
    },
    DisconnectConnections {
        user_id: Option<String>,
        session_id: Option<String>,
        reason: Option<String>,
    },
    ActivateRuntimeRecords {
        table: String,
        parent_key: Option<String>,
        nested: Option<String>,
        key: Option<String>,
        #[serde(default)]
        keys: Vec<String>,
        index_name: Option<String>,
        value: Option<Value>,
        values: Option<Value>,
        lower: Option<Value>,
        upper: Option<Value>,
        lower_values: Option<Value>,
        upper_values: Option<Value>,
        after_key: Option<String>,
        after_cursor: Option<String>,
        order: Option<String>,
        limit: Option<usize>,
        predicate: Option<Value>,
    },
    EvictRuntimeRecords {
        table: String,
        parent_key: Option<String>,
        nested: Option<String>,
        key: Option<String>,
        #[serde(default)]
        keys: Vec<String>,
        after_key: Option<String>,
        limit: Option<usize>,
    },
    ActivateRuntimeRoom {
        room_id: String,
        limit: Option<usize>,
    },
    EvictRuntimeRoom {
        room_id: String,
        limit: Option<usize>,
    },
    ScheduleActorReminder {
        kind: crate::actor::ActorKind,
        key: String,
        reminder_id: Option<String>,
        due_at_ms: Option<u64>,
        delay_ms: Option<u64>,
        payload: Option<Value>,
    },
    RequestHostHttp {
        request_id: Option<String>,
        method: String,
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
        body: Option<Value>,
        body_base64: Option<String>,
        timeout_ms: Option<u64>,
        actor_kind: crate::actor::ActorKind,
        actor_key: String,
        reminder_id: Option<String>,
        continuation: Value,
    },
}

impl BehaviorCommand {
    fn capability(&self) -> BehaviorCommandCapability {
        match self {
            BehaviorCommand::SendMessage { .. } => BehaviorCommandCapability::SendMessage,
            BehaviorCommand::PublishVolatile { .. } => BehaviorCommandCapability::PublishVolatile,
            BehaviorCommand::PublishUserVolatile { .. } => {
                BehaviorCommandCapability::PublishUserVolatile
            }
            BehaviorCommand::PublishUserEvent { .. } => BehaviorCommandCapability::PublishUserEvent,
            BehaviorCommand::PutObject { .. } => BehaviorCommandCapability::PutObject,
            BehaviorCommand::DeleteObject { .. } => BehaviorCommandCapability::DeleteObject,
            BehaviorCommand::UpsertRecord { .. } => BehaviorCommandCapability::UpsertRecord,
            BehaviorCommand::DeleteRecord { .. } => BehaviorCommandCapability::DeleteRecord,
            BehaviorCommand::RecordTransaction { .. } => {
                BehaviorCommandCapability::RecordTransaction
            }
            BehaviorCommand::BroadcastRealtimeChannel { .. } => {
                BehaviorCommandCapability::BroadcastRealtimeChannel
            }
            BehaviorCommand::UpdateRealtimeChannelState { .. } => {
                BehaviorCommandCapability::UpdateRealtimeChannelState
            }
            BehaviorCommand::UpdateRealtimePresence { .. } => {
                BehaviorCommandCapability::UpdateRealtimePresence
            }
            BehaviorCommand::DisconnectConnections { .. } => {
                BehaviorCommandCapability::DisconnectConnections
            }
            BehaviorCommand::ActivateRuntimeRecords { .. } => {
                BehaviorCommandCapability::ActivateRuntimeRecords
            }
            BehaviorCommand::EvictRuntimeRecords { .. } => {
                BehaviorCommandCapability::EvictRuntimeRecords
            }
            BehaviorCommand::ActivateRuntimeRoom { .. } => {
                BehaviorCommandCapability::ActivateRuntimeRoom
            }
            BehaviorCommand::EvictRuntimeRoom { .. } => BehaviorCommandCapability::EvictRuntimeRoom,
            BehaviorCommand::ScheduleActorReminder { .. } => {
                BehaviorCommandCapability::ScheduleActorReminder
            }
            BehaviorCommand::RequestHostHttp { .. } => BehaviorCommandCapability::RequestHostHttp,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BehaviorRecordTransactionOperation {
    Upsert {
        table: String,
        key: String,
        value: Value,
        expected_lsn: Option<u64>,
    },
    Delete {
        table: String,
        key: String,
        expected_lsn: Option<u64>,
    },
    NestedUpsert {
        table: String,
        parent_key: String,
        nested: String,
        nested_key: String,
        value: Value,
        expected_lsn: Option<u64>,
    },
    NestedDelete {
        table: String,
        parent_key: String,
        nested: String,
        nested_key: String,
        expected_lsn: Option<u64>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::fs;
    use uuid::Uuid;

    #[test]
    fn behavior_epoch_deadline_ticks_tracks_fuel_budget() {
        assert_eq!(behavior_epoch_deadline_ticks(0), 1);
        assert_eq!(behavior_epoch_deadline_ticks(999), 1);
        assert_eq!(behavior_epoch_deadline_ticks(1_000), 1);
        assert_eq!(behavior_epoch_deadline_ticks(10_000), 10);
        assert_eq!(
            behavior_epoch_deadline_ticks(u64::MAX),
            BEHAVIOR_EPOCH_DEADLINE_MAX_TICKS
        );
    }

    #[test]
    fn postcard_behavior_abi_frames_json_payloads() {
        let request = BehaviorInvokeRequest {
            behavior: "echo".to_string(),
            mutation: "echo.send".to_string(),
            user_id: Some("alice".to_string()),
            client_mutation_id: None,
            input: serde_json::json!({ "roomId": "room-1" }),
            read: BehaviorReadPlan::default(),
            context: Value::Null,
        };
        let encoded = encode_behavior_guest_input(&request, BehaviorAbiEncoding::Postcard)
            .expect("encode postcard input");
        let frame =
            postcard::from_bytes::<BehaviorPostcardFrame>(&encoded).expect("decode postcard frame");
        assert_eq!(frame.encoding, BehaviorPostcardPayloadEncoding::Json);
        let decoded_request = serde_json::from_slice::<Value>(&frame.payload).expect("decode json");
        assert_eq!(decoded_request["behavior"], serde_json::json!("echo"));
        assert_eq!(decoded_request["mutation"], serde_json::json!("echo.send"));
        assert_eq!(decoded_request["userId"], serde_json::json!("alice"));
        assert_eq!(
            decoded_request["input"],
            serde_json::json!({ "roomId": "room-1" })
        );
        assert_eq!(decoded_request["read"]["records"], serde_json::json!([]));

        let output = BehaviorInvokeOutput {
            commands: Vec::new(),
            result: serde_json::json!({ "ok": true }),
        };
        let output_json = serde_json::to_vec(&output).expect("encode output json");
        let output_frame = postcard::to_allocvec(&BehaviorPostcardFrame::json(output_json.clone()))
            .expect("encode output frame");
        assert_eq!(
            decode_behavior_guest_output(&output_frame, BehaviorAbiEncoding::Postcard)
                .expect("decode output")
                .result,
            serde_json::json!({ "ok": true })
        );
        let legacy_output_frame =
            postcard::to_allocvec(&LegacyBehaviorPostcardFrame { json: output_json })
                .expect("encode legacy output frame");
        assert_eq!(
            decode_behavior_guest_output(&legacy_output_frame, BehaviorAbiEncoding::Postcard)
                .expect("decode legacy output")
                .result,
            serde_json::json!({ "ok": true })
        );
        let typed_schema_payload = postcard::to_allocvec(&TypedBehaviorInvokeOutput {
            commands: Vec::new(),
            result: PostcardJsonValue::try_from(output.result.clone())
                .expect("encode typed result"),
        })
        .expect("encode typed output");
        let typed_schema_frame = postcard::to_allocvec(&BehaviorPostcardFrame {
            encoding: BehaviorPostcardPayloadEncoding::TypedSchema,
            payload: typed_schema_payload,
        })
        .expect("encode typed-schema frame");
        assert_eq!(
            decode_behavior_guest_output(&typed_schema_frame, BehaviorAbiEncoding::Postcard)
                .expect("decode typed-schema output")
                .result,
            serde_json::json!({ "ok": true })
        );

        let typed_encoded =
            encode_behavior_guest_input(&request, BehaviorAbiEncoding::PostcardTypedSchema)
                .expect("encode typed-schema postcard input");
        let typed_frame = postcard::from_bytes::<BehaviorPostcardFrame>(&typed_encoded)
            .expect("decode typed-schema postcard frame");
        assert_eq!(
            typed_frame.encoding,
            BehaviorPostcardPayloadEncoding::TypedSchema
        );
        let typed_request =
            postcard::from_bytes::<TypedBehaviorInvokeRequest>(&typed_frame.payload)
                .expect("decode typed-schema request");
        assert_eq!(typed_request.behavior, "echo");
        assert_eq!(typed_request.mutation, "echo.send");
        assert_eq!(typed_request.user_id.as_deref(), Some("alice"));
        assert_eq!(
            Value::from(typed_request.input),
            serde_json::json!({ "roomId": "room-1" })
        );
    }

    #[tokio::test]
    async fn reload_rejects_invalid_wasm_and_preserves_loaded_behaviors() {
        let root = temp_behavior_root("invalid-reload").await;
        write_behavior(&root, "echo", "echo", valid_wasm()).await;
        let runtime = BehaviorRuntime::load_checked(root.clone(), |_| Ok(()))
            .await
            .expect("load valid behavior");
        assert_eq!(runtime.list().await.len(), 1);
        assert_eq!(runtime.epoch(), 1);

        write_behavior(&root, "broken", "broken", b"not wasm".to_vec()).await;
        let error = runtime
            .reload_checked(root.clone(), |_| Ok(()))
            .await
            .expect_err("invalid wasm should fail reload");

        assert!(
            error
                .to_string()
                .contains("compile wasm module for behavior 'broken'"),
            "unexpected error: {error:#}"
        );
        let behaviors = runtime.list().await;
        assert_eq!(behaviors.len(), 1);
        assert_eq!(behaviors[0].name, "echo");
        assert_eq!(runtime.epoch(), 1);
        cleanup(root).await;
    }

    #[tokio::test]
    async fn successful_reload_advances_behavior_epoch() {
        let root = temp_behavior_root("epoch-reload").await;
        write_behavior(&root, "echo", "echo", valid_wasm()).await;
        let runtime = BehaviorRuntime::load_checked(root.clone(), |_| Ok(()))
            .await
            .expect("load behavior");
        assert_eq!(runtime.epoch(), 1);
        assert_eq!(loaded_behavior_epoch(&runtime, "echo").await, Some(1));

        runtime
            .reload_checked(root.clone(), |_| Ok(()))
            .await
            .expect("reload behavior");

        assert_eq!(runtime.epoch(), 2);
        assert_eq!(loaded_behavior_epoch(&runtime, "echo").await, Some(2));
        cleanup(root).await;
    }

    #[tokio::test]
    async fn behavior_runtime_status_reports_instance_pool() {
        let root = temp_behavior_root("runtime-status").await;
        write_behavior(&root, "echo", "echo", resident_counter_wasm()).await;
        let runtime = BehaviorRuntime::load_checked(root.clone(), |_| Ok(()))
            .await
            .expect("load behavior");

        let before = runtime.status().await;
        assert_eq!(before.epoch, 1);
        assert_eq!(before.behavior_count, 1);
        assert!(before.fuel_enabled);
        assert_eq!(before.instance_pool_max, DEFAULT_BEHAVIOR_INSTANCE_POOL_MAX);
        assert_eq!(before.pooled_instances, 0);
        assert_eq!(before.counters.invocations, 0);
        assert_eq!(before.counters.successes, 0);
        assert_eq!(before.behaviors[0].name, "echo");
        assert!(before.behaviors[0].fuel_enabled);
        assert_eq!(before.behaviors[0].pooled_instances, 0);
        assert_eq!(before.behaviors[0].counters.invocations, 0);

        runtime
            .invoke(behavior_request("echo", "echo"))
            .await
            .expect("invoke behavior");

        let after = runtime.status().await;
        assert_eq!(after.pooled_instances, 1);
        assert_eq!(after.behaviors[0].pooled_instances, 1);
        assert_eq!(
            after.behaviors[0].instance_pool_max,
            DEFAULT_BEHAVIOR_INSTANCE_POOL_MAX
        );
        assert_eq!(after.counters.invocations, 1);
        assert_eq!(after.counters.handle_message_invocations, 1);
        assert_eq!(after.counters.successes, 1);
        assert_eq!(after.counters.instances_created, 1);
        assert_eq!(after.counters.instances_returned, 1);
        assert_eq!(after.behaviors[0].counters.invocations, 1);
        cleanup(root).await;
    }

    #[tokio::test]
    async fn behavior_runtime_can_disable_fuel_instrumentation() {
        let root = temp_behavior_root("fuel-disabled").await;
        write_behavior(&root, "echo", "echo", resident_counter_wasm()).await;
        let runtime = BehaviorRuntime::load_checked_with_config(
            root.clone(),
            |_| Ok(()),
            BehaviorRuntimeConfig {
                fuel_enabled: false,
                instance_pool_max: DEFAULT_BEHAVIOR_INSTANCE_POOL_MAX,
                pooling_total_core_instances: DEFAULT_BEHAVIOR_POOL_TOTAL_CORE_INSTANCES,
                pooling_total_memories: DEFAULT_BEHAVIOR_POOL_TOTAL_MEMORIES,
                pooling_total_tables: DEFAULT_BEHAVIOR_POOL_TOTAL_TABLES,
            },
        )
        .await
        .expect("load behavior");

        let response = runtime
            .invoke(behavior_request("echo", "echo"))
            .await
            .expect("invoke without fuel instrumentation");
        assert_eq!(response.output.result["n"], serde_json::json!(2));

        let status = runtime.status().await;
        assert!(!status.fuel_enabled);
        assert!(!status.config.fuel_enabled);
        assert_eq!(status.pooled_instances, 1);
        assert!(!status.behaviors[0].fuel_enabled);
        assert_eq!(status.counters.successes, 1);
        cleanup(root).await;
    }

    #[tokio::test]
    async fn invoke_runs_on_activate_once_and_reuses_resident_instance() {
        let root = temp_behavior_root("resident-instance").await;
        write_behavior(&root, "echo", "echo", resident_counter_wasm()).await;
        let runtime = BehaviorRuntime::load_checked(root.clone(), |_| Ok(()))
            .await
            .expect("load behavior");

        let first = runtime
            .invoke(behavior_request("echo", "echo"))
            .await
            .expect("first invoke");
        let second = runtime
            .invoke(behavior_request("echo", "echo"))
            .await
            .expect("second invoke");

        assert_eq!(first.output.result["n"], serde_json::json!(2));
        assert_eq!(second.output.result["n"], serde_json::json!(3));
        assert_eq!(second.metadata.epoch, 1);
        cleanup(root).await;
    }

    #[tokio::test]
    async fn guest_deactivate_hook_runs_before_instance_is_discarded() {
        let root = temp_behavior_root("deactivate-instance").await;
        write_behavior(&root, "echo", "echo", resident_counter_wasm()).await;
        let runtime = BehaviorRuntime::load_checked(root.clone(), |_| Ok(()))
            .await
            .expect("load behavior");
        let loaded = {
            let behaviors = runtime.behaviors.read().await;
            behaviors.get("echo").cloned().expect("loaded behavior")
        };
        let max_fuel = loaded
            .manifest
            .max_fuel
            .unwrap_or(DEFAULT_BEHAVIOR_MAX_FUEL);
        let mut guest = loaded
            .take_guest_instance(&runtime.engine, &loaded.manifest)
            .expect("take guest");

        let before = guest
            .invoke(&behavior_request("echo", "echo"), max_fuel)
            .expect("invoke before deactivate");
        guest
            .deactivate(max_fuel, loaded.fuel_enabled)
            .expect("deactivate guest");
        let after = guest
            .invoke(&behavior_request("echo", "echo"), max_fuel)
            .expect("invoke after deactivate");

        assert_eq!(before.result["n"], serde_json::json!(2));
        assert_eq!(after.result["n"], serde_json::json!(4));
        cleanup(root).await;
    }

    #[tokio::test]
    async fn handle_message_entrypoint_and_unknown_message_are_supported() {
        let root = temp_behavior_root("message-entrypoint").await;
        write_behavior(&root, "echo", "echo", message_entrypoint_wasm()).await;
        let runtime = BehaviorRuntime::load_checked(root.clone(), |_| Ok(()))
            .await
            .expect("load behavior");

        let known = runtime
            .invoke(behavior_request("echo", "echo"))
            .await
            .expect("known message");
        let unknown = runtime
            .invoke(behavior_request("echo", "stale.echo"))
            .await
            .expect("unknown message");

        assert_eq!(known.output.result["entry"], serde_json::json!("handle"));
        assert_eq!(unknown.output.result["entry"], serde_json::json!("unknown"));
        cleanup(root).await;
    }

    #[test]
    fn schedule_actor_reminder_command_decodes_with_capability() {
        let command: BehaviorCommand = serde_json::from_value(serde_json::json!({
            "type": "scheduleActorReminder",
            "kind": "scope",
            "key": "table:rooms/bucket:00",
            "reminderId": "continue-1",
            "delayMs": 25,
            "payload": { "mutation": "continue", "callChainId": "chain-1" }
        }))
        .expect("decode scheduleActorReminder command");

        assert_eq!(
            command.capability(),
            BehaviorCommandCapability::ScheduleActorReminder
        );
        match command {
            BehaviorCommand::ScheduleActorReminder {
                kind,
                key,
                reminder_id,
                due_at_ms,
                delay_ms,
                payload,
            } => {
                assert_eq!(kind, crate::actor::ActorKind::Scope);
                assert_eq!(key, "table:rooms/bucket:00");
                assert_eq!(reminder_id.as_deref(), Some("continue-1"));
                assert_eq!(due_at_ms, None);
                assert_eq!(delay_ms, Some(25));
                assert_eq!(
                    payload
                        .as_ref()
                        .and_then(|value| value.get("callChainId"))
                        .and_then(|value| value.as_str()),
                    Some("chain-1")
                );
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn request_host_http_command_decodes_with_capability_and_scope() {
        let command: BehaviorCommand = serde_json::from_value(serde_json::json!({
            "type": "requestHostHttp",
            "requestId": "http-1",
            "method": "POST",
            "url": "https://api.example.test/v1/jobs",
            "headers": { "authorization": "Bearer test" },
            "body": { "job": "compact" },
            "timeoutMs": 1000,
            "actorKind": "scope",
            "actorKey": "table:jobs/bucket:00",
            "reminderId": "http-1-done",
            "continuation": {
                "type": "behaviorContinuation",
                "behavior": "jobs",
                "mutation": "onHttpResult"
            }
        }))
        .expect("decode requestHostHttp command");

        assert_eq!(
            command.capability(),
            BehaviorCommandCapability::RequestHostHttp
        );
        let manifest = BehaviorManifest {
            name: "jobs".to_string(),
            version: "1".to_string(),
            module_path: PathBuf::from("jobs.wasm"),
            abi_encoding: BehaviorAbiEncoding::Json,
            mutations: vec!["run".to_string()],
            inputs: BTreeMap::new(),
            reads: None,
            record_scopes: None,
            object_scopes: None,
            realtime_scopes: None,
            connection_scopes: None,
            user_scopes: None,
            event_scopes: None,
            host_http_scopes: Some(BehaviorHostHttpScopes {
                allow_url_prefixes: vec!["https://api.example.test/v1/".to_string()],
            }),
            commands: vec![BehaviorCommandCapability::RequestHostHttp],
            max_fuel: None,
        };

        validate_command_capabilities(&manifest, std::slice::from_ref(&command))
            .expect("capability allowed");
        validate_host_http_scopes_for_commands(&manifest, std::slice::from_ref(&command))
            .expect("url prefix allowed");

        let mut denied = manifest.clone();
        denied.host_http_scopes = Some(BehaviorHostHttpScopes {
            allow_url_prefixes: vec!["https://other.example.test/".to_string()],
        });
        let error = validate_host_http_scopes_for_commands(&denied, std::slice::from_ref(&command))
            .expect_err("url prefix should be denied");
        assert!(
            error
                .to_string()
                .contains("not allowed to request host HTTP")
        );

        let mut missing = manifest;
        missing.host_http_scopes = None;
        let error =
            validate_host_http_scopes_for_commands(&missing, std::slice::from_ref(&command))
                .expect_err("missing hostHttpScopes should be denied");
        assert!(
            error.to_string().contains("without hostHttpScopes"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn load_rejects_duplicate_behavior_names() {
        let root = temp_behavior_root("duplicate").await;
        write_behavior(&root, "one", "echo", valid_wasm()).await;
        write_behavior(&root, "two", "echo", valid_wasm()).await;

        let error = match BehaviorRuntime::load_checked(root.clone(), |_| Ok(())).await {
            Ok(_) => panic!("duplicate behavior names should fail load"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("duplicate behavior manifest 'echo'"),
            "unexpected error: {error:#}"
        );
        cleanup(root).await;
    }

    async fn loaded_behavior_epoch(runtime: &BehaviorRuntime, name: &str) -> Option<u64> {
        runtime
            .behaviors
            .read()
            .await
            .get(name)
            .map(|behavior| behavior.epoch)
    }

    fn valid_wasm() -> Vec<u8> {
        wat::parse_str("(module)").expect("valid wat")
    }

    fn resident_counter_wasm() -> Vec<u8> {
        wat::parse_str(
            r#"
            (module
              (memory (export "memory") 1)
              (global $next (mut i32) (i32.const 1024))
              (global $count (mut i32) (i32.const 0))
              (func (export "on_activate")
                global.get $count
                i32.const 1
                i32.add
                global.set $count)
              (func (export "on_deactivate")
                global.get $count
                i32.const 1
                i32.add
                global.set $count)
              (func (export "alloc") (param $len i32) (result i32)
                (local $ptr i32)
                global.get $next
                local.set $ptr
                global.get $next
                local.get $len
                i32.add
                global.set $next
                local.get $ptr)
              (func (export "dealloc") (param i32 i32))
              (func (export "invoke") (param i32 i32) (result i64)
                global.get $count
                i32.const 1
                i32.add
                global.set $count
                i32.const 2077
                i32.const 48
                global.get $count
                i32.add
                i32.store8
                i64.const 2048
                i64.const 32
                i64.shl
                i64.const 32
                i64.or)
              (data (i32.const 2048) "{\"commands\":[],\"result\":{\"n\":0}}"))
            "#,
        )
        .expect("valid resident counter wat")
    }

    fn message_entrypoint_wasm() -> Vec<u8> {
        let handle_output = r#"{"commands":[],"result":{"entry":"handle"}}"#;
        let unknown_output = r#"{"commands":[],"result":{"entry":"unknown"}}"#;
        wat::parse_str(format!(
            r#"
            (module
              (memory (export "memory") 1)
              (global $next (mut i32) (i32.const 1024))
              (func (export "alloc") (param $len i32) (result i32)
                (local $ptr i32)
                global.get $next
                local.set $ptr
                global.get $next
                local.get $len
                i32.add
                global.set $next
                local.get $ptr)
              (func (export "dealloc") (param i32 i32))
              (func (export "handle_message") (param i32 i32) (result i64)
                i64.const 2048
                i64.const 32
                i64.shl
                i64.const {handle_len}
                i64.or)
              (func (export "on_unknown_message") (param i32 i32) (result i64)
                i64.const 2112
                i64.const 32
                i64.shl
                i64.const {unknown_len}
                i64.or)
              (data (i32.const 2048) "{handle_output}")
              (data (i32.const 2112) "{unknown_output}"))
            "#,
            handle_len = handle_output.len(),
            unknown_len = unknown_output.len(),
            handle_output = handle_output.replace('"', "\\\""),
            unknown_output = unknown_output.replace('"', "\\\""),
        ))
        .expect("valid message entrypoint wat")
    }

    fn behavior_request(behavior: &str, mutation: &str) -> BehaviorInvokeRequest {
        BehaviorInvokeRequest {
            behavior: behavior.to_string(),
            mutation: mutation.to_string(),
            user_id: None,
            client_mutation_id: None,
            input: serde_json::json!({}),
            read: BehaviorReadPlan::default(),
            context: serde_json::Value::Null,
        }
    }

    async fn temp_behavior_root(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("nextdb-behavior-{name}-{}", Uuid::now_v7()));
        fs::create_dir_all(&path)
            .await
            .expect("create behavior temp root");
        path
    }

    async fn write_behavior(root: &std::path::Path, dir: &str, name: &str, wasm: Vec<u8>) {
        let behavior_dir = root.join(dir);
        fs::create_dir_all(&behavior_dir)
            .await
            .expect("create behavior dir");
        fs::write(behavior_dir.join("module.wasm"), wasm)
            .await
            .expect("write wasm");
        fs::write(
            behavior_dir.join("nextdb.behavior.json"),
            serde_json::json!({
                "name": name,
                "version": "0.1.0",
                "modulePath": "module.wasm",
                "mutations": ["echo"]
            })
            .to_string(),
        )
        .await
        .expect("write manifest");
    }

    async fn cleanup(path: PathBuf) {
        let _ = fs::remove_dir_all(path).await;
    }
}
