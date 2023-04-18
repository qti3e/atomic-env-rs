use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

type Key = Box<[u8]>;
type Value = Box<[u8]>;

pub struct Env {
    inner: Arc<EnvInner>,
}

struct EnvInner {
    version: AtomicUsize,
    entries: dashmap::DashMap<Key, Value, fxhash::FxBuildHasher>,
    snapshot_head: std::sync::RwLock<Option<Snapshot>>,
}

#[derive(Clone)]
struct Snapshot {
    inner: Arc<SnapshotInner>,
}

struct SnapshotInner {
    version: usize,
    entries: dashmap::DashMap<Key, Operation, fxhash::FxBuildHasher>,
    next: std::sync::RwLock<Option<Snapshot>>,
}

#[derive(Clone)]
pub struct QueryRunner {
    env: Arc<EnvInner>,
}

pub struct UpdateRunner {
    env: Arc<EnvInner>,
}

pub struct Ctx {
    /// The host environment which we're running the job from.
    env: Arc<EnvInner>,
    /// If `None` it means the context is running an update and not a query.
    snapshot: Option<Snapshot>,
    /// The current batch that is constructed during this job.
    batch: HashMap<Key, Operation, fxhash::FxBuildHasher>,
}

#[derive(Clone, Debug)]
enum Operation {
    Remove,
    Insert(Value),
}

impl Env {
    /// Create a new environment.
    pub fn new() -> Self {
        Env {
            inner: Arc::new(EnvInner {
                version: AtomicUsize::new(0),
                entries: Default::default(),
                snapshot_head: RwLock::new(None),
            }),
        }
    }

    /// Split the environment to the two query runner object and the update runner to
    /// run queries and updates.
    pub fn split(&mut self) -> (QueryRunner, UpdateRunner) {
        (
            QueryRunner::new(self.inner.clone()),
            UpdateRunner::new(self.inner.clone()),
        )
    }
}

impl QueryRunner {
    pub(crate) fn new(inner: Arc<EnvInner>) -> Self {
        Self { env: inner }
    }

    /// Run a query on the latest state, the context passed to the closure is not going to contain
    /// any intermediary changes happening in other threads.
    pub fn run<F, T>(&self, job: F) -> T
    where
        F: Fn(&mut Ctx) -> T,
    {
        let mut ctx = Ctx::new(self.env.clone());
        ctx.snapshot = Some(self.env.create_new_snapshot());
        job(&mut ctx)
    }
}

impl UpdateRunner {
    pub(crate) fn new(inner: Arc<EnvInner>) -> Self {
        Self { env: inner }
    }

    /// Run an update on the latest state. There can only be one update running on an environment.
    pub fn run<F, T>(&mut self, job: F) -> T
    where
        F: Fn(&mut Ctx) -> T,
    {
        let mut ctx = Ctx::new(self.env.clone());
        let result = job(&mut ctx);

        // commit the batch.
        // TOOD: Skip the commit if the execution failed.

        let env2 = self.env.clone();
        let lock = env2.snapshot_head.write().unwrap();
        let maybe_snapshot = &*lock;

        if let Some(snapshot) = maybe_snapshot {
            self.commit_with_snapshot(ctx.batch, snapshot);
        } else {
            self.commit_without_snapshot(ctx.batch);
        }

        self.env.version.fetch_add(1, Ordering::Relaxed);

        result
    }

    fn commit_with_snapshot(
        &mut self,
        batch: HashMap<Key, Operation, fxhash::FxBuildHasher>,
        snapshot: &Snapshot,
    ) {
        for (key, operation) in batch {
            let previous_value = self.env.entries.get(&key).map(|c| c.clone());

            match (operation, previous_value) {
                (Operation::Remove, None) => {}
                (Operation::Remove, Some(val)) => {
                    snapshot
                        .inner
                        .entries
                        .insert(key.clone(), Operation::Insert(val));

                    self.env.entries.remove(&key);
                }
                (Operation::Insert(value), None) => {
                    snapshot
                        .inner
                        .entries
                        .insert(key.clone(), Operation::Remove);

                    self.env.entries.insert(key, value);
                }
                (Operation::Insert(new_value), Some(prev_value)) => {
                    snapshot
                        .inner
                        .entries
                        .insert(key.clone(), Operation::Insert(prev_value));

                    self.env.entries.insert(key, new_value);
                }
            }
        }
    }

    fn commit_without_snapshot(&mut self, batch: HashMap<Key, Operation, fxhash::FxBuildHasher>) {
        for (key, operation) in batch {
            match operation {
                Operation::Remove => {
                    self.env.entries.remove(&key);
                }
                Operation::Insert(value) => {
                    self.env.entries.insert(key, value);
                }
            }
        }
    }
}

impl Ctx {
    pub(crate) fn new(inner: Arc<EnvInner>) -> Self {
        Self {
            env: inner,
            snapshot: None,
            batch: Default::default(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Value> {
        let mut operation = self.batch.get(key).cloned();

        if operation.is_none() && self.snapshot.is_some() {
            operation = self.snapshot.as_ref().unwrap().get(key);
        }

        match operation {
            Some(Operation::Remove) => return None,
            Some(Operation::Insert(value)) => return Some(value),
            None => self.env.entries.get(key).map(|c| c.clone()),
        }
    }

    pub fn set(&mut self, key: Key, value: Value) {
        self.batch.insert(key, Operation::Insert(value));
    }
}

impl Snapshot {
    pub fn get(&self, key: &[u8]) -> Option<Operation> {
        let result = self.inner.entries.get(key).map(|c| c.clone());

        if result.is_some() {
            result
        } else {
            let maybe_next = { self.inner.next.read().unwrap().clone() };
            if let Some(next) = maybe_next {
                next.get(key)
            } else {
                None
            }
        }
    }
}

impl EnvInner {
    /// Returns the current snapshot if still the same version in the state or create a
    /// new empty one.
    pub fn create_new_snapshot(&self) -> Snapshot {
        {
            let read_lock = self.snapshot_head.read().unwrap();
            let maybe_head = &*read_lock;
            let version = self.version.load(Ordering::Relaxed);

            if let Some(head) = maybe_head {
                if head.inner.version == version {
                    return head.clone();
                }
            }
        }

        let mut lock = self.snapshot_head.write().unwrap();
        let maybe_head = &*lock;
        let version = self.version.load(Ordering::Relaxed);
        let new_head = Snapshot {
            inner: Arc::new(SnapshotInner {
                version,
                entries: Default::default(),
                next: RwLock::new(None),
            }),
        };

        if let Some(head) = maybe_head {
            if head.inner.version == version {
                return head.clone();
            } else {
                let mut head_next_lock = head.inner.next.write().unwrap();
                let prev_next = head_next_lock.replace(new_head.clone());
                debug_assert!(prev_next.is_none());
            }
        }

        lock.replace(new_head.clone());
        new_head
    }
}

// --

pub fn mint(ctx: &mut Ctx, account: [u8; 32], amount: u128) {
    let current_balance = balance(ctx, account);
    let new_balance = current_balance + amount;
    let new_balance_le_bytes = new_balance.to_le_bytes().to_vec().into_boxed_slice();
    ctx.set(account.into(), new_balance_le_bytes);
}

pub fn balance(ctx: &mut Ctx, account: [u8; 32]) -> u128 {
    ctx.get(&account)
        .map(|v| {
            let slice = arrayref::array_ref![v, 0, 16];
            u128::from_le_bytes(*slice)
        })
        .unwrap_or(0)
}

pub fn transfer(ctx: &mut Ctx, from: [u8; 32], to: [u8; 32], amount: u128) {
    // Transfer some funds from the `from` account into the destination account
}

#[test]
fn demo() {
    let mut env = Env::new();
    let (query, mut update) = env.split();

    let b = query.run(|ctx| balance(ctx, [0; 32]));
    println!("Current balance = {b}");

    query.run(|ctx| mint(ctx, [0; 32], 1000));
    println!("Called mint as a query.");

    let b = query.run(|ctx| balance(ctx, [0; 32]));
    println!("Current balance = {b}");

    let q = query.clone();
    let (sender, recv) = std::sync::mpsc::channel::<()>();
    let handle = std::thread::spawn(move || {
        println!("running query...");

        sender.send(()).unwrap();

        q.run(|ctx| {
            dbg!(balance(ctx, [0; 32]));

            std::thread::sleep(std::time::Duration::new(2, 0));
            println!("woke up!");
            let b = balance(ctx, [0; 32]);
            println!("balance after wake up = {b}");
        });
    });

    recv.recv().unwrap();

    update.run(|ctx| mint(ctx, [0; 32], 1000));
    println!("Called mint as a update.");

    let b = query.run(|ctx| balance(ctx, [0; 32]));
    println!("Current balance = {b}");

    handle.join().unwrap();
}
