use std::{collections::HashMap, sync::Arc};

type Key = Box<[u8]>;
type Value = Box<[u8]>;

pub struct Env {
    inner: Arc<EnvInner>,
}

struct EnvInner {
    entires: dashmap::DashMap<Key, Value, fxhash::FxBuildHasher>,
}

#[derive(Clone)]
pub struct QueryRunner {
    env: Arc<EnvInner>,
}

pub struct UpdateRunner {
    env: Arc<EnvInner>,
}

pub struct Ctx {
    env: Arc<EnvInner>,
    batch: HashMap<Key, Operation, fxhash::FxBuildHasher>,
}

enum Operation {
    Remove,
    Insert(Value),
}

impl Env {
    /// Create a new environment.
    pub fn new() -> Self {
        Env {
            inner: Arc::new(EnvInner {
                entires: Default::default(),
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
        self.commit(ctx.batch);

        result
    }

    fn commit(&mut self, batch: HashMap<Key, Operation, fxhash::FxBuildHasher>) {
        for (key, operation) in batch {
            match operation {
                Operation::Remove => {
                    self.env.entires.remove(&key);
                }
                Operation::Insert(value) => {
                    self.env.entires.insert(key, value);
                }
            }
        }
    }
}

impl Ctx {
    pub(crate) fn new(inner: Arc<EnvInner>) -> Self {
        Self {
            env: inner,
            batch: Default::default(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Value> {
        match self.batch.get(key) {
            Some(Operation::Remove) => None,
            Some(Operation::Insert(value)) => Some(value.clone()),
            None => self.env.entires.get(key).map(|c| c.clone()),
        }
    }

    pub fn set(&mut self, key: Key, value: Value) {
        self.batch.insert(key, Operation::Insert(value));
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
            println!("balance = {b}");
        });
    });

    recv.recv().unwrap();

    update.run(|ctx| mint(ctx, [0; 32], 1000));
    println!("Called mint as a update.");

    let b = query.run(|ctx| balance(ctx, [0; 32]));
    println!("Current balance = {b}");

    handle.join().unwrap();
}
