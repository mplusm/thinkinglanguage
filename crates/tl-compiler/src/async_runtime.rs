// ThinkingLanguage — Async Runtime (tokio-backed)
// Licensed under MIT OR Apache-2.0
//
// Phase 25: Real async I/O implementations for the 9 async builtins.
// Feature-gated behind `async-runtime`.

use std::collections::HashMap;
use std::sync::{Arc, mpsc};

use tl_errors::{RuntimeError, TlError};
use tokio::runtime::Runtime;

use crate::security::SecurityPolicy;
use crate::value::{UpvalueRef, VmClosure, VmTask, VmValue};
use crate::vm::Vm;

fn runtime_err(msg: impl Into<String>) -> TlError {
    TlError::Runtime(RuntimeError {
        message: msg.into(),
        span: None,
        stack_trace: vec![],
    })
}

/// Close all upvalues (Open → Closed) using current stack values.
fn close_upvalues(closure: &VmClosure, stack: &[VmValue]) -> Vec<UpvalueRef> {
    closure
        .upvalues
        .iter()
        .map(|uv| match uv {
            UpvalueRef::Open { stack_index } => {
                let val = stack.get(*stack_index).cloned().unwrap_or(VmValue::None);
                UpvalueRef::Closed(val)
            }
            UpvalueRef::Closed(v) => UpvalueRef::Closed(v.clone()),
        })
        .collect()
}

// ── async_read_file ────────────────────────────────────────────────

pub fn async_read_file_impl(
    rt: &Runtime,
    args: &[VmValue],
    security_policy: &Option<SecurityPolicy>,
) -> Result<VmValue, TlError> {
    let path = match args.first() {
        Some(VmValue::String(s)) => s.clone(),
        _ => return Err(runtime_err("async_read_file() expects a string path")),
    };

    if let Some(policy) = security_policy {
        if !policy.check("file_read") {
            return Err(runtime_err(
                "async_read_file: file_read not allowed by security policy",
            ));
        }
    }

    let (tx, rx) = mpsc::channel();
    rt.spawn(async move {
        let result = tokio::fs::read_to_string(path.as_ref()).await;
        let _ = tx.send(
            result
                .map(|s| VmValue::String(Arc::from(s.as_str())))
                .map_err(|e| format!("async_read_file error: {e}")),
        );
    });
    Ok(VmValue::Task(Arc::new(VmTask::new(rx))))
}

// ── async_write_file ───────────────────────────────────────────────

pub fn async_write_file_impl(
    rt: &Runtime,
    args: &[VmValue],
    security_policy: &Option<SecurityPolicy>,
) -> Result<VmValue, TlError> {
    let path = match args.first() {
        Some(VmValue::String(s)) => s.clone(),
        _ => return Err(runtime_err("async_write_file() expects a string path")),
    };
    let content = match args.get(1) {
        Some(VmValue::String(s)) => s.clone(),
        _ => {
            return Err(runtime_err(
                "async_write_file() expects string content as second argument",
            ));
        }
    };

    if let Some(policy) = security_policy {
        if !policy.check("file_write") {
            return Err(runtime_err(
                "async_write_file: file_write not allowed by security policy",
            ));
        }
    }

    let (tx, rx) = mpsc::channel();
    rt.spawn(async move {
        let result = tokio::fs::write(path.as_ref(), content.as_ref().as_bytes()).await;
        let _ = tx.send(
            result
                .map(|_| VmValue::None)
                .map_err(|e| format!("async_write_file error: {e}")),
        );
    });
    Ok(VmValue::Task(Arc::new(VmTask::new(rx))))
}

// ── async_http_get ─────────────────────────────────────────────────

pub fn async_http_get_impl(
    rt: &Runtime,
    args: &[VmValue],
    security_policy: &Option<SecurityPolicy>,
) -> Result<VmValue, TlError> {
    let url = match args.first() {
        Some(VmValue::String(s)) => s.clone(),
        _ => return Err(runtime_err("async_http_get() expects a string URL")),
    };

    if let Some(policy) = security_policy {
        if !policy.check("network") {
            return Err(runtime_err(
                "async_http_get: network not allowed by security policy",
            ));
        }
    }

    let (tx, rx) = mpsc::channel();
    rt.spawn(async move {
        let result: Result<VmValue, String> = async {
            let body = reqwest::get(url.as_ref())
                .await
                .map_err(|e| format!("async_http_get error: {e}"))?
                .text()
                .await
                .map_err(|e| format!("async_http_get response error: {e}"))?;
            Ok(VmValue::String(Arc::from(body.as_str())))
        }
        .await;
        let _ = tx.send(result);
    });
    Ok(VmValue::Task(Arc::new(VmTask::new(rx))))
}

// ── async_http_post ────────────────────────────────────────────────

pub fn async_http_post_impl(
    rt: &Runtime,
    args: &[VmValue],
    security_policy: &Option<SecurityPolicy>,
) -> Result<VmValue, TlError> {
    let url = match args.first() {
        Some(VmValue::String(s)) => s.clone(),
        _ => return Err(runtime_err("async_http_post() expects a string URL")),
    };
    let body = match args.get(1) {
        Some(VmValue::String(s)) => s.clone(),
        _ => {
            return Err(runtime_err(
                "async_http_post() expects string body as second argument",
            ));
        }
    };

    if let Some(policy) = security_policy {
        if !policy.check("network") {
            return Err(runtime_err(
                "async_http_post: network not allowed by security policy",
            ));
        }
    }

    let (tx, rx) = mpsc::channel();
    rt.spawn(async move {
        let result: Result<VmValue, String> = async {
            let resp = reqwest::Client::new()
                .post(url.as_ref())
                .body(body.to_string())
                .send()
                .await
                .map_err(|e| format!("async_http_post error: {e}"))?
                .text()
                .await
                .map_err(|e| format!("async_http_post response error: {e}"))?;
            Ok(VmValue::String(Arc::from(resp.as_str())))
        }
        .await;
        let _ = tx.send(result);
    });
    Ok(VmValue::Task(Arc::new(VmTask::new(rx))))
}

// ── async_sleep ────────────────────────────────────────────────────

pub fn async_sleep_impl(rt: &Runtime, args: &[VmValue]) -> Result<VmValue, TlError> {
    let ms = match args.first() {
        Some(VmValue::Int(n)) => *n as u64,
        _ => {
            return Err(runtime_err(
                "async_sleep() expects an integer (milliseconds)",
            ));
        }
    };

    let (tx, rx) = mpsc::channel();
    rt.spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
        let _ = tx.send(Ok(VmValue::None));
    });
    Ok(VmValue::Task(Arc::new(VmTask::new(rx))))
}

// ── select ─────────────────────────────────────────────────────────
// Takes 2+ task arguments, returns the result of whichever finishes first.
// Uses std::thread racing since tasks are already mpsc receivers.

pub fn select_impl(args: &[VmValue]) -> Result<VmValue, TlError> {
    if args.len() < 2 {
        return Err(runtime_err("select() expects at least 2 task arguments"));
    }

    // Collect receivers from all tasks
    let mut receivers = Vec::new();
    for (i, arg) in args.iter().enumerate() {
        match arg {
            VmValue::Task(task) => {
                let rx = {
                    let mut guard = task.receiver.lock().unwrap();
                    guard.take()
                };
                match rx {
                    Some(r) => receivers.push(r),
                    None => {
                        return Err(runtime_err(format!("select: task {} already consumed", i)));
                    }
                }
            }
            _ => return Err(runtime_err(format!("select: argument {} is not a task", i))),
        }
    }

    // Race: spawn a thread per receiver, first result wins via a shared channel
    let (winner_tx, winner_rx) = mpsc::channel::<Result<VmValue, String>>();
    for rx in receivers {
        let tx = winner_tx.clone();
        std::thread::spawn(move || {
            if let Ok(result) = rx.recv() {
                let _ = tx.send(result);
            }
        });
    }
    drop(winner_tx);

    // Return a task that resolves to the first result
    Ok(VmValue::Task(Arc::new(VmTask::new(winner_rx))))
}

// ── race_all ───────────────────────────────────────────────────────
// Takes a list of tasks, returns the result of whichever finishes first.

pub fn race_all_impl(args: &[VmValue]) -> Result<VmValue, TlError> {
    let tasks = match args.first() {
        Some(VmValue::List(list)) => list.clone(),
        _ => return Err(runtime_err("race_all() expects a list of tasks")),
    };

    if tasks.is_empty() {
        return Err(runtime_err("race_all() expects a non-empty list of tasks"));
    }

    // Collect receivers
    let mut receivers = Vec::new();
    for (i, task_val) in tasks.iter().enumerate() {
        match task_val {
            VmValue::Task(task) => {
                let rx = {
                    let mut guard = task.receiver.lock().unwrap();
                    guard.take()
                };
                match rx {
                    Some(r) => receivers.push(r),
                    None => {
                        return Err(runtime_err(format!(
                            "race_all: task {} already consumed",
                            i
                        )));
                    }
                }
            }
            _ => {
                return Err(runtime_err(format!(
                    "race_all: element {} is not a task",
                    i
                )));
            }
        }
    }

    // Race: spawn a thread per receiver, first result wins
    let (winner_tx, winner_rx) = mpsc::channel::<Result<VmValue, String>>();
    for rx in receivers {
        let tx = winner_tx.clone();
        std::thread::spawn(move || {
            if let Ok(result) = rx.recv() {
                let _ = tx.send(result);
            }
        });
    }
    drop(winner_tx);

    Ok(VmValue::Task(Arc::new(VmTask::new(winner_rx))))
}

// ── async_map ──────────────────────────────────────────────────────
// Maps a closure over a list concurrently using spawn_blocking.

pub fn async_map_impl(
    rt: &Runtime,
    args: &[VmValue],
    globals: &HashMap<String, VmValue>,
    stack: &[VmValue],
) -> Result<VmValue, TlError> {
    let items = match args.first() {
        Some(VmValue::List(list)) => list.clone(),
        _ => return Err(runtime_err("async_map() expects a list as first argument")),
    };
    let closure = match args.get(1) {
        Some(VmValue::Function(c)) => c.clone(),
        _ => {
            return Err(runtime_err(
                "async_map() expects a function as second argument",
            ));
        }
    };

    let closed_upvalues = close_upvalues(&closure, stack);
    let proto = closure.prototype.clone();
    let globals = globals.clone();

    let (tx, rx) = mpsc::channel();
    rt.spawn(async move {
        let mut handles: Vec<tokio::task::JoinHandle<Result<VmValue, String>>> = Vec::new();
        for item in items {
            let proto = proto.clone();
            let upvalues = closed_upvalues.clone();
            let globals = globals.clone();
            let handle = tokio::task::spawn_blocking(move || {
                let mut vm = Vm::new();
                vm.globals = globals;
                vm.execute_closure_with_args(&proto, &upvalues, &[item])
                    .map_err(|e| format!("{e}"))
            });
            handles.push(handle);
        }

        let mut results = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(Ok(val)) => results.push(val),
                Ok(Err(e)) => {
                    let _ = tx.send(Err(format!("async_map error: {e}")));
                    return;
                }
                Err(e) => {
                    let _ = tx.send(Err(format!("async_map join error: {e}")));
                    return;
                }
            }
        }
        let _ = tx.send(Ok(VmValue::List(results)));
    });

    Ok(VmValue::Task(Arc::new(VmTask::new(rx))))
}

// ── async_filter ───────────────────────────────────────────────────
// Filters a list concurrently using spawn_blocking for the predicate.

pub fn async_filter_impl(
    rt: &Runtime,
    args: &[VmValue],
    globals: &HashMap<String, VmValue>,
    stack: &[VmValue],
) -> Result<VmValue, TlError> {
    let items = match args.first() {
        Some(VmValue::List(list)) => list.clone(),
        _ => {
            return Err(runtime_err(
                "async_filter() expects a list as first argument",
            ));
        }
    };
    let closure = match args.get(1) {
        Some(VmValue::Function(c)) => c.clone(),
        _ => {
            return Err(runtime_err(
                "async_filter() expects a function as second argument",
            ));
        }
    };

    let closed_upvalues = close_upvalues(&closure, stack);
    let proto = closure.prototype.clone();
    let globals = globals.clone();

    let (tx, rx) = mpsc::channel();
    rt.spawn(async move {
        let mut handles: Vec<tokio::task::JoinHandle<Result<VmValue, String>>> = Vec::new();
        for item in items.clone() {
            let proto = proto.clone();
            let upvalues = closed_upvalues.clone();
            let globals = globals.clone();
            let handle = tokio::task::spawn_blocking(move || {
                let mut vm = Vm::new();
                vm.globals = globals;
                vm.execute_closure_with_args(&proto, &upvalues, &[item])
                    .map_err(|e| format!("{e}"))
            });
            handles.push(handle);
        }

        let mut results = Vec::new();
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(val)) => {
                    let keep = matches!(&val, VmValue::Bool(true));
                    if keep {
                        results.push(items[i].clone());
                    }
                }
                Ok(Err(e)) => {
                    let _ = tx.send(Err(format!("async_filter error: {e}")));
                    return;
                }
                Err(e) => {
                    let _ = tx.send(Err(format!("async_filter join error: {e}")));
                    return;
                }
            }
        }
        let _ = tx.send(Ok(VmValue::List(results)));
    });

    Ok(VmValue::Task(Arc::new(VmTask::new(rx))))
}
