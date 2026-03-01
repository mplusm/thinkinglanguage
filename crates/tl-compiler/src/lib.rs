// ThinkingLanguage — Bytecode Compiler & VM
// Licensed under MIT OR Apache-2.0
//
// Phase 2: Compiles TL AST to register-based bytecode and executes it
// in a virtual machine for 5-10x speedup over the tree-walking interpreter.

// JIT runtime uses raw pointers by design (Cranelift FFI).
// VmValue is a large enum by design (register-based VM needs it).
#![allow(
    clippy::large_enum_variant,
    clippy::should_implement_trait,
    clippy::type_complexity
)]

#[cfg(feature = "async-runtime")]
pub mod async_runtime;
pub mod chunk;
pub mod compiler;
#[cfg(feature = "native")]
#[allow(
    clippy::not_unsafe_ptr_arg_deref,
    improper_ctypes_definitions,
    dead_code
)]
pub mod jit;
#[cfg(feature = "native")]
#[allow(clippy::not_unsafe_ptr_arg_deref, improper_ctypes_definitions)]
pub mod jit_runtime;
pub mod module;
pub mod opcode;
#[cfg(feature = "python")]
pub mod python;
pub mod schema;
pub mod security;
pub mod value;
pub mod vm;

pub use chunk::Prototype;
pub use compiler::{compile, compile_with_source};
#[cfg(feature = "native")]
pub use jit::JitCompiler;
pub use value::VmValue;
pub use vm::Vm;
