// ThinkingLanguage — Bytecode Compiler & VM
// Licensed under MIT OR Apache-2.0
//
// Phase 2: Compiles TL AST to register-based bytecode and executes it
// in a virtual machine for 5-10x speedup over the tree-walking interpreter.

pub mod opcode;
pub mod chunk;
pub mod value;
pub mod compiler;
pub mod vm;
pub mod jit;
pub mod jit_runtime;
pub mod module;
pub mod schema;
pub mod security;
#[cfg(feature = "python")]
pub mod python;

pub use compiler::{compile, compile_with_source};
pub use vm::Vm;
pub use chunk::Prototype;
pub use value::VmValue;
pub use jit::JitCompiler;
