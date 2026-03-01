// ThinkingLanguage — Cranelift JIT Compiler
// Compiles hot bytecode prototypes into native code.
//
// Strategy: baseline JIT — all type-dependent operations call runtime helpers.
// This avoids speculative type guards while still eliminating bytecode dispatch overhead.

use std::collections::HashMap;

use cranelift_codegen::settings::{self, Configurable};
use cranelift_frontend::FunctionBuilderContext;
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::Module;

use crate::chunk::Prototype;
use crate::jit_runtime;
use crate::value::VmValue;

/// JIT-compiled function signature:
/// extern "C" fn(args: *const VmValue, nargs: usize) -> VmValue
pub type JitFn = unsafe extern "C" fn(*const VmValue, usize) -> VmValue;

/// The Cranelift JIT compiler for TL functions.
pub struct JitCompiler {
    module: JITModule,
    ctx: cranelift_codegen::Context,
    builder_ctx: FunctionBuilderContext,
    /// Cache of compiled function pointers, keyed by prototype name
    compiled: HashMap<String, *const u8>,
}

impl JitCompiler {
    pub fn new() -> Result<Self, String> {
        let mut flag_builder = settings::builder();
        flag_builder
            .set("use_colocated_libcalls", "false")
            .map_err(|e| format!("JIT settings error: {e}"))?;
        flag_builder
            .set("is_pic", "false")
            .map_err(|e| format!("JIT settings error: {e}"))?;

        let isa_builder = cranelift_codegen::isa::lookup(target_lexicon::Triple::host())
            .map_err(|e| format!("JIT ISA error: {e}"))?;

        let isa = isa_builder
            .finish(settings::Flags::new(flag_builder))
            .map_err(|e| format!("JIT ISA finish error: {e}"))?;

        let mut builder = JITBuilder::with_isa(isa, cranelift_module::default_libcall_names());

        // Register runtime helper symbols
        builder.symbol("tl_rt_add", jit_runtime::tl_rt_add as *const u8);
        builder.symbol("tl_rt_sub", jit_runtime::tl_rt_sub as *const u8);
        builder.symbol("tl_rt_mul", jit_runtime::tl_rt_mul as *const u8);
        builder.symbol("tl_rt_cmp", jit_runtime::tl_rt_cmp as *const u8);
        builder.symbol("tl_rt_is_truthy", jit_runtime::tl_rt_is_truthy as *const u8);

        let module = JITModule::new(builder);
        let ctx = module.make_context();

        Ok(JitCompiler {
            module,
            ctx,
            builder_ctx: FunctionBuilderContext::new(),
            compiled: HashMap::new(),
        })
    }

    /// Check if a function has already been JIT-compiled.
    pub fn get_compiled(&self, name: &str) -> Option<*const u8> {
        self.compiled.get(name).copied()
    }

    /// Compile a prototype to native code and return the function pointer.
    /// Returns None if the function is too complex to JIT (e.g. uses table ops).
    pub fn compile_function(&mut self, proto: &Prototype) -> Result<Option<*const u8>, String> {
        // For now, only JIT simple numeric functions (no table ops, no closures)
        if !proto.upvalue_defs.is_empty() {
            return Ok(None); // Skip closures for now
        }

        // Check for table ops or complex instructions we can't JIT
        for &inst in &proto.code {
            let op = crate::opcode::decode_op(inst);
            match op {
                crate::opcode::Op::TablePipe
                | crate::opcode::Op::Interpolate
                | crate::opcode::Op::NewMap
                | crate::opcode::Op::GetMember => {
                    return Ok(None); // Too complex for baseline JIT
                }
                _ => {}
            }
        }

        // For now, return None — full Cranelift IR generation is complex
        // The JIT infrastructure is set up and ready for incremental improvement
        Ok(None)
    }
}

/// JIT call count threshold — functions called more than this many times get JIT-compiled
pub const JIT_THRESHOLD: u32 = 100;

/// Tiered compilation state, tracked per prototype.
#[derive(Debug, Default)]
pub struct TieringState {
    /// How many times each function has been called
    pub call_counts: HashMap<String, u32>,
}

impl TieringState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a call and return true if the function should be JIT-compiled.
    pub fn record_call(&mut self, name: &str) -> bool {
        let count = self.call_counts.entry(name.to_string()).or_insert(0);
        *count += 1;
        *count == JIT_THRESHOLD
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jit_compiler_creation() {
        let jit = JitCompiler::new();
        assert!(jit.is_ok(), "JIT compiler should initialize");
    }

    #[test]
    fn test_tiering_state() {
        let mut state = TieringState::new();
        for _ in 0..JIT_THRESHOLD - 1 {
            assert!(!state.record_call("test_fn"));
        }
        // The threshold-th call should trigger JIT
        assert!(state.record_call("test_fn"));
        // After threshold, no more triggers
        assert!(!state.record_call("test_fn"));
    }
}
