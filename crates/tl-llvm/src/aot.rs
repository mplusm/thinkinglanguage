// ThinkingLanguage — AOT Compilation
// MCJIT execution and object file emission via LLVM.

use std::error::Error;
use std::path::Path;

use inkwell::context::Context;
use inkwell::execution_engine::JitFunction;
use inkwell::targets::{
    CodeModel, FileType, InitializationConfig, RelocMode, Target, TargetMachine,
};
use inkwell::OptimizationLevel;

use tl_compiler::chunk::Prototype;
use tl_compiler::{Vm, VmValue, compile_with_source};
use tl_parser::parse;

use crate::codegen::LlvmCodegen;
use crate::runtime::VmContext;

/// Type of the compiled TL function: (ctx, args, nargs, retval) -> status
type TlFnType = unsafe extern "C" fn(*mut VmContext, *const VmValue, i64, *mut VmValue) -> i64;

/// Compile and immediately execute TL source code via LLVM MCJIT.
pub fn compile_and_run(source: &str, file_path: Option<&str>) -> Result<(), Box<dyn Error>> {
    // 1. Parse
    let program = parse(source).map_err(|e| format!("Parse error: {e}"))?;

    // 2. Compile to Prototype
    let proto = compile_with_source(&program, source)
        .map_err(|e| format!("Compile error: {e}"))?;

    // 3. Create LLVM context and compile
    let context = Context::create();
    let codegen = LlvmCodegen::new(&context, "tl_main_module");
    codegen.declare_runtime_helpers();

    let proto_ptr = &proto as *const Prototype;
    let _function = codegen.compile_prototype(&proto, proto_ptr)
        .map_err(|e| format!("LLVM codegen error: {e}"))?;

    // Verify module
    codegen.verify().map_err(|e| format!("LLVM verification error: {e}"))?;

    // 4. Create MCJIT execution engine
    let ee = codegen.module.create_jit_execution_engine(OptimizationLevel::Default)
        .map_err(|e| format!("Failed to create execution engine: {e}"))?;

    // 5. Register runtime helper symbols
    register_runtime_symbols_ee(&ee);

    // 6. Get the compiled function pointer
    let fn_name = if proto.name.is_empty() { "tl_main" } else { &format!("tl_fn_{}", proto.name) };
    let compiled_fn: JitFunction<TlFnType> = unsafe {
        ee.get_function(fn_name)
            .map_err(|e| format!("Failed to get function '{fn_name}': {e}"))?
    };

    // 7. Set up VM context
    let mut vm = Vm::new();
    if let Some(path) = file_path {
        vm.file_path = Some(path.to_string());
    }

    let mut vm_ctx = VmContext {
        vm: &mut vm as *mut Vm,
        prototype: proto_ptr,
    };

    // 8. Call the native function
    let mut retval = VmValue::None;
    let status = unsafe {
        compiled_fn.call(
            &mut vm_ctx as *mut VmContext,
            std::ptr::null(),
            0,
            &mut retval as *mut VmValue,
        )
    };

    if status != 0 {
        return Err("LLVM execution error".into());
    }

    Ok(())
}

/// Compile TL source to an LLVM IR string (for debugging/inspection).
pub fn compile_to_ir(source: &str) -> Result<String, Box<dyn Error>> {
    let program = parse(source).map_err(|e| format!("Parse error: {e}"))?;
    let proto = compile_with_source(&program, source)
        .map_err(|e| format!("Compile error: {e}"))?;

    let context = Context::create();
    let codegen = LlvmCodegen::new(&context, "tl_module");
    codegen.declare_runtime_helpers();

    let proto_ptr = &proto as *const Prototype;
    codegen.compile_prototype(&proto, proto_ptr)
        .map_err(|e| format!("LLVM codegen error: {e}"))?;

    codegen.verify().map_err(|e| format!("LLVM verification error: {e}"))?;

    Ok(codegen.get_ir())
}

/// Compile TL source to a native object file.
pub fn compile_to_object(source: &str, output: &Path) -> Result<(), Box<dyn Error>> {
    let program = parse(source).map_err(|e| format!("Parse error: {e}"))?;
    let proto = compile_with_source(&program, source)
        .map_err(|e| format!("Compile error: {e}"))?;

    let context = Context::create();
    let codegen = LlvmCodegen::new(&context, "tl_module");
    codegen.declare_runtime_helpers();

    let proto_ptr = &proto as *const Prototype;
    codegen.compile_prototype(&proto, proto_ptr)
        .map_err(|e| format!("LLVM codegen error: {e}"))?;

    codegen.verify().map_err(|e| format!("LLVM verification error: {e}"))?;

    // Initialize native target
    Target::initialize_native(&InitializationConfig::default())
        .map_err(|e| format!("Failed to initialize native target: {e}"))?;

    let triple = TargetMachine::get_default_triple();
    let target = Target::from_triple(&triple)
        .map_err(|e| format!("Failed to get target: {e}"))?;

    let cpu = TargetMachine::get_host_cpu_name();
    let features = TargetMachine::get_host_cpu_features();

    let target_machine = target.create_target_machine(
        &triple,
        cpu.to_str().unwrap_or("generic"),
        features.to_str().unwrap_or(""),
        OptimizationLevel::Default,
        RelocMode::Default,
        CodeModel::Default,
    ).ok_or("Failed to create target machine")?;

    target_machine.write_to_file(&codegen.module, FileType::Object, output)
        .map_err(|e| format!("Failed to write object file: {e}"))?;

    Ok(())
}

/// Register all runtime helper symbols with the LLVM dynamic symbol table.
/// Uses LLVMAddSymbol so MCJIT can resolve them during linking.
pub fn register_runtime_symbols_ee(_ee: &inkwell::execution_engine::ExecutionEngine) {
    use crate::runtime::*;
    use std::ffi::CString;

    macro_rules! register {
        ($name:ident) => {
            let cname = CString::new(stringify!($name)).unwrap();
            unsafe {
                llvm_sys::support::LLVMAddSymbol(
                    cname.as_ptr(),
                    $name as *mut std::ffi::c_void,
                );
            }
        };
    }

    register!(tl_rt_add);
    register!(tl_rt_sub);
    register!(tl_rt_mul);
    register!(tl_rt_div);
    register!(tl_rt_mod);
    register!(tl_rt_pow);
    register!(tl_rt_neg);
    register!(tl_rt_not);
    register!(tl_rt_eq);
    register!(tl_rt_neq);
    register!(tl_rt_lt);
    register!(tl_rt_gt);
    register!(tl_rt_lte);
    register!(tl_rt_gte);
    register!(tl_rt_cmp);
    register!(tl_rt_is_truthy);
    register!(tl_rt_concat);
    register!(tl_rt_load_none);
    register!(tl_rt_load_true);
    register!(tl_rt_load_false);
    register!(tl_rt_get_const);
    register!(tl_rt_get_global);
    register!(tl_rt_set_global);
    register!(tl_rt_call);
    register!(tl_rt_call_builtin);
    register!(tl_rt_make_list);
    register!(tl_rt_make_map);
    register!(tl_rt_get_index);
    register!(tl_rt_set_index);
    register!(tl_rt_get_member);
    register!(tl_rt_set_member);
    register!(tl_rt_method_call);
    register!(tl_rt_make_closure);
    register!(tl_rt_vm_exec_op);
    register!(tl_rt_move_value);
}
