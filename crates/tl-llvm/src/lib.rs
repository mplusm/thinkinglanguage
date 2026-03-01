// ThinkingLanguage — LLVM Backend (Phase 30)
// AOT native compilation via inkwell (LLVM bindings)

pub mod aot;
pub mod codegen;
pub mod runtime;
pub mod types;

#[cfg(test)]
mod tests {
    use inkwell::context::Context;
    use tl_compiler::chunk::Prototype;
    use tl_compiler::{Vm, VmValue, compile_with_source};
    use tl_parser::parse;

    use crate::codegen::LlvmCodegen;
    use crate::runtime::VmContext;

    /// Helper: compile TL source to Prototype
    fn compile_source(source: &str) -> Prototype {
        let program = parse(source).expect("parse failed");
        compile_with_source(&program, source).expect("compile failed")
    }

    /// Helper: compile TL source to LLVM IR string
    fn source_to_ir(source: &str) -> String {
        let proto = compile_source(source);
        let context = Context::create();
        let codegen = LlvmCodegen::new(&context, "test");
        codegen.declare_runtime_helpers();
        let proto_ptr = &proto as *const Prototype;
        codegen
            .compile_prototype(&proto, proto_ptr)
            .expect("codegen failed");
        codegen.verify().expect("verification failed");
        codegen.get_ir()
    }

    /// Helper: compile and execute TL source via MCJIT, return the captured output.
    fn run_llvm(source: &str) -> Vec<String> {
        use inkwell::OptimizationLevel;

        let proto = compile_source(source);
        let context = Context::create();
        let codegen = LlvmCodegen::new(&context, "test");
        codegen.declare_runtime_helpers();

        let proto_ptr = &proto as *const Prototype;
        codegen
            .compile_prototype(&proto, proto_ptr)
            .expect("codegen failed");
        codegen.verify().expect("verification failed");

        let ee = codegen
            .module
            .create_jit_execution_engine(OptimizationLevel::None)
            .expect("jit engine failed");

        crate::aot::register_runtime_symbols_ee(&ee);

        type TlFnType =
            unsafe extern "C" fn(*mut VmContext, *const VmValue, i64, *mut VmValue) -> i64;

        let compiled_fn: inkwell::execution_engine::JitFunction<TlFnType> =
            unsafe { ee.get_function("tl_main").expect("tl_main not found") };

        let mut vm = Vm::new();
        let mut vm_ctx = VmContext {
            vm: &mut vm as *mut Vm,
            prototype: proto_ptr,
        };
        let mut retval = VmValue::None;

        let status = unsafe {
            compiled_fn.call(
                &mut vm_ctx as *mut VmContext,
                std::ptr::null(),
                0,
                &mut retval as *mut VmValue,
            )
        };

        assert_eq!(status, 0, "LLVM execution returned error status");
        vm.output.clone()
    }

    // ── IR generation tests ──

    #[test]
    fn test_simple_constant_ir() {
        let ir = source_to_ir("let x = 42");
        eprintln!("IR:\n{}", &ir[..std::cmp::min(ir.len(), 2000)]);
        assert!(ir.contains("tl_main"), "Should contain main function");
        assert!(
            ir.contains("tl_rt_get_const"),
            "Should call tl_rt_get_const"
        );
    }

    #[test]
    fn test_arithmetic_ir() {
        let ir = source_to_ir("let x = 2 + 3");
        assert!(ir.contains("tl_rt_add"), "Should call tl_rt_add");
    }

    #[test]
    fn test_comparison_ir() {
        let ir = source_to_ir("let x = 1 < 2");
        assert!(ir.contains("tl_rt_lt"), "Should call tl_rt_lt");
    }

    #[test]
    fn test_control_flow_ir() {
        // Compile without verification to see the IR
        let source = "if true { println(1) } else { println(2) }";
        let proto = compile_source(source);
        let context = Context::create();
        let codegen = LlvmCodegen::new(&context, "test");
        codegen.declare_runtime_helpers();
        let proto_ptr = &proto as *const Prototype;
        codegen
            .compile_prototype(&proto, proto_ptr)
            .expect("codegen failed");
        let ir = codegen.get_ir();
        eprintln!("Control flow IR:\n{ir}");
        codegen.verify().expect("verification failed");
        assert!(
            ir.contains("tl_rt_is_truthy"),
            "Should call tl_rt_is_truthy"
        );
        assert!(ir.contains("br i1"), "Should have conditional branch");
    }

    #[test]
    fn test_function_call_ir() {
        let ir = source_to_ir("fn add(a, b) { a + b }\nadd(1, 2)");
        assert!(
            ir.contains("tl_rt_call") || ir.contains("tl_rt_add"),
            "Should contain call or add helper"
        );
    }

    #[test]
    fn test_builtin_call_ir() {
        let ir = source_to_ir("println(42)");
        assert!(
            ir.contains("tl_rt_call_builtin"),
            "Should call tl_rt_call_builtin"
        );
    }

    #[test]
    fn test_list_ir() {
        let ir = source_to_ir("let x = [1, 2, 3]");
        assert!(
            ir.contains("tl_rt_make_list"),
            "Should call tl_rt_make_list"
        );
    }

    #[test]
    fn test_global_access_ir() {
        let ir = source_to_ir("let x = 1\nlet y = x + 2");
        // x is a global in top-level scope
        assert!(
            ir.contains("tl_rt_get_const") || ir.contains("tl_rt_add"),
            "Should contain const loading or add"
        );
    }

    #[test]
    fn test_return_ir() {
        let ir = source_to_ir("fn f() { return 42 }");
        assert!(ir.contains("ret i64"), "Should have return instruction");
    }

    #[test]
    fn test_module_verification() {
        // Just verify that various programs generate valid LLVM IR
        let programs = [
            "let x = 42",
            "let x = 1 + 2 * 3",
            "if true { 1 }",
            "let x = not false",
            "let x = -42",
            "fn f(a) { a }",
        ];
        for src in &programs {
            let _ = source_to_ir(src); // panics if verification fails
        }
    }

    // ── MCJIT execution tests ──

    #[test]
    fn test_llvm_exec_print() {
        let output = run_llvm("println(42)");
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_llvm_exec_arithmetic() {
        let output = run_llvm("println(2 + 3 * 4)");
        assert_eq!(output, vec!["14"]);
    }

    #[test]
    fn test_llvm_exec_string() {
        let output = run_llvm(r#"println("hello")"#);
        assert_eq!(output, vec!["hello"]);
    }

    #[test]
    fn test_llvm_exec_bool() {
        let output = run_llvm("println(1 < 2)");
        assert_eq!(output, vec!["true"]);
    }

    #[test]
    fn test_llvm_exec_if_else() {
        let output = run_llvm("if true { println(1) } else { println(2) }");
        assert_eq!(output, vec!["1"]);
    }

    #[test]
    fn test_llvm_exec_if_false() {
        let output = run_llvm("if false { println(1) } else { println(2) }");
        assert_eq!(output, vec!["2"]);
    }

    #[test]
    fn test_llvm_exec_len_builtin() {
        let output = run_llvm("println(len([1, 2, 3]))");
        assert_eq!(output, vec!["3"]);
    }

    #[test]
    fn test_llvm_exec_none() {
        let output = run_llvm("println(none)");
        assert_eq!(output, vec!["none"]);
    }

    #[test]
    fn test_llvm_exec_negation() {
        let output = run_llvm("println(-5)");
        assert_eq!(output, vec!["-5"]);
    }

    #[test]
    fn test_llvm_exec_not() {
        let output = run_llvm("println(not true)");
        assert_eq!(output, vec!["false"]);
    }

    #[test]
    fn test_llvm_exec_comparison_chain() {
        let output = run_llvm("println(1 == 1)\nprintln(1 != 2)\nprintln(1 <= 1)\nprintln(2 >= 1)");
        assert_eq!(output, vec!["true", "true", "true", "true"]);
    }

    #[test]
    fn test_llvm_exec_string_concat() {
        let output = run_llvm(
            r#"let a = "hello" + " " + "world"
println(a)"#,
        );
        assert_eq!(output, vec!["hello world"]);
    }

    #[test]
    fn test_llvm_exec_multiple_prints() {
        let output = run_llvm("println(1)\nprintln(2)\nprintln(3)");
        assert_eq!(output, vec!["1", "2", "3"]);
    }

    #[test]
    fn test_llvm_exec_float() {
        let output = run_llvm("println(3.14)");
        assert!(
            output[0].starts_with("3.14"),
            "Expected 3.14, got {}",
            output[0]
        );
    }

    #[test]
    fn test_llvm_exec_division() {
        let output = run_llvm("println(10 / 3)");
        assert_eq!(output, vec!["3"]);
    }

    #[test]
    fn test_llvm_exec_modulo() {
        let output = run_llvm("println(10 % 3)");
        assert_eq!(output, vec!["1"]);
    }
}
