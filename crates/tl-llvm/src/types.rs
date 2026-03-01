// ThinkingLanguage — LLVM type definitions
// Maps TL's VmValue to LLVM IR types via opaque pointer representation.

use inkwell::context::Context;
use inkwell::types::{FloatType, FunctionType, IntType, PointerType, VoidType};

/// LLVM type definitions for the TL runtime interface.
///
/// All TL values are represented as opaque pointers (`i8*`) in LLVM IR.
/// Arithmetic and comparisons are dispatched through runtime helper functions
/// that operate on `*const VmValue` / `*mut VmValue`.
pub struct LlvmTypes<'ctx> {
    pub vmvalue_ptr: PointerType<'ctx>,
    pub i8_type: IntType<'ctx>,
    pub i32_type: IntType<'ctx>,
    pub i64_type: IntType<'ctx>,
    pub f64_type: FloatType<'ctx>,
    pub bool_type: IntType<'ctx>,
    pub void_type: VoidType<'ctx>,

    // Runtime helper function types
    /// (ptr a, ptr b, ptr out) -> void  [arithmetic ops]
    pub rt_binop_ty: FunctionType<'ctx>,
    /// (ptr a, ptr b) -> i64  [comparison returning -1/0/1]
    pub rt_cmp_ty: FunctionType<'ctx>,
    /// (ptr val) -> i64  [truthiness check]
    pub rt_truthy_ty: FunctionType<'ctx>,
    /// (ptr ctx, ptr fn, ptr args, i64 nargs, ptr out) -> i64
    pub rt_call_ty: FunctionType<'ctx>,
    /// (ptr ctx, ptr name, i64 name_len, ptr out) -> i64
    pub rt_get_global_ty: FunctionType<'ctx>,
    /// (ptr ctx, ptr name, i64 name_len, ptr val) -> i64
    pub rt_set_global_ty: FunctionType<'ctx>,
    /// (ptr ctx, i64 builtin_id, ptr args, i64 nargs, ptr out) -> i64
    pub rt_builtin_ty: FunctionType<'ctx>,
    /// (ptr proto, i64 idx, ptr out) -> void
    pub rt_get_const_ty: FunctionType<'ctx>,
    /// (ptr vals, i64 count, ptr out) -> void
    pub rt_make_list_ty: FunctionType<'ctx>,
    /// (ptr keys, ptr vals, i64 count, ptr out) -> void
    pub rt_make_map_ty: FunctionType<'ctx>,
    /// (ptr out) -> void  [load constant None/True/False]
    pub rt_load_const_ty: FunctionType<'ctx>,
    /// (ptr val, ptr idx, ptr out) -> i64  [get index]
    pub rt_get_index_ty: FunctionType<'ctx>,
    /// (ptr val, ptr idx, ptr new_val) -> i64  [set index]
    pub rt_set_index_ty: FunctionType<'ctx>,
    /// (ptr val, ptr name, i64 name_len, ptr out) -> i64  [get member]
    pub rt_get_member_ty: FunctionType<'ctx>,
    /// (ptr val, ptr name, i64 name_len, ptr new_val) -> i64  [set member]
    pub rt_set_member_ty: FunctionType<'ctx>,
    /// (ptr ctx, ptr obj, ptr name, i64 name_len, ptr args, i64 nargs, ptr out) -> i64
    pub rt_method_call_ty: FunctionType<'ctx>,
    /// (ptr ctx, i64 opcode, i64 a, i64 b, i64 c, ptr regs_base, i64 num_regs) -> i64
    pub rt_vm_exec_op_ty: FunctionType<'ctx>,
    /// (ptr a, ptr b, ptr out) -> void  [comparison producing Bool VmValue]
    pub rt_cmp_op_ty: FunctionType<'ctx>,
    /// (ptr ctx, ptr proto_const_idx, ptr upvalue_data, i64 num_upvalues, ptr regs_base, ptr out) -> void
    pub rt_make_closure_ty: FunctionType<'ctx>,
    /// (ptr a, ptr out) -> void  [unary ops]
    pub rt_unary_ty: FunctionType<'ctx>,

    /// Standard TL function type: (ptr ctx, ptr args, i64 nargs, ptr retval) -> i64
    pub tl_fn_ty: FunctionType<'ctx>,
}

impl<'ctx> LlvmTypes<'ctx> {
    pub fn new(context: &'ctx Context) -> Self {
        let i8_type = context.i8_type();
        let i32_type = context.i32_type();
        let i64_type = context.i64_type();
        let f64_type = context.f64_type();
        let bool_type = context.bool_type();
        let void_type = context.void_type();
        let ptr_type = context.ptr_type(inkwell::AddressSpace::default());

        // Runtime helper function types
        let rt_binop_ty =
            void_type.fn_type(&[ptr_type.into(), ptr_type.into(), ptr_type.into()], false);
        let rt_cmp_ty = i64_type.fn_type(&[ptr_type.into(), ptr_type.into()], false);
        let rt_truthy_ty = i64_type.fn_type(&[ptr_type.into()], false);
        let rt_call_ty = i64_type.fn_type(
            &[
                ptr_type.into(),
                ptr_type.into(),
                ptr_type.into(),
                i64_type.into(),
                ptr_type.into(),
            ],
            false,
        );
        let rt_get_global_ty = i64_type.fn_type(
            &[
                ptr_type.into(),
                ptr_type.into(),
                i64_type.into(),
                ptr_type.into(),
            ],
            false,
        );
        let rt_set_global_ty = i64_type.fn_type(
            &[
                ptr_type.into(),
                ptr_type.into(),
                i64_type.into(),
                ptr_type.into(),
            ],
            false,
        );
        let rt_builtin_ty = i64_type.fn_type(
            &[
                ptr_type.into(),
                i64_type.into(),
                ptr_type.into(),
                i64_type.into(),
                ptr_type.into(),
            ],
            false,
        );
        let rt_get_const_ty =
            void_type.fn_type(&[ptr_type.into(), i64_type.into(), ptr_type.into()], false);
        let rt_make_list_ty =
            void_type.fn_type(&[ptr_type.into(), i64_type.into(), ptr_type.into()], false);
        let rt_make_map_ty = void_type.fn_type(
            &[
                ptr_type.into(),
                ptr_type.into(),
                i64_type.into(),
                ptr_type.into(),
            ],
            false,
        );
        let rt_load_const_ty = void_type.fn_type(&[ptr_type.into()], false);
        let rt_get_index_ty =
            i64_type.fn_type(&[ptr_type.into(), ptr_type.into(), ptr_type.into()], false);
        let rt_set_index_ty =
            i64_type.fn_type(&[ptr_type.into(), ptr_type.into(), ptr_type.into()], false);
        let rt_get_member_ty = i64_type.fn_type(
            &[
                ptr_type.into(),
                ptr_type.into(),
                i64_type.into(),
                ptr_type.into(),
            ],
            false,
        );
        let rt_set_member_ty = i64_type.fn_type(
            &[
                ptr_type.into(),
                ptr_type.into(),
                i64_type.into(),
                ptr_type.into(),
            ],
            false,
        );
        let rt_method_call_ty = i64_type.fn_type(
            &[
                ptr_type.into(),
                ptr_type.into(),
                ptr_type.into(),
                i64_type.into(),
                ptr_type.into(),
                i64_type.into(),
                ptr_type.into(),
            ],
            false,
        );
        let rt_vm_exec_op_ty = i64_type.fn_type(
            &[
                ptr_type.into(),
                i64_type.into(),
                i64_type.into(),
                i64_type.into(),
                i64_type.into(),
                ptr_type.into(),
                i64_type.into(),
            ],
            false,
        );
        let rt_cmp_op_ty =
            void_type.fn_type(&[ptr_type.into(), ptr_type.into(), ptr_type.into()], false);
        let rt_make_closure_ty = void_type.fn_type(
            &[
                ptr_type.into(),
                ptr_type.into(),
                i64_type.into(),
                ptr_type.into(),
                ptr_type.into(),
            ],
            false,
        );
        let rt_unary_ty = void_type.fn_type(&[ptr_type.into(), ptr_type.into()], false);
        let tl_fn_ty = i64_type.fn_type(
            &[
                ptr_type.into(),
                ptr_type.into(),
                i64_type.into(),
                ptr_type.into(),
            ],
            false,
        );

        LlvmTypes {
            vmvalue_ptr: ptr_type,
            i8_type,
            i32_type,
            i64_type,
            f64_type,
            bool_type,
            void_type,
            rt_binop_ty,
            rt_cmp_ty,
            rt_truthy_ty,
            rt_call_ty,
            rt_get_global_ty,
            rt_set_global_ty,
            rt_builtin_ty,
            rt_get_const_ty,
            rt_make_list_ty,
            rt_make_map_ty,
            rt_load_const_ty,
            rt_get_index_ty,
            rt_set_index_ty,
            rt_get_member_ty,
            rt_set_member_ty,
            rt_method_call_ty,
            rt_vm_exec_op_ty,
            rt_cmp_op_ty,
            rt_make_closure_ty,
            rt_unary_ty,
            tl_fn_ty,
        }
    }
}
