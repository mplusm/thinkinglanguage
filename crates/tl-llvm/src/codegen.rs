// ThinkingLanguage — LLVM Codegen
// Translates Prototype bytecode to LLVM IR via inkwell.

use std::collections::HashMap;

use inkwell::IntPredicate;
use inkwell::basic_block::BasicBlock;
use inkwell::builder::Builder;
use inkwell::context::Context;
use inkwell::module::Module;
use inkwell::values::{FunctionValue, IntValue, PointerValue};

use tl_compiler::chunk::{Constant, Prototype};
use tl_compiler::opcode::{Op, decode_a, decode_b, decode_bx, decode_c, decode_op, decode_sbx};

use crate::types::LlvmTypes;

/// LLVM compiler that translates Prototype bytecode to LLVM IR.
pub struct LlvmCodegen<'ctx> {
    pub context: &'ctx Context,
    pub module: Module<'ctx>,
    pub builder: Builder<'ctx>,
    pub types: LlvmTypes<'ctx>,
    /// Size of VmValue in bytes (computed once)
    vmvalue_size: usize,
}

impl<'ctx> LlvmCodegen<'ctx> {
    pub fn new(context: &'ctx Context, module_name: &str) -> Self {
        let module = context.create_module(module_name);
        let builder = context.create_builder();
        let types = LlvmTypes::new(context);
        let vmvalue_size = std::mem::size_of::<tl_compiler::VmValue>();

        LlvmCodegen {
            context,
            module,
            builder,
            types,
            vmvalue_size,
        }
    }

    /// Declare all runtime helper functions in the LLVM module.
    pub fn declare_runtime_helpers(&self) {
        let t = &self.types;

        // Arithmetic: (ptr, ptr, ptr) -> void
        for name in &[
            "tl_rt_add",
            "tl_rt_sub",
            "tl_rt_mul",
            "tl_rt_div",
            "tl_rt_mod",
            "tl_rt_pow",
            "tl_rt_concat",
        ] {
            self.module.add_function(
                name,
                t.rt_binop_ty,
                Some(inkwell::module::Linkage::External),
            );
        }

        // Comparison producing Bool VmValue: (ptr, ptr, ptr) -> void
        for name in &[
            "tl_rt_eq",
            "tl_rt_neq",
            "tl_rt_lt",
            "tl_rt_gt",
            "tl_rt_lte",
            "tl_rt_gte",
        ] {
            self.module.add_function(
                name,
                t.rt_cmp_op_ty,
                Some(inkwell::module::Linkage::External),
            );
        }

        // Unary: (ptr, ptr) -> void
        for name in &["tl_rt_neg", "tl_rt_not"] {
            self.module.add_function(
                name,
                t.rt_unary_ty,
                Some(inkwell::module::Linkage::External),
            );
        }

        // Comparison: (ptr, ptr) -> i64
        self.module.add_function(
            "tl_rt_cmp",
            t.rt_cmp_ty,
            Some(inkwell::module::Linkage::External),
        );

        // Truthiness: (ptr) -> i64
        self.module.add_function(
            "tl_rt_is_truthy",
            t.rt_truthy_ty,
            Some(inkwell::module::Linkage::External),
        );

        // Constant loading: (ptr) -> void
        for name in &["tl_rt_load_none", "tl_rt_load_true", "tl_rt_load_false"] {
            self.module.add_function(
                name,
                t.rt_load_const_ty,
                Some(inkwell::module::Linkage::External),
            );
        }

        // Get constant: (ptr, i64, ptr) -> void
        self.module.add_function(
            "tl_rt_get_const",
            t.rt_get_const_ty,
            Some(inkwell::module::Linkage::External),
        );

        // Move value: (ptr, ptr) -> void
        self.module.add_function(
            "tl_rt_move_value",
            t.rt_unary_ty,
            Some(inkwell::module::Linkage::External),
        );

        // Global access
        self.module.add_function(
            "tl_rt_get_global",
            t.rt_get_global_ty,
            Some(inkwell::module::Linkage::External),
        );
        self.module.add_function(
            "tl_rt_set_global",
            t.rt_set_global_ty,
            Some(inkwell::module::Linkage::External),
        );

        // Function calls
        self.module.add_function(
            "tl_rt_call",
            t.rt_call_ty,
            Some(inkwell::module::Linkage::External),
        );
        self.module.add_function(
            "tl_rt_call_builtin",
            t.rt_builtin_ty,
            Some(inkwell::module::Linkage::External),
        );

        // Data structure construction
        self.module.add_function(
            "tl_rt_make_list",
            t.rt_make_list_ty,
            Some(inkwell::module::Linkage::External),
        );
        self.module.add_function(
            "tl_rt_make_map",
            t.rt_make_map_ty,
            Some(inkwell::module::Linkage::External),
        );

        // Index access
        self.module.add_function(
            "tl_rt_get_index",
            t.rt_get_index_ty,
            Some(inkwell::module::Linkage::External),
        );
        self.module.add_function(
            "tl_rt_set_index",
            t.rt_set_index_ty,
            Some(inkwell::module::Linkage::External),
        );

        // Member access
        self.module.add_function(
            "tl_rt_get_member",
            t.rt_get_member_ty,
            Some(inkwell::module::Linkage::External),
        );
        self.module.add_function(
            "tl_rt_set_member",
            t.rt_set_member_ty,
            Some(inkwell::module::Linkage::External),
        );

        // Method call
        self.module.add_function(
            "tl_rt_method_call",
            t.rt_method_call_ty,
            Some(inkwell::module::Linkage::External),
        );

        // Closure
        self.module.add_function(
            "tl_rt_make_closure",
            t.rt_make_closure_ty,
            Some(inkwell::module::Linkage::External),
        );

        // VM fallback
        self.module.add_function(
            "tl_rt_vm_exec_op",
            t.rt_vm_exec_op_ty,
            Some(inkwell::module::Linkage::External),
        );
    }

    /// Compile a Prototype into an LLVM function.
    /// Returns the LLVM function value and the name used.
    pub fn compile_prototype(
        &self,
        proto: &Prototype,
        proto_ptr: *const Prototype,
    ) -> Result<FunctionValue<'ctx>, String> {
        let fn_name = if proto.name.is_empty() || proto.name == "<main>" {
            "tl_main".to_string()
        } else {
            format!(
                "tl_fn_{}",
                proto
                    .name
                    .replace("::", "_")
                    .replace('<', "")
                    .replace('>', "")
            )
        };

        let function = self
            .module
            .add_function(&fn_name, self.types.tl_fn_ty, None);
        let entry_block = self.context.append_basic_block(function, "entry");
        self.builder.position_at_end(entry_block);

        // Function params: (ptr ctx, ptr args, i64 nargs, ptr retval) -> i64
        let ctx_param = function.get_nth_param(0).unwrap().into_pointer_value();
        let args_param = function.get_nth_param(1).unwrap().into_pointer_value();
        let _nargs_param = function.get_nth_param(2).unwrap().into_int_value();
        let retval_param = function.get_nth_param(3).unwrap().into_pointer_value();

        // Allocate register slots as an array of bytes (VmValue-sized)
        let num_regs = proto.num_registers as usize;
        if num_regs == 0 {
            // No registers, just return 0
            let zero = self.types.i64_type.const_int(0, false);
            self.builder
                .build_return(Some(&zero))
                .map_err(|e| e.to_string())?;
            return Ok(function);
        }

        // Allocate a single contiguous array of VmValue-sized slots.
        // This ensures pointers to consecutive registers are contiguous in memory,
        // which is required by runtime helpers like tl_rt_call_builtin that take
        // a base pointer + count.
        // VmValue requires 16-byte alignment, so use i128 as element type.
        let _vmvalue_align = std::mem::align_of::<tl_compiler::VmValue>();
        debug_assert!(
            _vmvalue_align <= 16,
            "VmValue alignment exceeds i128 alignment"
        );
        let units_per_value = (self.vmvalue_size + 15) / 16; // i128 units per VmValue
        let total_units = units_per_value * num_regs;
        let i128_type = self.context.i128_type();
        let regs_array_ty = i128_type.array_type(total_units as u32);
        let regs_base = self
            .builder
            .build_alloca(regs_array_ty, "regs")
            .map_err(|e| e.to_string())?;
        // Explicitly set alignment to match VmValue's alignment requirement (16 bytes)
        if let Some(inst) = regs_base.as_instruction() {
            let _ = inst.set_alignment(_vmvalue_align as u32);
        }

        // Compute per-register pointers via GEP
        let vmvalue_size_val = self
            .types
            .i64_type
            .const_int(self.vmvalue_size as u64, false);
        let mut reg_slots: Vec<PointerValue<'ctx>> = Vec::with_capacity(num_regs);
        for i in 0..num_regs {
            let byte_offset = self
                .types
                .i64_type
                .const_int((i * self.vmvalue_size) as u64, false);
            let slot = unsafe {
                self.builder
                    .build_gep(
                        self.context.i8_type(),
                        regs_base,
                        &[byte_offset],
                        &format!("r{i}"),
                    )
                    .map_err(|e| e.to_string())?
            };
            reg_slots.push(slot);
        }

        // Initialize all registers to None
        let load_none_fn = self.module.get_function("tl_rt_load_none").unwrap();
        for slot in &reg_slots {
            self.builder
                .build_call(load_none_fn, &[(*slot).into()], "")
                .map_err(|e| e.to_string())?;
        }

        // Copy args into first registers
        let move_fn = self.module.get_function("tl_rt_move_value").unwrap();
        for i in 0..proto.arity as usize {
            if i < num_regs {
                // args_param + i * vmvalue_size → reg_slots[i]
                let offset = self.types.i64_type.const_int(i as u64, false);
                let byte_offset = self
                    .builder
                    .build_int_mul(offset, vmvalue_size_val, "byte_off")
                    .map_err(|e| e.to_string())?;
                let arg_ptr = unsafe {
                    self.builder
                        .build_gep(
                            self.context.i8_type(),
                            args_param,
                            &[byte_offset],
                            &format!("arg{i}"),
                        )
                        .map_err(|e| e.to_string())?
                };
                self.builder
                    .build_call(move_fn, &[arg_ptr.into(), reg_slots[i].into()], "")
                    .map_err(|e| e.to_string())?;
            }
        }

        // Create prototype pointer constant (store as i64 then cast to ptr)
        let proto_ptr_int = self.types.i64_type.const_int(proto_ptr as u64, false);
        let proto_ptr_val = self
            .builder
            .build_int_to_ptr(proto_ptr_int, self.types.vmvalue_ptr, "proto_ptr")
            .map_err(|e| e.to_string())?;

        // First pass: discover all jump targets and create basic blocks
        let mut block_map: HashMap<usize, BasicBlock<'ctx>> = HashMap::new();
        for (ip, &inst) in proto.code.iter().enumerate() {
            let op = decode_op(inst);
            match op {
                Op::Jump | Op::JumpIfFalse | Op::JumpIfTrue => {
                    let offset = decode_sbx(inst) as i32;
                    let target = (ip as i32 + 1 + offset) as usize;
                    block_map.entry(target).or_insert_with(|| {
                        self.context
                            .append_basic_block(function, &format!("L{target}"))
                    });
                    // Also need a fallthrough block for conditional jumps
                    if matches!(op, Op::JumpIfFalse | Op::JumpIfTrue) {
                        block_map.entry(ip + 1).or_insert_with(|| {
                            self.context
                                .append_basic_block(function, &format!("L{}", ip + 1))
                        });
                    }
                }
                _ => {}
            }
        }

        // Error block: return 1
        let error_block = self.context.append_basic_block(function, "error");
        self.builder.position_at_end(error_block);
        let one = self.types.i64_type.const_int(1, false);
        self.builder
            .build_return(Some(&one))
            .map_err(|e| e.to_string())?;

        // Position back at entry
        self.builder.position_at_end(entry_block);

        // Second pass: emit LLVM IR for each opcode
        let mut ip = 0;
        while ip < proto.code.len() {
            // If this IP is a jump target, switch to that block
            if let Some(&target_block) = block_map.get(&ip) {
                let current_block = self.builder.get_insert_block().unwrap();
                // If current block has no terminator AND is different from target,
                // add a branch to the target
                if current_block.get_terminator().is_none() && current_block != target_block {
                    self.builder
                        .build_unconditional_branch(target_block)
                        .map_err(|e| e.to_string())?;
                }
                self.builder.position_at_end(target_block);
            } else {
                // If the current block already has a terminator (dead code),
                // skip this instruction
                let current_block = self.builder.get_insert_block().unwrap();
                if current_block.get_terminator().is_some() {
                    ip += 1;
                    continue;
                }
            }

            let inst = proto.code[ip];
            let op = decode_op(inst);
            let a = decode_a(inst) as usize;
            let b = decode_b(inst) as usize;
            let c = decode_c(inst) as usize;
            let bx = decode_bx(inst);
            let sbx = decode_sbx(inst);

            match op {
                // ── Tier 1: Core ops emitted as runtime calls ──
                Op::LoadConst => {
                    let get_const_fn = self.module.get_function("tl_rt_get_const").unwrap();
                    let idx_val = self.types.i64_type.const_int(bx as u64, false);
                    self.builder
                        .build_call(
                            get_const_fn,
                            &[proto_ptr_val.into(), idx_val.into(), reg_slots[a].into()],
                            "",
                        )
                        .map_err(|e| e.to_string())?;
                }

                Op::LoadNone => {
                    self.builder
                        .build_call(load_none_fn, &[reg_slots[a].into()], "")
                        .map_err(|e| e.to_string())?;
                }

                Op::LoadTrue => {
                    let f = self.module.get_function("tl_rt_load_true").unwrap();
                    self.builder
                        .build_call(f, &[reg_slots[a].into()], "")
                        .map_err(|e| e.to_string())?;
                }

                Op::LoadFalse => {
                    let f = self.module.get_function("tl_rt_load_false").unwrap();
                    self.builder
                        .build_call(f, &[reg_slots[a].into()], "")
                        .map_err(|e| e.to_string())?;
                }

                Op::Move | Op::GetLocal => {
                    if b < num_regs {
                        self.builder
                            .build_call(move_fn, &[reg_slots[b].into(), reg_slots[a].into()], "")
                            .map_err(|e| e.to_string())?;
                    }
                }

                Op::SetLocal => {
                    if b < num_regs {
                        self.builder
                            .build_call(move_fn, &[reg_slots[a].into(), reg_slots[b].into()], "")
                            .map_err(|e| e.to_string())?;
                    }
                }

                Op::GetGlobal => {
                    self.emit_global_access(
                        proto,
                        a,
                        bx,
                        &reg_slots,
                        ctx_param,
                        true,
                        error_block,
                    )?;
                }

                Op::SetGlobal => {
                    self.emit_global_access(
                        proto,
                        a,
                        bx,
                        &reg_slots,
                        ctx_param,
                        false,
                        error_block,
                    )?;
                }

                Op::GetUpvalue | Op::SetUpvalue => {
                    // Fallback to VM for upvalue handling
                    self.emit_vm_fallback(
                        inst,
                        &reg_slots,
                        ctx_param,
                        proto_ptr_val,
                        num_regs,
                        error_block,
                    )?;
                }

                // Arithmetic
                Op::Add => self.emit_binop("tl_rt_add", a, b, c, &reg_slots)?,
                Op::Sub => self.emit_binop("tl_rt_sub", a, b, c, &reg_slots)?,
                Op::Mul => self.emit_binop("tl_rt_mul", a, b, c, &reg_slots)?,
                Op::Div => self.emit_binop("tl_rt_div", a, b, c, &reg_slots)?,
                Op::Mod => self.emit_binop("tl_rt_mod", a, b, c, &reg_slots)?,
                Op::Pow => self.emit_binop("tl_rt_pow", a, b, c, &reg_slots)?,

                // Unary
                Op::Neg => self.emit_unary("tl_rt_neg", a, b, &reg_slots)?,
                Op::Not => self.emit_unary("tl_rt_not", a, b, &reg_slots)?,

                // Comparison
                Op::Eq => self.emit_binop("tl_rt_eq", a, b, c, &reg_slots)?,
                Op::Neq => self.emit_binop("tl_rt_neq", a, b, c, &reg_slots)?,
                Op::Lt => self.emit_binop("tl_rt_lt", a, b, c, &reg_slots)?,
                Op::Gt => self.emit_binop("tl_rt_gt", a, b, c, &reg_slots)?,
                Op::Lte => self.emit_binop("tl_rt_lte", a, b, c, &reg_slots)?,
                Op::Gte => self.emit_binop("tl_rt_gte", a, b, c, &reg_slots)?,

                Op::Concat => self.emit_binop("tl_rt_concat", a, b, c, &reg_slots)?,

                // Logical And/Or with short-circuit
                Op::And => {
                    self.emit_logical_op(function, a, b, c, &reg_slots, true, &mut block_map, ip)?;
                }

                Op::Or => {
                    self.emit_logical_op(function, a, b, c, &reg_slots, false, &mut block_map, ip)?;
                }

                // Control flow
                Op::Jump => {
                    let target = (ip as i32 + 1 + sbx as i32) as usize;
                    if let Some(&target_block) = block_map.get(&target) {
                        self.builder
                            .build_unconditional_branch(target_block)
                            .map_err(|e| e.to_string())?;
                    }
                }

                Op::JumpIfFalse => {
                    self.emit_conditional_jump(
                        a, sbx, ip, &reg_slots, &block_map, function, false,
                    )?;
                }

                Op::JumpIfTrue => {
                    self.emit_conditional_jump(a, sbx, ip, &reg_slots, &block_map, function, true)?;
                }

                Op::Return => {
                    // Copy register A to retval pointer
                    if a < num_regs {
                        self.builder
                            .build_call(move_fn, &[reg_slots[a].into(), retval_param.into()], "")
                            .map_err(|e| e.to_string())?;
                    }
                    let zero = self.types.i64_type.const_int(0, false);
                    self.builder
                        .build_return(Some(&zero))
                        .map_err(|e| e.to_string())?;
                }

                // ── Tier 2: Runtime dispatch ──
                Op::Call => {
                    self.emit_call(a, b, c, &reg_slots, ctx_param, error_block)?;
                }

                Op::CallBuiltin => {
                    // ABx format: a=dest, bx=builtin_id (16-bit)
                    // Next instruction: A=arg_count, B=first_arg_reg
                    let builtin_id = decode_bx(proto.code[ip]) as usize;
                    let next_inst = proto.code[ip + 1];
                    let arg_count = decode_a(next_inst) as usize;
                    let first_arg = decode_b(next_inst) as usize;
                    self.emit_call_builtin(
                        a,
                        builtin_id,
                        first_arg,
                        arg_count,
                        &reg_slots,
                        ctx_param,
                        error_block,
                    )?;
                    ip += 1; // skip extra instruction word
                }

                Op::NewList => {
                    self.emit_new_list(a, b, c, &reg_slots)?;
                }

                Op::NewMap => {
                    self.emit_new_map(a, b, c, &reg_slots)?;
                }

                Op::GetIndex => {
                    self.emit_get_index(a, b, c, &reg_slots, error_block)?;
                }

                Op::SetIndex => {
                    self.emit_set_index(a, b, c, &reg_slots, error_block)?;
                }

                Op::GetMember => {
                    self.emit_get_member(proto, a, b, c, &reg_slots, ctx_param, error_block)?;
                }

                Op::SetMember => {
                    self.emit_set_member(proto, a, b, c, &reg_slots, error_block)?;
                }

                Op::MethodCall => {
                    // Next instruction: args_start in A, arg_count in B
                    let next_inst = proto.code[ip + 1];
                    let args_start = decode_a(next_inst) as usize;
                    let arg_count = decode_b(next_inst) as usize;
                    self.emit_method_call(
                        proto,
                        a,
                        b,
                        c,
                        args_start,
                        arg_count,
                        &reg_slots,
                        ctx_param,
                        error_block,
                    )?;
                    ip += 1; // skip extra instruction word
                }

                Op::NullCoalesce => {
                    // if R[A] is None, R[A] = R[B]
                    self.emit_null_coalesce(function, a, b, &reg_slots, &mut block_map, ip)?;
                }

                // ── Tier 3: VM fallback ──
                Op::Closure
                | Op::TablePipe
                | Op::Train
                | Op::PipelineExec
                | Op::StreamExec
                | Op::ConnectorDecl
                | Op::NewStruct
                | Op::NewEnum
                | Op::MatchEnum
                | Op::Throw
                | Op::TryBegin
                | Op::TryEnd
                | Op::Import
                | Op::Await
                | Op::Yield
                | Op::ForIter
                | Op::ForPrep
                | Op::TestMatch
                | Op::Interpolate
                | Op::ExtractField
                | Op::ExtractNamedField
                | Op::TryPropagate
                | Op::LoadMoved
                | Op::MakeRef
                | Op::ParallelFor
                | Op::AgentExec => {
                    self.emit_vm_fallback(
                        inst,
                        &reg_slots,
                        ctx_param,
                        proto_ptr_val,
                        num_regs,
                        error_block,
                    )?;

                    // Some opcodes consume an extra instruction word
                    match op {
                        Op::Interpolate | Op::NewEnum | Op::CallBuiltin | Op::MethodCall => {
                            ip += 1;
                        }
                        Op::Closure => {
                            // Closure is followed by upvalue descriptors
                            let proto_idx = bx as usize;
                            if let Some(Constant::Prototype(sub_proto)) =
                                proto.constants.get(proto_idx)
                            {
                                ip += sub_proto.upvalue_defs.len();
                            }
                        }
                        _ => {}
                    }
                }
            }

            ip += 1;
        }

        // If we fall through to here without a return, return None
        let current_block = self.builder.get_insert_block().unwrap();
        if current_block.get_terminator().is_none() {
            self.builder
                .build_call(load_none_fn, &[retval_param.into()], "")
                .map_err(|e| e.to_string())?;
            let zero = self.types.i64_type.const_int(0, false);
            self.builder
                .build_return(Some(&zero))
                .map_err(|e| e.to_string())?;
        }

        Ok(function)
    }

    /// Get the LLVM IR as a string.
    pub fn get_ir(&self) -> String {
        self.module.print_to_string().to_string()
    }

    /// Verify the module.
    pub fn verify(&self) -> Result<(), String> {
        self.module.verify().map_err(|e| e.to_string())
    }

    // ── Helper emitters ──

    fn emit_binop(
        &self,
        fn_name: &str,
        a: usize,
        b: usize,
        c: usize,
        reg_slots: &[PointerValue<'ctx>],
    ) -> Result<(), String> {
        let func = self.module.get_function(fn_name).unwrap();
        self.builder
            .build_call(
                func,
                &[
                    reg_slots[b].into(),
                    reg_slots[c].into(),
                    reg_slots[a].into(),
                ],
                "",
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn emit_unary(
        &self,
        fn_name: &str,
        a: usize,
        b: usize,
        reg_slots: &[PointerValue<'ctx>],
    ) -> Result<(), String> {
        let func = self.module.get_function(fn_name).unwrap();
        self.builder
            .build_call(func, &[reg_slots[b].into(), reg_slots[a].into()], "")
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn emit_global_access(
        &self,
        proto: &Prototype,
        a: usize,
        bx: u16,
        reg_slots: &[PointerValue<'ctx>],
        ctx_param: PointerValue<'ctx>,
        is_get: bool,
        error_block: BasicBlock<'ctx>,
    ) -> Result<(), String> {
        // Get the global name from constant pool
        let name = match &proto.constants[bx as usize] {
            Constant::String(s) => s.as_ref(),
            _ => return Err("Expected string constant for global name".into()),
        };

        // Create a global string constant for the name
        let name_bytes = name.as_bytes();
        let name_global = self
            .builder
            .build_global_string_ptr(name, &format!("gname_{name}"))
            .map_err(|e| e.to_string())?;
        let name_len = self
            .types
            .i64_type
            .const_int(name_bytes.len() as u64, false);

        if is_get {
            let func = self.module.get_function("tl_rt_get_global").unwrap();
            let status = self
                .builder
                .build_call(
                    func,
                    &[
                        ctx_param.into(),
                        name_global.as_pointer_value().into(),
                        name_len.into(),
                        reg_slots[a].into(),
                    ],
                    "get_global_status",
                )
                .map_err(|e| e.to_string())?;
            // Check status and branch to error on failure
            self.check_status(
                status.try_as_basic_value().unwrap_basic().into_int_value(),
                error_block,
            )?;
        } else {
            let func = self.module.get_function("tl_rt_set_global").unwrap();
            self.builder
                .build_call(
                    func,
                    &[
                        ctx_param.into(),
                        name_global.as_pointer_value().into(),
                        name_len.into(),
                        reg_slots[a].into(),
                    ],
                    "",
                )
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn emit_conditional_jump(
        &self,
        a: usize,
        sbx: i16,
        ip: usize,
        reg_slots: &[PointerValue<'ctx>],
        block_map: &HashMap<usize, BasicBlock<'ctx>>,
        function: FunctionValue<'ctx>,
        jump_if_true: bool,
    ) -> Result<(), String> {
        let truthy_fn = self.module.get_function("tl_rt_is_truthy").unwrap();
        let result = self
            .builder
            .build_call(truthy_fn, &[reg_slots[a].into()], "truthy")
            .map_err(|e| e.to_string())?;
        let truthy_val = result.try_as_basic_value().unwrap_basic().into_int_value();

        let zero = self.types.i64_type.const_int(0, false);
        let cond = if jump_if_true {
            self.builder
                .build_int_compare(IntPredicate::NE, truthy_val, zero, "is_true")
                .map_err(|e| e.to_string())?
        } else {
            self.builder
                .build_int_compare(IntPredicate::EQ, truthy_val, zero, "is_false")
                .map_err(|e| e.to_string())?
        };

        let target = (ip as i32 + 1 + sbx as i32) as usize;
        let target_block = block_map.get(&target).copied().unwrap_or_else(|| {
            self.context
                .append_basic_block(function, &format!("L{target}"))
        });
        let fallthrough = block_map.get(&(ip + 1)).copied().unwrap_or_else(|| {
            self.context
                .append_basic_block(function, &format!("L{}", ip + 1))
        });

        self.builder
            .build_conditional_branch(cond, target_block, fallthrough)
            .map_err(|e| e.to_string())?;

        // Continue emitting in the fallthrough block
        self.builder.position_at_end(fallthrough);
        Ok(())
    }

    fn emit_logical_op(
        &self,
        function: FunctionValue<'ctx>,
        a: usize,
        b: usize,
        c: usize,
        reg_slots: &[PointerValue<'ctx>],
        is_and: bool,
        _block_map: &mut HashMap<usize, BasicBlock<'ctx>>,
        ip: usize,
    ) -> Result<(), String> {
        let truthy_fn = self.module.get_function("tl_rt_is_truthy").unwrap();
        let move_fn = self.module.get_function("tl_rt_move_value").unwrap();

        // Check truthiness of B
        let result = self
            .builder
            .build_call(truthy_fn, &[reg_slots[b].into()], "b_truthy")
            .map_err(|e| e.to_string())?;
        let b_truthy = result.try_as_basic_value().unwrap_basic().into_int_value();
        let zero = self.types.i64_type.const_int(0, false);

        let use_c_block = self
            .context
            .append_basic_block(function, &format!("and_c_{ip}"));
        let done_block = self
            .context
            .append_basic_block(function, &format!("and_done_{ip}"));

        if is_and {
            // And: if B is falsy, result is B; else result is C
            let b_is_truthy = self
                .builder
                .build_int_compare(IntPredicate::NE, b_truthy, zero, "")
                .map_err(|e| e.to_string())?;
            self.builder
                .build_conditional_branch(b_is_truthy, use_c_block, done_block)
                .map_err(|e| e.to_string())?;

            // B is falsy → A = B
            self.builder.position_at_end(done_block);
            // We'll move B into A here, but we need to handle both paths
            // Actually, let's restructure: done_from_b and done_from_c merge
            // Simpler approach: use B block and C block, both jump to merge
            let done_from_b = self
                .context
                .append_basic_block(function, &format!("and_b_{ip}"));

            // Rebuild: entry → check B → if truthy → use_c, else → done_from_b → merge
            // Need to reposition. Let's use a simpler pattern:
            self.builder.position_at_end(done_block);
            // This is the "B is falsy" path
            self.builder
                .build_call(move_fn, &[reg_slots[b].into(), reg_slots[a].into()], "")
                .map_err(|e| e.to_string())?;
            self.builder
                .build_unconditional_branch(done_from_b)
                .map_err(|e| e.to_string())?;

            self.builder.position_at_end(use_c_block);
            self.builder
                .build_call(move_fn, &[reg_slots[c].into(), reg_slots[a].into()], "")
                .map_err(|e| e.to_string())?;
            self.builder
                .build_unconditional_branch(done_from_b)
                .map_err(|e| e.to_string())?;

            self.builder.position_at_end(done_from_b);
        } else {
            // Or: if B is truthy, result is B; else result is C
            let b_is_truthy = self
                .builder
                .build_int_compare(IntPredicate::NE, b_truthy, zero, "")
                .map_err(|e| e.to_string())?;
            self.builder
                .build_conditional_branch(b_is_truthy, done_block, use_c_block)
                .map_err(|e| e.to_string())?;

            let merge_block = self
                .context
                .append_basic_block(function, &format!("or_merge_{ip}"));

            self.builder.position_at_end(done_block);
            self.builder
                .build_call(move_fn, &[reg_slots[b].into(), reg_slots[a].into()], "")
                .map_err(|e| e.to_string())?;
            self.builder
                .build_unconditional_branch(merge_block)
                .map_err(|e| e.to_string())?;

            self.builder.position_at_end(use_c_block);
            self.builder
                .build_call(move_fn, &[reg_slots[c].into(), reg_slots[a].into()], "")
                .map_err(|e| e.to_string())?;
            self.builder
                .build_unconditional_branch(merge_block)
                .map_err(|e| e.to_string())?;

            self.builder.position_at_end(merge_block);
        }
        Ok(())
    }

    fn emit_null_coalesce(
        &self,
        function: FunctionValue<'ctx>,
        a: usize,
        b: usize,
        reg_slots: &[PointerValue<'ctx>],
        _block_map: &mut HashMap<usize, BasicBlock<'ctx>>,
        ip: usize,
    ) -> Result<(), String> {
        let truthy_fn = self.module.get_function("tl_rt_is_truthy").unwrap();
        let move_fn = self.module.get_function("tl_rt_move_value").unwrap();

        // Check if A is truthy (non-None)
        let result = self
            .builder
            .build_call(truthy_fn, &[reg_slots[a].into()], "a_check")
            .map_err(|e| e.to_string())?;
        let a_truthy = result.try_as_basic_value().unwrap_basic().into_int_value();
        let zero = self.types.i64_type.const_int(0, false);
        let is_none = self
            .builder
            .build_int_compare(IntPredicate::EQ, a_truthy, zero, "is_none")
            .map_err(|e| e.to_string())?;

        let use_b_block = self
            .context
            .append_basic_block(function, &format!("coalesce_b_{ip}"));
        let done_block = self
            .context
            .append_basic_block(function, &format!("coalesce_done_{ip}"));

        self.builder
            .build_conditional_branch(is_none, use_b_block, done_block)
            .map_err(|e| e.to_string())?;

        self.builder.position_at_end(use_b_block);
        self.builder
            .build_call(move_fn, &[reg_slots[b].into(), reg_slots[a].into()], "")
            .map_err(|e| e.to_string())?;
        self.builder
            .build_unconditional_branch(done_block)
            .map_err(|e| e.to_string())?;

        self.builder.position_at_end(done_block);
        Ok(())
    }

    fn emit_call(
        &self,
        a: usize,
        b: usize,
        c: usize,
        reg_slots: &[PointerValue<'ctx>],
        ctx_param: PointerValue<'ctx>,
        error_block: BasicBlock<'ctx>,
    ) -> Result<(), String> {
        // Call: R[A] = R[B](R[B+1]..R[B+C])
        let call_fn = self.module.get_function("tl_rt_call").unwrap();

        // Args pointer: address of R[B+1] (or R[B] if c == 0, doesn't matter)
        let args_ptr = if c > 0 && b + 1 < reg_slots.len() {
            reg_slots[b + 1]
        } else {
            reg_slots[b] // won't be dereferenced if nargs=0
        };
        let nargs = self.types.i64_type.const_int(c as u64, false);

        let status = self
            .builder
            .build_call(
                call_fn,
                &[
                    ctx_param.into(),
                    reg_slots[b].into(),
                    args_ptr.into(),
                    nargs.into(),
                    reg_slots[a].into(),
                ],
                "call_status",
            )
            .map_err(|e| e.to_string())?;
        self.check_status(
            status.try_as_basic_value().unwrap_basic().into_int_value(),
            error_block,
        )?;
        Ok(())
    }

    fn emit_call_builtin(
        &self,
        a: usize,
        b: usize,
        c: usize,
        arg_count: usize,
        reg_slots: &[PointerValue<'ctx>],
        ctx_param: PointerValue<'ctx>,
        error_block: BasicBlock<'ctx>,
    ) -> Result<(), String> {
        let builtin_fn = self.module.get_function("tl_rt_call_builtin").unwrap();
        let builtin_id = self.types.i64_type.const_int(b as u64, false);
        let args_ptr = if c < reg_slots.len() {
            reg_slots[c]
        } else {
            reg_slots[0]
        };
        let nargs = self.types.i64_type.const_int(arg_count as u64, false);

        let status = self
            .builder
            .build_call(
                builtin_fn,
                &[
                    ctx_param.into(),
                    builtin_id.into(),
                    args_ptr.into(),
                    nargs.into(),
                    reg_slots[a].into(),
                ],
                "builtin_status",
            )
            .map_err(|e| e.to_string())?;
        self.check_status(
            status.try_as_basic_value().unwrap_basic().into_int_value(),
            error_block,
        )?;
        Ok(())
    }

    fn emit_new_list(
        &self,
        a: usize,
        b: usize,
        c: usize,
        reg_slots: &[PointerValue<'ctx>],
    ) -> Result<(), String> {
        let make_list_fn = self.module.get_function("tl_rt_make_list").unwrap();
        let vals_ptr = if c > 0 && b < reg_slots.len() {
            reg_slots[b]
        } else {
            reg_slots[a]
        };
        let count = self.types.i64_type.const_int(c as u64, false);
        self.builder
            .build_call(
                make_list_fn,
                &[vals_ptr.into(), count.into(), reg_slots[a].into()],
                "",
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn emit_new_map(
        &self,
        a: usize,
        b: usize,
        c: usize,
        reg_slots: &[PointerValue<'ctx>],
    ) -> Result<(), String> {
        let make_map_fn = self.module.get_function("tl_rt_make_map").unwrap();
        let keys_ptr = if b < reg_slots.len() {
            reg_slots[b]
        } else {
            reg_slots[a]
        };
        // Keys and values are interleaved: R[B], R[B+1], R[B+2], R[B+3], ...
        // For simplicity, pass the base pointer and count; runtime will handle layout
        let count = self.types.i64_type.const_int(c as u64, false);
        // keys_ptr = R[B] (even indices), vals_ptr = R[B+1] (odd indices)
        let vals_ptr = if b + 1 < reg_slots.len() {
            reg_slots[b + 1]
        } else {
            keys_ptr
        };
        self.builder
            .build_call(
                make_map_fn,
                &[
                    keys_ptr.into(),
                    vals_ptr.into(),
                    count.into(),
                    reg_slots[a].into(),
                ],
                "",
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn emit_get_index(
        &self,
        a: usize,
        b: usize,
        c: usize,
        reg_slots: &[PointerValue<'ctx>],
        error_block: BasicBlock<'ctx>,
    ) -> Result<(), String> {
        let func = self.module.get_function("tl_rt_get_index").unwrap();
        let status = self
            .builder
            .build_call(
                func,
                &[
                    reg_slots[b].into(),
                    reg_slots[c].into(),
                    reg_slots[a].into(),
                ],
                "getidx_status",
            )
            .map_err(|e| e.to_string())?;
        self.check_status(
            status.try_as_basic_value().unwrap_basic().into_int_value(),
            error_block,
        )?;
        Ok(())
    }

    fn emit_set_index(
        &self,
        a: usize,
        b: usize,
        c: usize,
        reg_slots: &[PointerValue<'ctx>],
        error_block: BasicBlock<'ctx>,
    ) -> Result<(), String> {
        let func = self.module.get_function("tl_rt_set_index").unwrap();
        let status = self
            .builder
            .build_call(
                func,
                &[
                    reg_slots[b].into(),
                    reg_slots[c].into(),
                    reg_slots[a].into(),
                ],
                "setidx_status",
            )
            .map_err(|e| e.to_string())?;
        self.check_status(
            status.try_as_basic_value().unwrap_basic().into_int_value(),
            error_block,
        )?;
        Ok(())
    }

    fn emit_get_member(
        &self,
        proto: &Prototype,
        a: usize,
        b: usize,
        c: usize,
        reg_slots: &[PointerValue<'ctx>],
        _ctx_param: PointerValue<'ctx>,
        error_block: BasicBlock<'ctx>,
    ) -> Result<(), String> {
        let func = self.module.get_function("tl_rt_get_member").unwrap();
        let name = match &proto.constants[c] {
            Constant::String(s) => s.as_ref(),
            _ => return Err("Expected string constant for member name".into()),
        };
        let name_global = self
            .builder
            .build_global_string_ptr(name, &format!("member_{name}"))
            .map_err(|e| e.to_string())?;
        let name_len = self.types.i64_type.const_int(name.len() as u64, false);

        let status = self
            .builder
            .build_call(
                func,
                &[
                    reg_slots[b].into(),
                    name_global.as_pointer_value().into(),
                    name_len.into(),
                    reg_slots[a].into(),
                ],
                "getmember_status",
            )
            .map_err(|e| e.to_string())?;
        self.check_status(
            status.try_as_basic_value().unwrap_basic().into_int_value(),
            error_block,
        )?;
        Ok(())
    }

    fn emit_set_member(
        &self,
        proto: &Prototype,
        a: usize,
        b: usize,
        c: usize,
        reg_slots: &[PointerValue<'ctx>],
        error_block: BasicBlock<'ctx>,
    ) -> Result<(), String> {
        let func = self.module.get_function("tl_rt_set_member").unwrap();
        let name = match &proto.constants[b] {
            Constant::String(s) => s.as_ref(),
            _ => return Err("Expected string constant for member name".into()),
        };
        let name_global = self
            .builder
            .build_global_string_ptr(name, &format!("setmember_{name}"))
            .map_err(|e| e.to_string())?;
        let name_len = self.types.i64_type.const_int(name.len() as u64, false);

        let status = self
            .builder
            .build_call(
                func,
                &[
                    reg_slots[a].into(),
                    name_global.as_pointer_value().into(),
                    name_len.into(),
                    reg_slots[c].into(),
                ],
                "setmember_status",
            )
            .map_err(|e| e.to_string())?;
        self.check_status(
            status.try_as_basic_value().unwrap_basic().into_int_value(),
            error_block,
        )?;
        Ok(())
    }

    fn emit_method_call(
        &self,
        proto: &Prototype,
        a: usize,
        b: usize,
        c: usize,
        args_start: usize,
        arg_count: usize,
        reg_slots: &[PointerValue<'ctx>],
        ctx_param: PointerValue<'ctx>,
        error_block: BasicBlock<'ctx>,
    ) -> Result<(), String> {
        let func = self.module.get_function("tl_rt_method_call").unwrap();
        let name = match &proto.constants[c] {
            Constant::String(s) => s.as_ref(),
            _ => return Err("Expected string constant for method name".into()),
        };
        let name_global = self
            .builder
            .build_global_string_ptr(name, &format!("method_{name}"))
            .map_err(|e| e.to_string())?;
        let name_len = self.types.i64_type.const_int(name.len() as u64, false);
        let args_ptr = if arg_count > 0 && args_start < reg_slots.len() {
            reg_slots[args_start]
        } else {
            reg_slots[0] // won't be used
        };
        let nargs = self.types.i64_type.const_int(arg_count as u64, false);

        let status = self
            .builder
            .build_call(
                func,
                &[
                    ctx_param.into(),
                    reg_slots[b].into(),
                    name_global.as_pointer_value().into(),
                    name_len.into(),
                    args_ptr.into(),
                    nargs.into(),
                    reg_slots[a].into(),
                ],
                "method_status",
            )
            .map_err(|e| e.to_string())?;
        self.check_status(
            status.try_as_basic_value().unwrap_basic().into_int_value(),
            error_block,
        )?;
        Ok(())
    }

    fn emit_vm_fallback(
        &self,
        inst: u32,
        reg_slots: &[PointerValue<'ctx>],
        ctx_param: PointerValue<'ctx>,
        _proto_ptr_val: PointerValue<'ctx>,
        num_regs: usize,
        error_block: BasicBlock<'ctx>,
    ) -> Result<(), String> {
        let vm_exec_fn = self.module.get_function("tl_rt_vm_exec_op").unwrap();
        let opcode = self
            .types
            .i64_type
            .const_int(((inst >> 24) & 0xFF) as u64, false);
        let a_val = self
            .types
            .i64_type
            .const_int(((inst >> 16) & 0xFF) as u64, false);
        let b_val = self
            .types
            .i64_type
            .const_int(((inst >> 8) & 0xFF) as u64, false);
        let c_val = self.types.i64_type.const_int((inst & 0xFF) as u64, false);
        let regs_base = reg_slots[0]; // base of register array
        let nr = self.types.i64_type.const_int(num_regs as u64, false);

        let status = self
            .builder
            .build_call(
                vm_exec_fn,
                &[
                    ctx_param.into(),
                    opcode.into(),
                    a_val.into(),
                    b_val.into(),
                    c_val.into(),
                    regs_base.into(),
                    nr.into(),
                ],
                "vm_fallback_status",
            )
            .map_err(|e| e.to_string())?;
        self.check_status(
            status.try_as_basic_value().unwrap_basic().into_int_value(),
            error_block,
        )?;
        Ok(())
    }

    fn check_status(
        &self,
        status: IntValue<'ctx>,
        error_block: BasicBlock<'ctx>,
    ) -> Result<(), String> {
        let zero = self.types.i64_type.const_int(0, false);
        let is_error = self
            .builder
            .build_int_compare(IntPredicate::NE, status, zero, "is_err")
            .map_err(|e| e.to_string())?;
        let current_fn = self
            .builder
            .get_insert_block()
            .unwrap()
            .get_parent()
            .unwrap();
        let continue_block = self.context.append_basic_block(current_fn, "ok");
        self.builder
            .build_conditional_branch(is_error, error_block, continue_block)
            .map_err(|e| e.to_string())?;
        self.builder.position_at_end(continue_block);
        Ok(())
    }
}
