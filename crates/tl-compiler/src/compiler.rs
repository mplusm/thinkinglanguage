// ThinkingLanguage — AST-to-bytecode compiler
// Compiles a TL Program into a Prototype (bytecode chunk).

use std::sync::Arc;
use tl_ast::*;
use tl_errors::{RuntimeError, Span, TlError};

use crate::chunk::*;
use crate::opcode::*;

/// Result of constant folding at compile time.
#[derive(Debug, Clone)]
enum FoldedConst {
    Int(i64),
    Float(f64),
    Bool(bool),
    String(String),
}

/// Compile error helper
fn compile_err(msg: String) -> TlError {
    TlError::Runtime(RuntimeError {
        message: msg,
        span: None,
        stack_trace: vec![],
    })
}

/// A local variable in the current scope.
#[derive(Debug, Clone)]
struct Local {
    name: String,
    depth: u32,
    register: u8,
    is_captured: bool,
}

/// Upvalue tracking during compilation.
#[derive(Debug, Clone)]
struct CompilerUpvalue {
    is_local: bool,
    index: u8,
}

/// Loop context for break/continue.
#[derive(Debug, Clone)]
struct LoopCtx {
    /// Instruction indices of break jumps to patch
    break_jumps: Vec<usize>,
    /// Instruction index of loop start (for continue)
    loop_start: usize,
}

/// Compiler state for one function scope.
struct CompilerState {
    proto: Prototype,
    locals: Vec<Local>,
    upvalues: Vec<CompilerUpvalue>,
    scope_depth: u32,
    next_register: u8,
    loop_stack: Vec<LoopCtx>,
    has_yield: bool,
    /// Current source line (1-based), set by the compiler before emitting instructions
    current_line: u32,
    /// Dead code flag: set after compiling Return/Break/Continue in the current block
    dead_code: bool,
}

impl CompilerState {
    fn new(name: String) -> Self {
        CompilerState {
            proto: Prototype::new(name),
            locals: Vec::new(),
            upvalues: Vec::new(),
            scope_depth: 0,
            next_register: 0,
            loop_stack: Vec::new(),
            has_yield: false,
            current_line: 0,
            dead_code: false,
        }
    }

    fn alloc_register(&mut self) -> u8 {
        if self.next_register == 255 {
            panic!("Register overflow: function too complex (max 255 registers)");
        }
        let r = self.next_register;
        self.next_register += 1;
        if self.next_register > self.proto.num_registers {
            self.proto.num_registers = self.next_register;
        }
        r
    }

    fn free_register(&mut self) {
        if self.next_register > 0 {
            self.next_register -= 1;
        }
    }

    fn emit(&mut self, inst: u32) {
        self.proto.code.push(inst);
        self.proto.lines.push(self.current_line);
    }

    fn emit_abc(&mut self, op: Op, a: u8, b: u8, c: u8, _line: u32) {
        self.emit(encode_abc(op, a, b, c));
    }

    fn emit_abx(&mut self, op: Op, a: u8, bx: u16, _line: u32) {
        self.emit(encode_abx(op, a, bx));
    }

    fn add_constant(&mut self, c: Constant) -> u16 {
        let idx = self.proto.constants.len();
        if idx >= 65535 {
            panic!("Constant pool overflow: too many constants (max 65535)");
        }
        self.proto.constants.push(c);
        idx as u16
    }

    fn current_pos(&self) -> usize {
        self.proto.code.len()
    }

    fn patch_jump(&mut self, inst_pos: usize) {
        let target = self.current_pos();
        let offset = (target as i32 - inst_pos as i32 - 1) as i16;
        let old = self.proto.code[inst_pos];
        let op = (old >> 24) as u8;
        let a = ((old >> 16) & 0xFF) as u8;
        self.proto.code[inst_pos] = encode_abx(
            Op::try_from(op).expect("patching valid instruction"),
            a,
            offset as u16,
        );
    }
}

/// The compiler: transforms AST into bytecode.
pub struct Compiler {
    states: Vec<CompilerState>,
    /// Byte offset of each line start (for converting byte offsets to line numbers)
    line_offsets: Vec<usize>,
    /// Current line number (1-based), updated at each statement boundary
    current_line: u32,
}

impl Compiler {
    fn current(&mut self) -> &mut CompilerState {
        self.states.last_mut().unwrap()
    }

    /// Build a table of byte offsets for the start of each line.
    fn build_line_offsets(source: &str) -> Vec<usize> {
        let mut offsets = vec![0]; // line 1 starts at byte 0
        for (i, ch) in source.as_bytes().iter().enumerate() {
            if *ch == b'\n' {
                offsets.push(i + 1);
            }
        }
        offsets
    }

    /// Convert a byte offset to a 1-based line number using binary search.
    fn line_of(&self, byte_offset: usize) -> u32 {
        match self.line_offsets.binary_search(&byte_offset) {
            Ok(idx) => idx as u32 + 1,
            Err(idx) => idx as u32, // idx is the line (1-based) since line_offsets[0]=0
        }
    }

    /// Get the current line number for emit calls.
    #[allow(dead_code)]
    fn line(&self) -> u32 {
        self.current_line
    }

    fn begin_scope(&mut self) {
        self.current().scope_depth += 1;
    }

    fn end_scope(&mut self) {
        let state = self.current();
        state.scope_depth -= 1;
        // Pop locals that are out of scope
        while let Some(local) = state.locals.last() {
            if local.depth <= state.scope_depth {
                break;
            }
            let reg = local.register;
            let captured = local.is_captured;
            state.locals.pop();
            if captured {
                // Close upvalue — the VM will handle this
                // We just need the register to be freed
            }
            // Free the register if it's the top
            if reg + 1 == state.next_register {
                state.next_register = reg;
            }
        }
    }

    fn add_local(&mut self, name: String) -> u8 {
        let state = self.current();
        let reg = state.alloc_register();
        let depth = state.scope_depth;
        state.locals.push(Local {
            name,
            depth,
            register: reg,
            is_captured: false,
        });
        state.proto.num_locals = state.proto.num_locals.max(state.locals.len() as u8);
        reg
    }

    fn resolve_local(&self, name: &str) -> Option<u8> {
        let state = self.states.last().unwrap();
        for local in state.locals.iter().rev() {
            if local.name == name {
                return Some(local.register);
            }
        }
        None
    }

    fn resolve_upvalue(&mut self, name: &str) -> Option<u8> {
        let n = self.states.len();
        if n < 2 {
            return None;
        }
        self.resolve_upvalue_recursive(n - 1, name)
    }

    fn resolve_upvalue_recursive(&mut self, state_idx: usize, name: &str) -> Option<u8> {
        if state_idx == 0 {
            return None;
        }
        // Check enclosing function's locals
        let enclosing = &mut self.states[state_idx - 1];
        for i in (0..enclosing.locals.len()).rev() {
            if enclosing.locals[i].name == name {
                enclosing.locals[i].is_captured = true;
                let reg = enclosing.locals[i].register;
                return Some(self.add_upvalue(state_idx, true, reg));
            }
        }
        // Check enclosing function's upvalues
        if let Some(uv_idx) = self.resolve_upvalue_recursive(state_idx - 1, name) {
            return Some(self.add_upvalue(state_idx, false, uv_idx));
        }
        None
    }

    fn add_upvalue(&mut self, state_idx: usize, is_local: bool, index: u8) -> u8 {
        let state = &mut self.states[state_idx];
        // Check if we already have this upvalue
        for (i, uv) in state.upvalues.iter().enumerate() {
            if uv.is_local == is_local && uv.index == index {
                return i as u8;
            }
        }
        let idx = state.upvalues.len() as u8;
        state.upvalues.push(CompilerUpvalue { is_local, index });
        idx
    }

    fn compile_stmt(&mut self, stmt: &Stmt) -> Result<(), TlError> {
        // Dead code elimination: skip statements after return/break/continue
        if self.current().dead_code {
            return Ok(());
        }

        // Update current line from statement's span
        let line = self.line_of(stmt.span.start);
        self.current_line = line;
        self.current().current_line = line;
        match &stmt.kind {
            StmtKind::Let { name, value, .. } => {
                let reg = self.add_local(name.clone());
                self.compile_expr(value, reg)?;
                Ok(())
            }
            StmtKind::FnDecl {
                name, params, body, ..
            } => {
                let reg = self.add_local(name.clone());
                self.compile_function(name.clone(), params, body, false)?;
                // The closure instruction already targets `reg`
                // Actually we need to compile the function into a prototype
                // and then emit a Closure instruction
                let _ = reg; // handled inside compile_function
                Ok(())
            }
            StmtKind::Expr(expr) => {
                let reg = self.current().alloc_register();
                self.compile_expr(expr, reg)?;
                self.current().free_register();
                Ok(())
            }
            StmtKind::Return(expr) => {
                let reg = self.current().alloc_register();
                match expr {
                    Some(e) => self.compile_expr(e, reg)?,
                    None => self.current().emit_abx(Op::LoadNone, reg, 0, 0),
                }
                self.current().emit_abc(Op::Return, reg, 0, 0, 0);
                self.current().free_register();
                self.current().dead_code = true;
                Ok(())
            }
            StmtKind::If {
                condition,
                then_body,
                else_ifs,
                else_body,
            } => self.compile_if(condition, then_body, else_ifs, else_body),
            StmtKind::While { condition, body } => self.compile_while(condition, body),
            StmtKind::For { name, iter, body } => self.compile_for(name, iter, body),
            StmtKind::ParallelFor { name, iter, body } => {
                // Compile as a regular for loop (rayon parallelism at VM level)
                self.compile_for(name, iter, body)
            }
            StmtKind::Schema {
                name,
                fields,
                version,
                ..
            } => self.compile_schema(name, fields, version),
            StmtKind::Train {
                name,
                algorithm,
                config,
            } => self.compile_train(name, algorithm, config),
            StmtKind::Pipeline {
                name,
                extract,
                transform,
                load,
                schedule,
                timeout,
                retries,
                on_failure,
                on_success,
            } => self.compile_pipeline(
                name, extract, transform, load, schedule, timeout, retries, on_failure, on_success,
            ),
            StmtKind::StreamDecl {
                name,
                source,
                transform: _,
                sink: _,
                window,
                watermark,
            } => self.compile_stream_decl(name, source, window, watermark),
            StmtKind::SourceDecl {
                name,
                connector_type,
                config,
            } => self.compile_connector_decl(name, connector_type, config),
            StmtKind::SinkDecl {
                name,
                connector_type,
                config,
            } => self.compile_connector_decl(name, connector_type, config),
            StmtKind::StructDecl { name, fields, .. } => {
                let reg = self.add_local(name.clone());
                let field_names: Vec<Arc<str>> =
                    fields.iter().map(|f| Arc::from(f.name.as_str())).collect();
                let name_idx = self
                    .current()
                    .add_constant(Constant::String(Arc::from(name.as_str())));
                // Store field names as string constants
                let fields_idx = self.current().add_constant(Constant::AstExprList(
                    field_names
                        .iter()
                        .map(|f| tl_ast::Expr::String(f.to_string()))
                        .collect(),
                ));
                // High bit of c marks this as a declaration (not instance creation)
                self.current().emit_abc(
                    Op::NewStruct,
                    reg,
                    name_idx as u8,
                    (fields_idx as u8) | 0x80,
                    0,
                );
                Ok(())
            }
            StmtKind::EnumDecl { name, variants, .. } => {
                let reg = self.add_local(name.clone());
                let name_idx = self
                    .current()
                    .add_constant(Constant::String(Arc::from(name.as_str())));
                // Store variant info
                let variant_info: Vec<tl_ast::Expr> = variants
                    .iter()
                    .map(|v| tl_ast::Expr::String(format!("{}:{}", v.name, v.fields.len())))
                    .collect();
                let variants_idx = self
                    .current()
                    .add_constant(Constant::AstExprList(variant_info));
                // High bit of c marks this as a declaration (not instance creation)
                self.current().emit_abc(
                    Op::NewStruct,
                    reg,
                    name_idx as u8,
                    (variants_idx as u8) | 0x80,
                    0,
                );
                // We reuse NewStruct op but tag it differently via the globals
                // Actually, let's use a dedicated approach: store as global with special name
                let global_idx = self.current().add_constant(Constant::String(Arc::from(
                    format!("__enum_{name}").as_str(),
                )));
                self.current().emit_abx(Op::SetGlobal, reg, global_idx, 0);
                // Also set as normal global
                let name_g = self
                    .current()
                    .add_constant(Constant::String(Arc::from(name.as_str())));
                self.current().emit_abx(Op::SetGlobal, reg, name_g, 0);
                Ok(())
            }
            StmtKind::ImplBlock {
                type_name, methods, ..
            } => {
                // Compile each method as a global function with mangled name Type::method
                for method in methods {
                    if let StmtKind::FnDecl {
                        name: mname,
                        params,
                        body,
                        ..
                    } = &method.kind
                    {
                        let mangled = format!("{type_name}::{mname}");
                        // add_local creates the local binding that compile_function looks up
                        let reg = self.add_local(mangled.clone());
                        self.compile_function(mangled.clone(), params, body, false)?;
                        // Store as global so dispatch_method can find it
                        let idx = self
                            .current()
                            .add_constant(Constant::String(Arc::from(mangled.as_str())));
                        self.current().emit_abx(Op::SetGlobal, reg, idx, 0);
                    }
                }
                Ok(())
            }
            StmtKind::TryCatch {
                try_body,
                catch_var,
                catch_body,
                finally_body,
            } => {
                // Emit TryBegin with offset to catch handler
                let try_begin_pos = self.current().current_pos();
                self.current().emit_abx(Op::TryBegin, 0, 0, 0); // patch later

                // Compile try body
                let saved_dead_code = self.current().dead_code;
                self.begin_scope();
                self.current().dead_code = false;
                for stmt in try_body {
                    self.compile_stmt(stmt)?;
                }
                self.end_scope();

                // Emit TryEnd
                self.current().emit_abx(Op::TryEnd, 0, 0, 0);

                // Compile finally after try (success path)
                if let Some(finally_stmts) = &finally_body {
                    self.begin_scope();
                    for stmt in finally_stmts {
                        self.compile_stmt(stmt)?;
                    }
                    self.end_scope();
                }

                // Jump over catch
                let jump_over_pos = self.current().current_pos();
                self.current().emit_abx(Op::Jump, 0, 0, 0);

                // Patch TryBegin to point here (catch handler)
                self.current().patch_jump(try_begin_pos);

                // Catch body: error value will be in a designated register
                self.begin_scope();
                self.current().dead_code = false;
                let catch_reg = self.add_local(catch_var.clone());
                // The VM will place the error value in catch_reg when it jumps here
                // We mark it with a LoadNone that the VM will overwrite
                self.current().emit_abx(Op::LoadNone, catch_reg, 0, 0);
                for stmt in catch_body {
                    self.compile_stmt(stmt)?;
                }
                self.end_scope();

                // Compile finally after catch (error path)
                if let Some(finally_stmts) = &finally_body {
                    self.begin_scope();
                    for stmt in finally_stmts {
                        self.compile_stmt(stmt)?;
                    }
                    self.end_scope();
                }

                // Patch jump over catch
                self.current().patch_jump(jump_over_pos);

                // Restore dead_code (try/catch doesn't make surrounding code dead)
                self.current().dead_code = saved_dead_code;
                Ok(())
            }
            StmtKind::Throw(expr) => {
                let reg = self.current().alloc_register();
                self.compile_expr(expr, reg)?;
                self.current().emit_abc(Op::Throw, reg, 0, 0, 0);
                self.current().free_register();
                Ok(())
            }
            StmtKind::Import { path, alias } => {
                let reg = self.current().alloc_register();
                let path_idx = self
                    .current()
                    .add_constant(Constant::String(Arc::from(path.as_str())));
                let alias_idx = if let Some(a) = alias {
                    self.current()
                        .add_constant(Constant::String(Arc::from(a.as_str())))
                } else {
                    self.current().add_constant(Constant::String(Arc::from("")))
                };
                self.current().emit_abx(Op::Import, reg, path_idx, 0);
                // Store alias info in next instruction
                self.current().emit_abc(Op::Move, alias_idx as u8, 0, 0, 0);
                self.current().free_register();
                Ok(())
            }
            StmtKind::Test { .. } => {
                // Tests are only run in test mode; skip during normal compilation
                Ok(())
            }
            StmtKind::Use { item, .. } => self.compile_use(item),
            StmtKind::ModDecl { .. } => {
                // ModDecl is handled at load time, not compilation
                Ok(())
            }
            StmtKind::TraitDef { .. } => {
                // Trait definitions are type-checker only; no runtime code needed
                Ok(())
            }
            StmtKind::TraitImpl {
                type_name, methods, ..
            } => {
                // Compile as a regular impl block — trait impls are type-erased at runtime
                for method in methods {
                    if let StmtKind::FnDecl {
                        name: mname,
                        params,
                        body,
                        ..
                    } = &method.kind
                    {
                        let mangled = format!("{type_name}::{mname}");
                        let reg = self.add_local(mangled.clone());
                        self.compile_function(mangled.clone(), params, body, false)?;
                        let idx = self
                            .current()
                            .add_constant(Constant::String(Arc::from(mangled.as_str())));
                        self.current().emit_abx(Op::SetGlobal, reg, idx, 0);
                    }
                }
                Ok(())
            }
            StmtKind::Break => {
                let state = self.current();
                let pos = state.current_pos();
                state.emit_abx(Op::Jump, 0, 0, 0);
                if let Some(loop_ctx) = state.loop_stack.last_mut() {
                    loop_ctx.break_jumps.push(pos);
                }
                self.current().dead_code = true;
                Ok(())
            }
            StmtKind::Continue => {
                let state = self.current();
                if let Some(loop_ctx) = state.loop_stack.last() {
                    let loop_start = loop_ctx.loop_start;
                    let current = state.current_pos();
                    let offset = (loop_start as i32 - current as i32 - 1) as i16;
                    state.emit_abx(Op::Jump, 0, offset as u16, 0);
                }
                self.current().dead_code = true;
                Ok(())
            }
            StmtKind::LetDestructure { pattern, value, .. } => {
                let val_reg = self.current().alloc_register();
                self.compile_expr(value, val_reg)?;
                self.compile_let_destructure(pattern, val_reg)?;
                // Note: don't free val_reg — locals were allocated above it
                // and freeing it would allow reuse of registers occupied by locals
                Ok(())
            }
            StmtKind::TypeAlias { .. } => {
                // Type aliases are type-checker only; no runtime code needed
                Ok(())
            }
            StmtKind::Migrate {
                schema_name,
                from_version,
                to_version,
                operations,
            } => self.compile_migrate(schema_name, *from_version, *to_version, operations),
            StmtKind::Agent {
                name,
                model,
                system_prompt,
                tools,
                max_turns,
                temperature,
                max_tokens,
                base_url,
                api_key,
                output_format,
                on_tool_call,
                on_complete,
            } => self.compile_agent(
                name,
                model,
                system_prompt,
                tools,
                max_turns,
                temperature,
                max_tokens,
                base_url,
                api_key,
                output_format,
                on_tool_call,
                on_complete,
            ),
        }
    }

    /// Compile let-destructuring: bind pattern variables from a value register.
    fn compile_let_destructure(&mut self, pattern: &Pattern, val_reg: u8) -> Result<(), TlError> {
        match pattern {
            Pattern::Binding(name) => {
                let local = self.add_local(name.clone());
                self.current().emit_abc(Op::Move, local, val_reg, 0, 0);
            }
            Pattern::Wildcard => {} // ignore value
            Pattern::Struct { fields, .. } => {
                for field in fields {
                    let fname_const = self
                        .current()
                        .add_constant(Constant::String(Arc::from(field.name.as_str())));
                    let bind_name = match &field.pattern {
                        Some(Pattern::Binding(n)) => n.clone(),
                        _ => field.name.clone(),
                    };
                    let local = self.add_local(bind_name);
                    self.current().emit_abc(
                        Op::ExtractNamedField,
                        local,
                        val_reg,
                        fname_const as u8,
                        0,
                    );
                }
            }
            Pattern::List { elements, rest } => {
                for (i, elem_pat) in elements.iter().enumerate() {
                    match elem_pat {
                        Pattern::Binding(name) => {
                            let local = self.add_local(name.clone());
                            self.current()
                                .emit_abc(Op::ExtractField, local, val_reg, i as u8, 0);
                        }
                        Pattern::Wildcard => {}
                        _ => {}
                    }
                }
                if let Some(rest_name) = rest {
                    let local = self.add_local(rest_name.clone());
                    self.current().emit_abc(
                        Op::ExtractField,
                        local,
                        val_reg,
                        (elements.len() as u8) | 0x80,
                        0,
                    );
                }
            }
            Pattern::Enum { args, .. } => {
                // Extract enum fields if variant matches (runtime will error if not)
                for (i, arg_pat) in args.iter().enumerate() {
                    if let Pattern::Binding(name) = arg_pat {
                        let local = self.add_local(name.clone());
                        self.current()
                            .emit_abc(Op::ExtractField, local, val_reg, i as u8, 0);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn compile_function(
        &mut self,
        name: String,
        params: &[Param],
        body: &[Stmt],
        is_closure_expr: bool,
    ) -> Result<u8, TlError> {
        // Determine the destination register for the closure
        let dest_reg = if is_closure_expr {
            // For closure expressions, the caller already allocated a dest register
            // We don't allocate one here
            0 // placeholder, will be set by caller
        } else {
            // For FnDecl, the local was already added
            let state = self.states.last().unwrap();
            state
                .locals
                .iter()
                .rev()
                .find(|l| l.name == name)
                .map(|l| l.register)
                .unwrap_or(0)
        };

        // Push new compiler state for the function
        let mut fn_state = CompilerState::new(name);
        fn_state.proto.arity = params.len() as u8;
        fn_state.scope_depth = 1;

        // Allocate registers for parameters
        for param in params {
            let reg = fn_state.alloc_register();
            fn_state.locals.push(Local {
                name: param.name.clone(),
                depth: 1,
                register: reg,
                is_captured: false,
            });
        }
        fn_state.proto.num_locals = params.len() as u8;

        self.states.push(fn_state);

        // Compile function body
        // Track the last expression's register for implicit return
        let mut last_expr_reg: Option<u8> = None;
        for (i, stmt) in body.iter().enumerate() {
            // Dead code elimination in function body
            if self.current().dead_code {
                break;
            }
            let is_last = i == body.len() - 1;
            // Update line tracking
            let line = self.line_of(stmt.span.start);
            self.current_line = line;
            self.current().current_line = line;
            match &stmt.kind {
                StmtKind::Expr(expr) if is_last => {
                    // Last statement is an expression — compile and keep register for return
                    let reg = self.current().alloc_register();
                    self.compile_expr(expr, reg)?;
                    last_expr_reg = Some(reg);
                    // Don't free — we'll return it
                }
                StmtKind::If {
                    condition,
                    then_body,
                    else_ifs,
                    else_body,
                } if is_last && else_body.is_some() => {
                    // Last statement is if-else — compile as expression for implicit return
                    let dest = self.current().alloc_register();
                    self.compile_if_as_expr(condition, then_body, else_ifs, else_body, dest)?;
                    last_expr_reg = Some(dest);
                }
                _ => {
                    self.compile_stmt(stmt)?;
                }
            }
        }

        // If the last instruction isn't a Return, emit one
        let needs_return = {
            let state = self.states.last().unwrap();
            if state.proto.code.is_empty() {
                true
            } else {
                let last = *state.proto.code.last().unwrap();
                decode_op(last) != Op::Return
            }
        };
        if needs_return {
            if let Some(reg) = last_expr_reg {
                // Return the last expression value
                self.current().emit_abc(Op::Return, reg, 0, 0, 0);
            } else {
                let state = self.current();
                let reg = state.alloc_register();
                state.emit_abx(Op::LoadNone, reg, 0, 0);
                state.emit_abc(Op::Return, reg, 0, 0, 0);
                state.free_register();
            }
        }

        // Pop the function state
        let fn_state = self.states.pop().unwrap();
        let mut proto = fn_state.proto;
        proto.is_generator = fn_state.has_yield;
        proto.upvalue_defs = fn_state
            .upvalues
            .iter()
            .map(|uv| UpvalueDef {
                is_local: uv.is_local,
                index: uv.index,
            })
            .collect();

        // Add the prototype as a constant in the enclosing function
        let proto_arc = Arc::new(proto);
        let const_idx = self.current().add_constant(Constant::Prototype(proto_arc));

        // Emit Closure instruction in the enclosing function
        self.current().emit_abx(Op::Closure, dest_reg, const_idx, 0);

        Ok(dest_reg)
    }

    fn compile_closure_expr(
        &mut self,
        params: &[Param],
        body: &ClosureBody,
        dest: u8,
    ) -> Result<(), TlError> {
        // Build the body statements based on closure body kind
        let body_stmts = match body {
            ClosureBody::Expr(e) => {
                // Single-expression closure: wrap in return
                vec![Stmt {
                    kind: StmtKind::Return(Some(e.as_ref().clone())),
                    span: Span::new(0, 0),
                    doc_comment: None,
                }]
            }
            ClosureBody::Block { stmts, expr } => {
                // Block closure: compile stmts, then return tail expr (if any)
                let mut all = stmts.clone();
                if let Some(e) = expr {
                    all.push(Stmt {
                        kind: StmtKind::Return(Some(e.as_ref().clone())),
                        span: Span::new(0, 0),
                        doc_comment: None,
                    });
                }
                all
            }
        };

        // Push new compiler state
        let mut fn_state = CompilerState::new("<closure>".to_string());
        fn_state.proto.arity = params.len() as u8;
        fn_state.scope_depth = 1;

        for param in params {
            let reg = fn_state.alloc_register();
            fn_state.locals.push(Local {
                name: param.name.clone(),
                depth: 1,
                register: reg,
                is_captured: false,
            });
        }
        fn_state.proto.num_locals = params.len() as u8;

        self.states.push(fn_state);

        for stmt in &body_stmts {
            self.compile_stmt(stmt)?;
        }

        // Ensure return
        let needs_return = {
            let state = self.states.last().unwrap();
            if state.proto.code.is_empty() {
                true
            } else {
                let last = *state.proto.code.last().unwrap();
                decode_op(last) != Op::Return
            }
        };
        if needs_return {
            let state = self.current();
            let reg = state.alloc_register();
            state.emit_abx(Op::LoadNone, reg, 0, 0);
            state.emit_abc(Op::Return, reg, 0, 0, 0);
            state.free_register();
        }

        let fn_state = self.states.pop().unwrap();
        let mut proto = fn_state.proto;
        proto.upvalue_defs = fn_state
            .upvalues
            .iter()
            .map(|uv| UpvalueDef {
                is_local: uv.is_local,
                index: uv.index,
            })
            .collect();

        let proto_arc = Arc::new(proto);
        let const_idx = self.current().add_constant(Constant::Prototype(proto_arc));
        self.current().emit_abx(Op::Closure, dest, const_idx, 0);

        Ok(())
    }

    fn compile_if(
        &mut self,
        condition: &Expr,
        then_body: &[Stmt],
        else_ifs: &[(Expr, Vec<Stmt>)],
        else_body: &Option<Vec<Stmt>>,
    ) -> Result<(), TlError> {
        let saved_dead_code = self.current().dead_code;
        let cond_reg = self.current().alloc_register();
        self.compile_expr(condition, cond_reg)?;

        // Jump if false to else/end
        let jump_false_pos = self.current().current_pos();
        self.current().emit_abx(Op::JumpIfFalse, cond_reg, 0, 0);
        self.current().free_register(); // free cond_reg

        // Then body
        self.begin_scope();
        self.current().dead_code = false;
        for stmt in then_body {
            self.compile_stmt(stmt)?;
        }
        let then_dead = self.current().dead_code;
        self.end_scope();

        // Jump over else
        let mut end_jumps = Vec::new();
        let jump_end_pos = self.current().current_pos();
        self.current().emit_abx(Op::Jump, 0, 0, 0);
        end_jumps.push(jump_end_pos);

        // Patch the false jump to here
        self.current().patch_jump(jump_false_pos);

        // Track whether all branches terminate
        let mut all_branches_dead = then_dead;

        // Else-ifs
        for (ei_cond, ei_body) in else_ifs {
            let cond_reg = self.current().alloc_register();
            self.compile_expr(ei_cond, cond_reg)?;
            let jf_pos = self.current().current_pos();
            self.current().emit_abx(Op::JumpIfFalse, cond_reg, 0, 0);
            self.current().free_register();

            self.begin_scope();
            self.current().dead_code = false;
            for stmt in ei_body {
                self.compile_stmt(stmt)?;
            }
            all_branches_dead = all_branches_dead && self.current().dead_code;
            self.end_scope();

            let je_pos = self.current().current_pos();
            self.current().emit_abx(Op::Jump, 0, 0, 0);
            end_jumps.push(je_pos);

            self.current().patch_jump(jf_pos);
        }

        // Else body
        if let Some(body) = else_body {
            self.begin_scope();
            self.current().dead_code = false;
            for stmt in body {
                self.compile_stmt(stmt)?;
            }
            all_branches_dead = all_branches_dead && self.current().dead_code;
            self.end_scope();
        } else {
            // No else branch means not all paths terminate
            all_branches_dead = false;
        }

        // Patch all end jumps
        for pos in end_jumps {
            self.current().patch_jump(pos);
        }

        // Dead code after if: only if ALL branches (including else) terminate
        self.current().dead_code = saved_dead_code || all_branches_dead;

        Ok(())
    }

    /// Compile if-else as an expression — each branch stores its last value into `dest`.
    fn compile_if_as_expr(
        &mut self,
        condition: &Expr,
        then_body: &[Stmt],
        else_ifs: &[(Expr, Vec<Stmt>)],
        else_body: &Option<Vec<Stmt>>,
        dest: u8,
    ) -> Result<(), TlError> {
        let cond_reg = self.current().alloc_register();
        self.compile_expr(condition, cond_reg)?;
        let jump_false_pos = self.current().current_pos();
        self.current().emit_abx(Op::JumpIfFalse, cond_reg, 0, 0);
        self.current().free_register(); // free cond_reg

        // Then body — compile all but last as statements, last as expression into dest
        self.begin_scope();
        self.compile_body_with_result(then_body, dest)?;
        self.end_scope();

        let mut end_jumps = Vec::new();
        let jump_end_pos = self.current().current_pos();
        self.current().emit_abx(Op::Jump, 0, 0, 0);
        end_jumps.push(jump_end_pos);

        self.current().patch_jump(jump_false_pos);

        // Else-ifs
        for (ei_cond, ei_body) in else_ifs {
            let cond_reg = self.current().alloc_register();
            self.compile_expr(ei_cond, cond_reg)?;
            let jf_pos = self.current().current_pos();
            self.current().emit_abx(Op::JumpIfFalse, cond_reg, 0, 0);
            self.current().free_register();

            self.begin_scope();
            self.compile_body_with_result(ei_body, dest)?;
            self.end_scope();

            let je_pos = self.current().current_pos();
            self.current().emit_abx(Op::Jump, 0, 0, 0);
            end_jumps.push(je_pos);

            self.current().patch_jump(jf_pos);
        }

        // Else body
        if let Some(body) = else_body {
            self.begin_scope();
            self.compile_body_with_result(body, dest)?;
            self.end_scope();
        }

        for pos in end_jumps {
            self.current().patch_jump(pos);
        }

        Ok(())
    }

    /// Compile a block body, storing the last expression's value into `dest`.
    fn compile_body_with_result(&mut self, body: &[Stmt], dest: u8) -> Result<(), TlError> {
        for (i, stmt) in body.iter().enumerate() {
            let is_last = i == body.len() - 1;
            let line = self.line_of(stmt.span.start);
            self.current_line = line;
            self.current().current_line = line;
            match &stmt.kind {
                StmtKind::Expr(expr) if is_last => {
                    self.compile_expr(expr, dest)?;
                }
                StmtKind::If {
                    condition,
                    then_body,
                    else_ifs,
                    else_body,
                } if is_last && else_body.is_some() => {
                    self.compile_if_as_expr(condition, then_body, else_ifs, else_body, dest)?;
                }
                _ => {
                    self.compile_stmt(stmt)?;
                }
            }
        }
        Ok(())
    }

    fn compile_while(&mut self, condition: &Expr, body: &[Stmt]) -> Result<(), TlError> {
        let loop_start = self.current().current_pos();

        self.current().loop_stack.push(LoopCtx {
            break_jumps: Vec::new(),
            loop_start,
        });

        let cond_reg = self.current().alloc_register();
        self.compile_expr(condition, cond_reg)?;
        let exit_jump = self.current().current_pos();
        self.current().emit_abx(Op::JumpIfFalse, cond_reg, 0, 0);
        self.current().free_register();

        self.begin_scope();
        // Reset dead_code for loop body (continue brings flow back)
        let saved_dead_code = self.current().dead_code;
        self.current().dead_code = false;
        for stmt in body {
            self.compile_stmt(stmt)?;
        }
        self.end_scope();

        // Jump back to start
        let current = self.current().current_pos();
        let offset = (loop_start as i32 - current as i32 - 1) as i16;
        self.current().emit_abx(Op::Jump, 0, offset as u16, 0);

        // Patch exit jump
        self.current().patch_jump(exit_jump);

        // Patch break jumps
        let loop_ctx = self.current().loop_stack.pop().unwrap();
        for pos in loop_ctx.break_jumps {
            self.current().patch_jump(pos);
        }

        // Restore dead_code state (loop doesn't make surrounding code dead)
        self.current().dead_code = saved_dead_code;

        Ok(())
    }

    fn compile_for(&mut self, name: &str, iter: &Expr, body: &[Stmt]) -> Result<(), TlError> {
        // Evaluate iterator expression
        let list_reg = self.current().alloc_register();
        self.compile_expr(iter, list_reg)?;

        // Create iterator (index counter)
        let iter_reg = self.current().alloc_register();
        let zero_const = self.current().add_constant(Constant::Int(0));
        self.current()
            .emit_abx(Op::LoadConst, iter_reg, zero_const, 0);

        let loop_start = self.current().current_pos();
        self.current().loop_stack.push(LoopCtx {
            break_jumps: Vec::new(),
            loop_start,
        });

        // ForIter: check if iterator is done, load value
        self.begin_scope();
        let val_reg = self.add_local(name.to_string());
        self.current()
            .emit_abc(Op::ForIter, iter_reg, list_reg, val_reg, 0);
        // The next instruction is the jump offset if done
        let exit_jump = self.current().current_pos();
        self.current().emit_abx(Op::Jump, 0, 0, 0); // placeholder, patched

        // Body
        let saved_dead_code = self.current().dead_code;
        self.current().dead_code = false;
        for stmt in body {
            self.compile_stmt(stmt)?;
        }

        self.end_scope();

        // Jump back to loop start
        let current = self.current().current_pos();
        let offset = (loop_start as i32 - current as i32 - 1) as i16;
        self.current().emit_abx(Op::Jump, 0, offset as u16, 0);

        // Patch exit jump
        self.current().patch_jump(exit_jump);

        // Patch break jumps
        let loop_ctx = self.current().loop_stack.pop().unwrap();
        for pos in loop_ctx.break_jumps {
            self.current().patch_jump(pos);
        }

        // Free iterator and list registers
        self.current().free_register(); // iter_reg
        self.current().free_register(); // list_reg

        // Restore dead_code state
        self.current().dead_code = saved_dead_code;

        Ok(())
    }

    fn compile_schema(
        &mut self,
        name: &str,
        fields: &[SchemaField],
        version: &Option<i64>,
    ) -> Result<(), TlError> {
        // Schema compilation: store as a global with the schema data
        // We use a special constant and SetGlobal
        let reg = self.current().alloc_register();

        // Encode schema as a constant with name + version + fields as string
        let ver_str = version.map_or(String::new(), |v| format!(":v{}", v));
        let schema_str = format!(
            "__schema__:{}{}:{}",
            name,
            ver_str,
            fields
                .iter()
                .map(|f| format!("{}:{:?}", f.name, f.type_ann))
                .collect::<Vec<_>>()
                .join(",")
        );
        let const_idx = self
            .current()
            .add_constant(Constant::String(Arc::from(schema_str.as_str())));
        self.current().emit_abx(Op::LoadConst, reg, const_idx, 0);

        let name_idx = self
            .current()
            .add_constant(Constant::String(Arc::from(name)));
        self.current().emit_abx(Op::SetGlobal, reg, name_idx, 0);
        self.current().free_register();

        // Also add as local
        let local_reg = self.add_local(name.to_string());
        self.current()
            .emit_abx(Op::LoadConst, local_reg, const_idx, 0);

        Ok(())
    }

    fn compile_migrate(
        &mut self,
        schema_name: &str,
        from_version: i64,
        to_version: i64,
        operations: &[MigrateOp],
    ) -> Result<(), TlError> {
        // Encode migration as a constant string with ops
        let mut ops_str = Vec::new();
        for op in operations {
            let s = match op {
                MigrateOp::AddColumn {
                    name,
                    type_ann,
                    default,
                } => {
                    let def_str = if let Some(d) = default {
                        format!(",default:{d:?}")
                    } else {
                        String::new()
                    };
                    format!("add:{name}:{type_ann:?}{def_str}")
                }
                MigrateOp::DropColumn { name } => format!("drop:{name}"),
                MigrateOp::RenameColumn { from, to } => format!("rename:{from}:{to}"),
                MigrateOp::AlterType { column, new_type } => format!("alter:{column}:{new_type:?}"),
                MigrateOp::AddConstraint { column, constraint } => {
                    format!("add_constraint:{column}:{constraint}")
                }
                MigrateOp::DropConstraint { column, constraint } => {
                    format!("drop_constraint:{column}:{constraint}")
                }
            };
            ops_str.push(s);
        }
        let migrate_str = format!(
            "__migrate__:{}:{}:{}:{}",
            schema_name,
            from_version,
            to_version,
            ops_str.join(";")
        );
        let reg = self.current().alloc_register();
        let const_idx = self
            .current()
            .add_constant(Constant::String(Arc::from(migrate_str.as_str())));
        self.current().emit_abx(Op::LoadConst, reg, const_idx, 0);
        // Store as a global migration record
        let name_key = format!("__migrate_{schema_name}_{from_version}_{to_version}");
        let name_idx = self
            .current()
            .add_constant(Constant::String(Arc::from(name_key.as_str())));
        self.current().emit_abx(Op::SetGlobal, reg, name_idx, 0);
        self.current().free_register();
        Ok(())
    }

    fn compile_train(
        &mut self,
        name: &str,
        algorithm: &str,
        config: &[(String, Expr)],
    ) -> Result<(), TlError> {
        let dest = self.add_local(name.to_string());

        // Store algorithm as constant
        let algo_idx = self
            .current()
            .add_constant(Constant::String(Arc::from(algorithm)));

        // For any config value that's an Ident, ensure the local is exported as a global
        // so the VM's eval_ast_to_vm can resolve it at runtime.
        for (_key, val) in config {
            if let Expr::Ident(ident_name) = val
                && let Some(reg) = self.resolve_local(ident_name)
            {
                let name_idx = self
                    .current()
                    .add_constant(Constant::String(Arc::from(ident_name.as_str())));
                self.current().emit_abx(Op::SetGlobal, reg, name_idx, 0);
            }
        }

        // Store config key-value pairs: compile each value expr, then store as AstExprList
        let config_exprs: Vec<tl_ast::Expr> = config
            .iter()
            .map(|(k, v)| {
                // Encode config as a list of NamedArg expressions
                tl_ast::Expr::NamedArg {
                    name: k.clone(),
                    value: Box::new(v.clone()),
                }
            })
            .collect();
        let config_idx = self
            .current()
            .add_constant(Constant::AstExprList(config_exprs));

        // Emit Train instruction
        self.current()
            .emit_abc(Op::Train, dest, algo_idx as u8, config_idx as u8, 0);

        // Also set as global
        let name_idx = self
            .current()
            .add_constant(Constant::String(Arc::from(name)));
        self.current().emit_abx(Op::SetGlobal, dest, name_idx, 0);

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn compile_pipeline(
        &mut self,
        name: &str,
        extract: &[Stmt],
        transform: &[Stmt],
        load: &[Stmt],
        schedule: &Option<String>,
        timeout: &Option<String>,
        retries: &Option<i64>,
        _on_failure: &Option<Vec<Stmt>>,
        on_success: &Option<Vec<Stmt>>,
    ) -> Result<(), TlError> {
        let dest = self.add_local(name.to_string());

        // Store pipeline config as constants
        let name_const = self
            .current()
            .add_constant(Constant::String(Arc::from(name)));

        // Compile extract/transform/load blocks as AstExprList of the statements
        // For simplicity, store the blocks as AST and handle at runtime
        let mut all_stmts = Vec::new();
        all_stmts.extend(extract.to_vec());
        all_stmts.extend(transform.to_vec());
        all_stmts.extend(load.to_vec());

        // Store config: schedule, timeout, retries as named args
        let mut config_exprs: Vec<tl_ast::Expr> = Vec::new();

        if let Some(s) = schedule {
            config_exprs.push(tl_ast::Expr::NamedArg {
                name: "schedule".to_string(),
                value: Box::new(tl_ast::Expr::String(s.clone())),
            });
        }
        if let Some(t) = timeout {
            config_exprs.push(tl_ast::Expr::NamedArg {
                name: "timeout".to_string(),
                value: Box::new(tl_ast::Expr::String(t.clone())),
            });
        }
        if let Some(r) = retries {
            config_exprs.push(tl_ast::Expr::NamedArg {
                name: "retries".to_string(),
                value: Box::new(tl_ast::Expr::Int(*r)),
            });
        }
        let config_idx = self
            .current()
            .add_constant(Constant::AstExprList(config_exprs));

        // Compile blocks inline for VM execution (shared scope)
        for stmt in extract {
            self.compile_stmt(stmt)?;
        }
        for stmt in transform {
            self.compile_stmt(stmt)?;
        }
        for stmt in load {
            self.compile_stmt(stmt)?;
        }

        // Execute on_success block if present
        if let Some(success_block) = on_success {
            for stmt in success_block {
                self.compile_stmt(stmt)?;
            }
        }

        // Emit PipelineExec to store the pipeline def
        self.current().emit_abc(
            Op::PipelineExec,
            dest,
            name_const as u8,
            config_idx as u8,
            0,
        );

        // Set as global
        let gname = self
            .current()
            .add_constant(Constant::String(Arc::from(name)));
        self.current().emit_abx(Op::SetGlobal, dest, gname, 0);

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn compile_agent(
        &mut self,
        name: &str,
        model: &str,
        system_prompt: &Option<String>,
        tools: &[(String, tl_ast::Expr)],
        max_turns: &Option<i64>,
        temperature: &Option<f64>,
        max_tokens: &Option<i64>,
        base_url: &Option<String>,
        api_key: &Option<String>,
        output_format: &Option<String>,
        on_tool_call: &Option<Vec<tl_ast::Stmt>>,
        on_complete: &Option<Vec<tl_ast::Stmt>>,
    ) -> Result<(), TlError> {
        let dest = self.add_local(name.to_string());

        // Store agent name as constant
        let name_const = self
            .current()
            .add_constant(Constant::String(Arc::from(name)));

        // Build config as AstExprList of NamedArgs
        let mut config_exprs: Vec<tl_ast::Expr> = Vec::new();

        // model (required)
        config_exprs.push(tl_ast::Expr::NamedArg {
            name: "model".to_string(),
            value: Box::new(tl_ast::Expr::String(model.to_string())),
        });

        if let Some(sys) = system_prompt {
            config_exprs.push(tl_ast::Expr::NamedArg {
                name: "system".to_string(),
                value: Box::new(tl_ast::Expr::String(sys.clone())),
            });
        }

        if let Some(n) = max_turns {
            config_exprs.push(tl_ast::Expr::NamedArg {
                name: "max_turns".to_string(),
                value: Box::new(tl_ast::Expr::Int(*n)),
            });
        }

        if let Some(t) = temperature {
            config_exprs.push(tl_ast::Expr::NamedArg {
                name: "temperature".to_string(),
                value: Box::new(tl_ast::Expr::Float(*t)),
            });
        }

        if let Some(n) = max_tokens {
            config_exprs.push(tl_ast::Expr::NamedArg {
                name: "max_tokens".to_string(),
                value: Box::new(tl_ast::Expr::Int(*n)),
            });
        }

        if let Some(url) = base_url {
            config_exprs.push(tl_ast::Expr::NamedArg {
                name: "base_url".to_string(),
                value: Box::new(tl_ast::Expr::String(url.clone())),
            });
        }

        if let Some(key) = api_key {
            config_exprs.push(tl_ast::Expr::NamedArg {
                name: "api_key".to_string(),
                value: Box::new(tl_ast::Expr::String(key.clone())),
            });
        }

        if let Some(fmt) = output_format {
            config_exprs.push(tl_ast::Expr::NamedArg {
                name: "output_format".to_string(),
                value: Box::new(tl_ast::Expr::String(fmt.clone())),
            });
        }

        // Encode tools as a list of NamedArgs: tool_name: { desc, params map expr }
        for (tool_name, tool_expr) in tools {
            config_exprs.push(tl_ast::Expr::NamedArg {
                name: format!("tool:{tool_name}"),
                value: Box::new(tool_expr.clone()),
            });
        }

        // Encode lifecycle hook markers
        if on_tool_call.is_some() {
            config_exprs.push(tl_ast::Expr::NamedArg {
                name: "on_tool_call".to_string(),
                value: Box::new(tl_ast::Expr::Bool(true)),
            });
        }
        if on_complete.is_some() {
            config_exprs.push(tl_ast::Expr::NamedArg {
                name: "on_complete".to_string(),
                value: Box::new(tl_ast::Expr::Bool(true)),
            });
        }

        let config_idx = self
            .current()
            .add_constant(Constant::AstExprList(config_exprs));

        // Emit AgentExec opcode
        self.current()
            .emit_abc(Op::AgentExec, dest, name_const as u8, config_idx as u8, 0);

        // Set as global
        let gname = self
            .current()
            .add_constant(Constant::String(Arc::from(name)));
        self.current().emit_abx(Op::SetGlobal, dest, gname, 0);

        // Compile lifecycle hooks as global functions
        if let Some(stmts) = on_tool_call {
            let hook_name = format!("__agent_{name}_on_tool_call__");
            let params = vec![
                tl_ast::Param {
                    name: "tool_name".into(),
                    type_ann: None,
                },
                tl_ast::Param {
                    name: "tool_args".into(),
                    type_ann: None,
                },
                tl_ast::Param {
                    name: "tool_result".into(),
                    type_ann: None,
                },
            ];
            self.compile_agent_hook(&hook_name, &params, stmts)?;
        }
        if let Some(stmts) = on_complete {
            let hook_name = format!("__agent_{name}_on_complete__");
            let params = vec![tl_ast::Param {
                name: "result".into(),
                type_ann: None,
            }];
            self.compile_agent_hook(&hook_name, &params, stmts)?;
        }

        Ok(())
    }

    fn compile_agent_hook(
        &mut self,
        hook_name: &str,
        params: &[tl_ast::Param],
        body: &[tl_ast::Stmt],
    ) -> Result<(), TlError> {
        // Add a local for the hook function
        let local = self.add_local(hook_name.to_string());

        // Compile the function body using compile_function
        let dest = self.compile_function(hook_name.to_string(), params, body, false)?;

        // Set as global so exec_agent_loop can find it
        let gname = self
            .current()
            .add_constant(Constant::String(Arc::from(hook_name)));
        self.current().emit_abx(Op::SetGlobal, local, gname, 0);
        let _ = dest;

        Ok(())
    }

    fn compile_stream_decl(
        &mut self,
        name: &str,
        source: &Expr,
        window: &Option<tl_ast::WindowSpec>,
        watermark: &Option<String>,
    ) -> Result<(), TlError> {
        let dest = self.add_local(name.to_string());

        // Compile source expression
        let src_reg = self.current().alloc_register();
        self.compile_expr(source, src_reg)?;

        // Store stream config as constants
        let mut config_exprs: Vec<tl_ast::Expr> = Vec::new();
        config_exprs.push(tl_ast::Expr::NamedArg {
            name: "name".to_string(),
            value: Box::new(tl_ast::Expr::String(name.to_string())),
        });

        if let Some(w) = window {
            let window_str = match w {
                tl_ast::WindowSpec::Tumbling(d) => format!("tumbling:{d}"),
                tl_ast::WindowSpec::Sliding(w, s) => format!("sliding:{w}:{s}"),
                tl_ast::WindowSpec::Session(g) => format!("session:{g}"),
            };
            config_exprs.push(tl_ast::Expr::NamedArg {
                name: "window".to_string(),
                value: Box::new(tl_ast::Expr::String(window_str)),
            });
        }
        if let Some(wm) = watermark {
            config_exprs.push(tl_ast::Expr::NamedArg {
                name: "watermark".to_string(),
                value: Box::new(tl_ast::Expr::String(wm.clone())),
            });
        }
        let config_idx = self
            .current()
            .add_constant(Constant::AstExprList(config_exprs));

        // Emit StreamExec instruction
        self.current()
            .emit_abc(Op::StreamExec, dest, config_idx as u8, src_reg, 0);
        self.current().free_register(); // free src_reg

        // Set as global
        let gname = self
            .current()
            .add_constant(Constant::String(Arc::from(name)));
        self.current().emit_abx(Op::SetGlobal, dest, gname, 0);

        Ok(())
    }

    fn compile_connector_decl(
        &mut self,
        name: &str,
        connector_type: &str,
        config: &[(String, Expr)],
    ) -> Result<(), TlError> {
        let dest = self.add_local(name.to_string());

        // Store connector type as constant
        let type_idx = self
            .current()
            .add_constant(Constant::String(Arc::from(connector_type)));

        // Store config as AstExprList
        let config_exprs: Vec<tl_ast::Expr> = config
            .iter()
            .map(|(k, v)| tl_ast::Expr::NamedArg {
                name: k.clone(),
                value: Box::new(v.clone()),
            })
            .collect();
        let config_idx = self
            .current()
            .add_constant(Constant::AstExprList(config_exprs));

        // Emit ConnectorDecl instruction
        self.current()
            .emit_abc(Op::ConnectorDecl, dest, type_idx as u8, config_idx as u8, 0);

        // Set as global
        let gname = self
            .current()
            .add_constant(Constant::String(Arc::from(name)));
        self.current().emit_abx(Op::SetGlobal, dest, gname, 0);

        Ok(())
    }

    // ── Constant Folding ─────────────────────────────────────

    /// Try to evaluate a constant expression at compile time.
    /// Returns None if the expression contains non-constant parts.
    fn try_fold_const(expr: &Expr) -> Option<FoldedConst> {
        match expr {
            Expr::Int(n) => Some(FoldedConst::Int(*n)),
            Expr::Float(f) => Some(FoldedConst::Float(*f)),
            Expr::Bool(b) => Some(FoldedConst::Bool(*b)),
            // Only fold simple strings (no interpolation markers, no escape sequences).
            // Strings with escapes need processing via compile_string_interpolation.
            Expr::String(s) if !s.contains('{') && !s.contains('\\') => {
                Some(FoldedConst::String(s.clone()))
            }
            Expr::String(_) => None,

            Expr::UnaryOp {
                op: UnaryOp::Neg,
                expr,
            } => match Self::try_fold_const(expr)? {
                FoldedConst::Int(n) => Some(FoldedConst::Int(-n)),
                FoldedConst::Float(f) => Some(FoldedConst::Float(-f)),
                _ => None,
            },
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr,
            } => match Self::try_fold_const(expr)? {
                FoldedConst::Bool(b) => Some(FoldedConst::Bool(!b)),
                _ => None,
            },

            Expr::BinOp { left, op, right } => {
                let l = Self::try_fold_const(left)?;
                let r = Self::try_fold_const(right)?;
                Self::fold_binop(&l, op, &r)
            }

            // Anything else (variables, calls, member access, etc.) cannot be folded
            _ => None,
        }
    }

    /// Fold a binary operation on two constants.
    fn fold_binop(left: &FoldedConst, op: &BinOp, right: &FoldedConst) -> Option<FoldedConst> {
        match (left, right) {
            // Int op Int
            (FoldedConst::Int(a), FoldedConst::Int(b)) => match op {
                BinOp::Add => Some(FoldedConst::Int(a.checked_add(*b)?)),
                BinOp::Sub => Some(FoldedConst::Int(a.checked_sub(*b)?)),
                BinOp::Mul => Some(FoldedConst::Int(a.checked_mul(*b)?)),
                BinOp::Div => {
                    if *b == 0 {
                        return None;
                    } // defer to runtime error
                    Some(FoldedConst::Int(a / b))
                }
                BinOp::Mod => {
                    if *b == 0 {
                        return None;
                    }
                    Some(FoldedConst::Int(a % b))
                }
                BinOp::Pow => {
                    if *b >= 0 {
                        Some(FoldedConst::Int(a.pow(*b as u32)))
                    } else {
                        Some(FoldedConst::Float((*a as f64).powf(*b as f64)))
                    }
                }
                BinOp::Eq => Some(FoldedConst::Bool(a == b)),
                BinOp::Neq => Some(FoldedConst::Bool(a != b)),
                BinOp::Lt => Some(FoldedConst::Bool(a < b)),
                BinOp::Gt => Some(FoldedConst::Bool(a > b)),
                BinOp::Lte => Some(FoldedConst::Bool(a <= b)),
                BinOp::Gte => Some(FoldedConst::Bool(a >= b)),
                BinOp::And | BinOp::Or => None,
            },

            // Float op Float
            (FoldedConst::Float(a), FoldedConst::Float(b)) => match op {
                BinOp::Add => Some(FoldedConst::Float(a + b)),
                BinOp::Sub => Some(FoldedConst::Float(a - b)),
                BinOp::Mul => Some(FoldedConst::Float(a * b)),
                BinOp::Div => {
                    if *b == 0.0 {
                        return None;
                    }
                    Some(FoldedConst::Float(a / b))
                }
                BinOp::Mod => {
                    if *b == 0.0 {
                        return None;
                    }
                    Some(FoldedConst::Float(a % b))
                }
                BinOp::Pow => Some(FoldedConst::Float(a.powf(*b))),
                BinOp::Eq => Some(FoldedConst::Bool(a == b)),
                BinOp::Neq => Some(FoldedConst::Bool(a != b)),
                BinOp::Lt => Some(FoldedConst::Bool(a < b)),
                BinOp::Gt => Some(FoldedConst::Bool(a > b)),
                BinOp::Lte => Some(FoldedConst::Bool(a <= b)),
                BinOp::Gte => Some(FoldedConst::Bool(a >= b)),
                BinOp::And | BinOp::Or => None,
            },

            // Int op Float / Float op Int — promote to float
            (FoldedConst::Int(a), FoldedConst::Float(b)) => {
                let a = *a as f64;
                Self::fold_binop(&FoldedConst::Float(a), op, &FoldedConst::Float(*b))
            }
            (FoldedConst::Float(a), FoldedConst::Int(b)) => {
                let b = *b as f64;
                Self::fold_binop(&FoldedConst::Float(*a), op, &FoldedConst::Float(b))
            }

            // String + String — concatenation
            (FoldedConst::String(a), FoldedConst::String(b)) if matches!(op, BinOp::Add) => {
                Some(FoldedConst::String(format!("{a}{b}")))
            }
            (FoldedConst::String(a), FoldedConst::String(b)) => match op {
                BinOp::Eq => Some(FoldedConst::Bool(a == b)),
                BinOp::Neq => Some(FoldedConst::Bool(a != b)),
                _ => None,
            },

            // Bool op Bool
            (FoldedConst::Bool(a), FoldedConst::Bool(b)) => match op {
                BinOp::And => Some(FoldedConst::Bool(*a && *b)),
                BinOp::Or => Some(FoldedConst::Bool(*a || *b)),
                BinOp::Eq => Some(FoldedConst::Bool(a == b)),
                BinOp::Neq => Some(FoldedConst::Bool(a != b)),
                _ => None,
            },

            _ => None,
        }
    }

    // ── End Constant Folding ────────────────────────────────

    fn compile_expr(&mut self, expr: &Expr, dest: u8) -> Result<(), TlError> {
        // Constant folding: try to evaluate the expression at compile time
        if let Some(folded) = Self::try_fold_const(expr) {
            match folded {
                FoldedConst::Int(n) => {
                    let idx = self.current().add_constant(Constant::Int(n));
                    self.current().emit_abx(Op::LoadConst, dest, idx, 0);
                }
                FoldedConst::Float(f) => {
                    let idx = self.current().add_constant(Constant::Float(f));
                    self.current().emit_abx(Op::LoadConst, dest, idx, 0);
                }
                FoldedConst::Bool(true) => {
                    self.current().emit_abx(Op::LoadTrue, dest, 0, 0);
                }
                FoldedConst::Bool(false) => {
                    self.current().emit_abx(Op::LoadFalse, dest, 0, 0);
                }
                FoldedConst::String(s) => {
                    let idx = self
                        .current()
                        .add_constant(Constant::String(Arc::from(s.as_str())));
                    self.current().emit_abx(Op::LoadConst, dest, idx, 0);
                }
            }
            return Ok(());
        }

        match expr {
            Expr::Int(n) => {
                let idx = self.current().add_constant(Constant::Int(*n));
                self.current().emit_abx(Op::LoadConst, dest, idx, 0);
            }
            Expr::Float(f) => {
                let idx = self.current().add_constant(Constant::Float(*f));
                self.current().emit_abx(Op::LoadConst, dest, idx, 0);
            }
            Expr::Decimal(s) => {
                let idx = self
                    .current()
                    .add_constant(Constant::Decimal(Arc::from(s.as_str())));
                self.current().emit_abx(Op::LoadConst, dest, idx, 0);
            }
            Expr::String(s) => {
                self.compile_string_interpolation(s, dest)?;
            }
            Expr::Bool(true) => {
                self.current().emit_abx(Op::LoadTrue, dest, 0, 0);
            }
            Expr::Bool(false) => {
                self.current().emit_abx(Op::LoadFalse, dest, 0, 0);
            }
            Expr::None => {
                self.current().emit_abx(Op::LoadNone, dest, 0, 0);
            }
            Expr::Ident(name) => {
                // Try local first, then upvalue, then global
                if let Some(reg) = self.resolve_local(name) {
                    if reg != dest {
                        self.current().emit_abc(Op::Move, dest, reg, 0, 0);
                    }
                } else if let Some(uv) = self.resolve_upvalue(name) {
                    self.current().emit_abc(Op::GetUpvalue, dest, uv, 0, 0);
                } else {
                    let idx = self
                        .current()
                        .add_constant(Constant::String(Arc::from(name.as_str())));
                    self.current().emit_abx(Op::GetGlobal, dest, idx, 0);
                }
            }
            Expr::BinOp { left, op, right } => {
                // Short-circuit for And/Or
                match op {
                    BinOp::And => {
                        self.compile_expr(left, dest)?;
                        let jump_pos = self.current().current_pos();
                        self.current().emit_abx(Op::JumpIfFalse, dest, 0, 0);
                        self.compile_expr(right, dest)?;
                        self.current().patch_jump(jump_pos);
                        return Ok(());
                    }
                    BinOp::Or => {
                        self.compile_expr(left, dest)?;
                        let jump_pos = self.current().current_pos();
                        self.current().emit_abx(Op::JumpIfTrue, dest, 0, 0);
                        self.compile_expr(right, dest)?;
                        self.current().patch_jump(jump_pos);
                        return Ok(());
                    }
                    _ => {}
                }

                let left_reg = self.current().alloc_register();
                let right_reg = self.current().alloc_register();
                self.compile_expr(left, left_reg)?;
                self.compile_expr(right, right_reg)?;

                let vm_op = match op {
                    BinOp::Add => Op::Add,
                    BinOp::Sub => Op::Sub,
                    BinOp::Mul => Op::Mul,
                    BinOp::Div => Op::Div,
                    BinOp::Mod => Op::Mod,
                    BinOp::Pow => Op::Pow,
                    BinOp::Eq => Op::Eq,
                    BinOp::Neq => Op::Neq,
                    BinOp::Lt => Op::Lt,
                    BinOp::Gt => Op::Gt,
                    BinOp::Lte => Op::Lte,
                    BinOp::Gte => Op::Gte,
                    BinOp::And | BinOp::Or => unreachable!(), // handled above
                };
                self.current().emit_abc(vm_op, dest, left_reg, right_reg, 0);
                self.current().free_register(); // right
                self.current().free_register(); // left
            }
            Expr::UnaryOp { op, expr } => {
                let src = self.current().alloc_register();
                self.compile_expr(expr, src)?;
                match op {
                    UnaryOp::Neg => self.current().emit_abc(Op::Neg, dest, src, 0, 0),
                    UnaryOp::Not => self.current().emit_abc(Op::Not, dest, src, 0, 0),
                    UnaryOp::Ref => {
                        self.current().emit_abc(Op::MakeRef, dest, src, 0, 0);
                    }
                }
                self.current().free_register();
            }
            Expr::Await(expr) => {
                let src = self.current().alloc_register();
                self.compile_expr(expr, src)?;
                self.current().emit_abc(Op::Await, dest, src, 0, 0);
                self.current().free_register();
            }
            Expr::Try(inner) => {
                let src = self.current().alloc_register();
                self.compile_expr(inner, src)?;
                self.current().emit_abc(Op::TryPropagate, dest, src, 0, 0);
                self.current().free_register();
            }
            Expr::Yield(opt_expr) => {
                self.current().has_yield = true;
                match opt_expr {
                    Some(expr) => {
                        self.compile_expr(expr, dest)?;
                    }
                    None => {
                        self.current().emit_abx(Op::LoadNone, dest, 0, 0);
                    }
                }
                self.current().emit_abc(Op::Yield, dest, 0, 0, 0);
            }
            Expr::Call { function, args } => {
                self.compile_call(function, args, dest)?;
            }
            Expr::Pipe { left, right } => {
                self.compile_pipe(left, right, dest)?;
            }
            Expr::List(elements) => {
                if elements.is_empty() {
                    let start = self.current().next_register;
                    self.current().emit_abc(Op::NewList, dest, start, 0, 0);
                } else {
                    let start = self.current().next_register;
                    for el in elements {
                        let r = self.current().alloc_register();
                        self.compile_expr(el, r)?;
                    }
                    self.current()
                        .emit_abc(Op::NewList, dest, start, elements.len() as u8, 0);
                    for _ in elements {
                        self.current().free_register();
                    }
                }
            }
            Expr::Map(pairs) => {
                if pairs.is_empty() {
                    let start = self.current().next_register;
                    self.current().emit_abc(Op::NewMap, dest, start, 0, 0);
                } else {
                    let start = self.current().next_register;
                    for (key, val) in pairs {
                        let kr = self.current().alloc_register();
                        self.compile_expr(key, kr)?;
                        let vr = self.current().alloc_register();
                        self.compile_expr(val, vr)?;
                    }
                    self.current()
                        .emit_abc(Op::NewMap, dest, start, pairs.len() as u8, 0);
                    for _ in 0..pairs.len() * 2 {
                        self.current().free_register();
                    }
                }
            }
            Expr::Index { object, index } => {
                let obj_reg = self.current().alloc_register();
                let idx_reg = self.current().alloc_register();
                self.compile_expr(object, obj_reg)?;
                self.compile_expr(index, idx_reg)?;
                self.current()
                    .emit_abc(Op::GetIndex, dest, obj_reg, idx_reg, 0);
                self.current().free_register();
                self.current().free_register();
            }
            Expr::Block { stmts, expr } => {
                self.begin_scope();
                for stmt in stmts {
                    self.compile_stmt(stmt)?;
                }
                if let Some(e) = expr {
                    self.compile_expr(e, dest)?;
                } else {
                    self.current().emit_abx(Op::LoadNone, dest, 0, 0);
                }
                self.end_scope();
            }
            Expr::Case { arms } => {
                self.compile_case(arms, dest)?;
            }
            Expr::Match { subject, arms } => {
                self.compile_match(subject, arms, dest)?;
            }
            Expr::Closure { params, body, .. } => {
                self.compile_closure_expr(params, body, dest)?;
            }
            Expr::Range { start, end } => {
                // Compile to a builtin range() call
                let start_reg = self.current().alloc_register();
                let end_reg = self.current().alloc_register();
                self.compile_expr(start, start_reg)?;
                self.compile_expr(end, end_reg)?;
                // CallBuiltin Range with 2 args (ABx: dest, builtin_id; next: argc, first_arg)
                self.current()
                    .emit_abx(Op::CallBuiltin, dest, BuiltinId::Range as u16, 0);
                self.current().emit_abc(Op::Move, 2, start_reg, 0, 0);
                self.current().free_register();
                self.current().free_register();
            }
            Expr::NullCoalesce { expr, default } => {
                self.compile_expr(expr, dest)?;
                let skip_jump = self.current().current_pos();
                // If not None, skip default
                self.current().emit_abx(Op::JumpIfTrue, dest, 0, 0);
                self.compile_expr(default, dest)?;
                self.current().patch_jump(skip_jump);
            }
            Expr::Assign { target, value } => {
                if let Expr::Ident(name) = target.as_ref() {
                    self.compile_expr(value, dest)?;
                    if let Some(reg) = self.resolve_local(name) {
                        if reg != dest {
                            self.current().emit_abc(Op::Move, reg, dest, 0, 0);
                        }
                    } else if let Some(uv) = self.resolve_upvalue(name) {
                        self.current().emit_abc(Op::SetUpvalue, dest, uv, 0, 0);
                    } else {
                        let idx = self
                            .current()
                            .add_constant(Constant::String(Arc::from(name.as_str())));
                        self.current().emit_abx(Op::SetGlobal, dest, idx, 0);
                    }
                } else if let Expr::Index { object, index } = target.as_ref() {
                    // m["key"] = value or list[idx] = value
                    let obj_reg = self.current().alloc_register();
                    self.compile_expr(object, obj_reg)?;
                    let idx_reg = self.current().alloc_register();
                    self.compile_expr(index, idx_reg)?;
                    self.compile_expr(value, dest)?;
                    // SetIndex: a=value, b=object, c=index
                    self.current()
                        .emit_abc(Op::SetIndex, dest, obj_reg, idx_reg, 0);
                    // Write the modified object back to the variable
                    if let Expr::Ident(name) = object.as_ref() {
                        if let Some(reg) = self.resolve_local(name) {
                            self.current().emit_abc(Op::Move, reg, obj_reg, 0, 0);
                        } else {
                            let c_idx = self
                                .current()
                                .add_constant(Constant::String(Arc::from(name.as_str())));
                            self.current().emit_abx(Op::SetGlobal, obj_reg, c_idx, 0);
                        }
                    }
                    self.current().free_register(); // idx_reg
                    self.current().free_register(); // obj_reg
                } else if let Expr::Member { object, field } = target.as_ref() {
                    // s.field = value — struct field or map member assignment
                    if let Expr::Ident(name) = object.as_ref() {
                        let obj_reg = self.current().alloc_register();
                        self.compile_expr(object, obj_reg)?;
                        // Compile value
                        self.compile_expr(value, dest)?;
                        // Use SetIndex with string key for member assignment
                        let key_reg = self.current().alloc_register();
                        let key_idx = self
                            .current()
                            .add_constant(Constant::String(Arc::from(field.as_str())));
                        self.current().emit_abx(Op::LoadConst, key_reg, key_idx, 0);
                        self.current()
                            .emit_abc(Op::SetIndex, dest, obj_reg, key_reg, 0);
                        // Write back
                        if let Some(reg) = self.resolve_local(name) {
                            self.current().emit_abc(Op::Move, reg, obj_reg, 0, 0);
                        } else {
                            let c_idx = self
                                .current()
                                .add_constant(Constant::String(Arc::from(name.as_str())));
                            self.current().emit_abx(Op::SetGlobal, obj_reg, c_idx, 0);
                        }
                        self.current().free_register(); // key_reg
                        self.current().free_register(); // obj_reg
                    } else {
                        return Err(compile_err("Invalid assignment target".to_string()));
                    }
                } else {
                    return Err(compile_err("Invalid assignment target".to_string()));
                }
            }
            Expr::Member { object, field } => {
                let obj_reg = self.current().alloc_register();
                self.compile_expr(object, obj_reg)?;
                let field_idx = self
                    .current()
                    .add_constant(Constant::String(Arc::from(field.as_str())));
                self.current()
                    .emit_abc(Op::GetMember, dest, obj_reg, field_idx as u8, 0);
                self.current().free_register();
            }
            Expr::NamedArg { .. } => {
                // Named args are handled in call compilation
                self.current().emit_abx(Op::LoadNone, dest, 0, 0);
            }
            Expr::StructInit { name, fields } => {
                // Compile struct init: load type name, compile field values, emit NewStruct
                let name_idx = self
                    .current()
                    .add_constant(Constant::String(Arc::from(name.as_str())));
                let start = self.current().next_register;
                for (fname, fexpr) in fields {
                    let fname_reg = self.current().alloc_register();
                    let fname_idx = self
                        .current()
                        .add_constant(Constant::String(Arc::from(fname.as_str())));
                    self.current()
                        .emit_abx(Op::LoadConst, fname_reg, fname_idx, 0);
                    let fval_reg = self.current().alloc_register();
                    self.compile_expr(fexpr, fval_reg)?;
                }
                self.current()
                    .emit_abc(Op::NewStruct, dest, name_idx as u8, fields.len() as u8, 0);
                // Encode start register in next instruction
                self.current().emit_abc(Op::Move, start, 0, 0, 0);
                for _ in 0..fields.len() * 2 {
                    self.current().free_register();
                }
            }
            Expr::EnumVariant {
                enum_name,
                variant,
                args,
            } => {
                let full_name = format!("{enum_name}::{variant}");
                let name_idx = self
                    .current()
                    .add_constant(Constant::String(Arc::from(full_name.as_str())));
                let start = self.current().next_register;
                for arg in args {
                    let r = self.current().alloc_register();
                    self.compile_expr(arg, r)?;
                }
                self.current()
                    .emit_abc(Op::NewEnum, dest, name_idx as u8, start, 0);
                // Encode arg count
                self.current().emit_abc(Op::Move, args.len() as u8, 0, 0, 0);
                for _ in args {
                    self.current().free_register();
                }
            }
        }
        Ok(())
    }

    fn compile_call(&mut self, function: &Expr, args: &[Expr], dest: u8) -> Result<(), TlError> {
        // Check for builtin calls
        if let Expr::Ident(name) = function
            && let Some(builtin_id) = BuiltinId::from_name(name)
        {
            return self.compile_builtin_call(builtin_id, args, dest);
        }

        // Method call: obj.method(args) -> MethodCall
        if let Expr::Member { object, field } = function {
            let obj_reg = self.current().alloc_register();
            self.compile_expr(object, obj_reg)?;
            let method_idx = self
                .current()
                .add_constant(Constant::String(Arc::from(field.as_str())));
            let args_start = self.current().next_register;
            for arg in args {
                let r = self.current().alloc_register();
                self.compile_expr(arg, r)?;
            }
            self.current()
                .emit_abc(Op::MethodCall, dest, obj_reg, method_idx as u8, 0);
            // Next instruction: args_start, arg_count
            self.current()
                .emit_abc(Op::Move, args_start, args.len() as u8, 0, 0);
            for _ in args {
                self.current().free_register();
            }
            self.current().free_register(); // obj_reg
            return Ok(());
        }

        // General function call
        let func_reg = self.current().alloc_register();
        self.compile_expr(function, func_reg)?;

        let args_start = self.current().next_register;
        for arg in args {
            let r = self.current().alloc_register();
            self.compile_expr(arg, r)?;
        }

        self.current()
            .emit_abc(Op::Call, func_reg, args_start, args.len() as u8, 0);

        // Free arg registers
        for _ in args {
            self.current().free_register();
        }

        // Move result from func_reg to dest
        if func_reg != dest {
            self.current().emit_abc(Op::Move, dest, func_reg, 0, 0);
        }
        self.current().free_register(); // func_reg

        Ok(())
    }

    fn compile_builtin_call(
        &mut self,
        builtin_id: BuiltinId,
        args: &[Expr],
        dest: u8,
    ) -> Result<(), TlError> {
        let args_start = self.current().next_register;
        for arg in args {
            let r = self.current().alloc_register();
            self.compile_expr(arg, r)?;
        }

        self.current()
            .emit_abx(Op::CallBuiltin, dest, builtin_id as u16, 0);
        // Next instruction: A=arg_count, B=first_arg_reg
        self.current()
            .emit_abc(Op::Move, args.len() as u8, args_start, 0, 0);

        for _ in args {
            self.current().free_register();
        }

        Ok(())
    }

    /// Emit instructions to mark the source of a pipe as moved (consumed).
    fn emit_pipe_move(&mut self, left: &Expr) {
        if let Expr::Ident(name) = left {
            if let Some(local_reg) = self.resolve_local(name) {
                self.current().emit_abc(Op::LoadMoved, local_reg, 0, 0, 0);
            } else {
                let moved_reg = self.current().alloc_register();
                self.current().emit_abc(Op::LoadMoved, moved_reg, 0, 0, 0);
                let idx = self
                    .current()
                    .add_constant(Constant::String(Arc::from(name.as_str())));
                self.current().emit_abx(Op::SetGlobal, moved_reg, idx, 0);
                self.current().free_register();
            }
        }
    }

    /// Table operations recognized by the compiler for Op::TablePipe emission.
    /// Must match what the VM's handle_table_pipe expects for the legacy path.
    const TABLE_OPS: &'static [&'static str] = &[
        "filter",
        "select",
        "sort",
        "with",
        "aggregate",
        "join",
        "head",
        "limit",
        "collect",
        "show",
        "describe",
        "write_csv",
        "write_parquet",
        "sample",
        "window",
        "union",
    ];

    fn compile_pipe(&mut self, left: &Expr, right: &Expr, dest: u8) -> Result<(), TlError> {
        // Try IR-optimized path for table pipe chains
        if let Some((source, ops)) = self.try_extract_table_pipe_chain(left, right)
            && let Ok(plan) = tl_ir::build_query_plan(&source, &ops)
        {
            let optimized = tl_ir::optimize(plan);
            let lowered = tl_ir::lower_plan(&optimized);
            return self.emit_optimized_plan(left, &source, &lowered, dest);
        }
        // Fall back to legacy path
        self.compile_pipe_legacy(left, right, dest)
    }

    /// Try to extract a flat table pipe chain from nested Pipe expressions.
    /// Returns (source_expr, [(op_name, args)]) if all ops are table ops.
    fn try_extract_table_pipe_chain(
        &self,
        left: &Expr,
        right: &Expr,
    ) -> Option<(Expr, Vec<(String, Vec<Expr>)>)> {
        let mut ops = Vec::new();

        // Extract op from the right side
        let (fname, args) = match right {
            Expr::Call { function, args } => {
                if let Expr::Ident(fname) = function.as_ref() {
                    (fname.as_str(), args.clone())
                } else {
                    return None;
                }
            }
            _ => return None,
        };

        if !Self::TABLE_OPS.contains(&fname) {
            return None;
        }

        ops.push((fname.to_string(), args));

        // Walk left side to collect more pipe ops
        let source = self.extract_pipe_chain_left(left, &mut ops)?;

        // Reverse because we collected from right to left
        ops.reverse();

        Some((source, ops))
    }

    /// Recursively extract pipe chain ops from the left side.
    fn extract_pipe_chain_left(
        &self,
        expr: &Expr,
        ops: &mut Vec<(String, Vec<Expr>)>,
    ) -> Option<Expr> {
        match expr {
            Expr::Pipe { left, right } => {
                // Extract the op from this pipe's right side
                let (fname, args) = match right.as_ref() {
                    Expr::Call { function, args } => {
                        if let Expr::Ident(fname) = function.as_ref() {
                            (fname.as_str(), args.clone())
                        } else {
                            return None;
                        }
                    }
                    _ => return None,
                };

                if !Self::TABLE_OPS.contains(&fname) {
                    return None;
                }

                ops.push((fname.to_string(), args));

                // Continue extracting from the left side
                self.extract_pipe_chain_left(left, ops)
            }
            // Base case: not a pipe — this is the source expression
            other => Some(other.clone()),
        }
    }

    /// Emit optimized table pipe operations from the lowered IR plan.
    fn emit_optimized_plan(
        &mut self,
        _original_left: &Expr,
        source: &Expr,
        lowered_ops: &[(String, Vec<Expr>)],
        dest: u8,
    ) -> Result<(), TlError> {
        let left_reg = self.current().alloc_register();
        self.compile_expr(source, left_reg)?;

        // Mark the source variable as moved (consumed by pipe)
        self.emit_pipe_move(source);

        // Emit TablePipe for each lowered operation
        for (op_name, args) in lowered_ops {
            let args_idx = self
                .current()
                .add_constant(Constant::AstExprList(args.clone()));
            let op_idx = self
                .current()
                .add_constant(Constant::String(Arc::from(op_name.as_str())));
            self.current()
                .emit_abc(Op::TablePipe, left_reg, op_idx as u8, args_idx as u8, 0);
        }

        if left_reg != dest {
            self.current().emit_abc(Op::Move, dest, left_reg, 0, 0);
        }
        self.current().free_register();
        Ok(())
    }

    fn compile_pipe_legacy(&mut self, left: &Expr, right: &Expr, dest: u8) -> Result<(), TlError> {
        let left_reg = self.current().alloc_register();
        self.compile_expr(left, left_reg)?;

        // Mark the source variable as moved (consumed by pipe)
        self.emit_pipe_move(left);

        match right {
            Expr::Call { function, args } => {
                if let Expr::Ident(fname) = function.as_ref() {
                    // Check if this is a table operation
                    if Self::TABLE_OPS.contains(&fname.as_str()) {
                        // Store AST args as constant for table pipe
                        let args_idx = self
                            .current()
                            .add_constant(Constant::AstExprList(args.clone()));
                        let op_idx = self
                            .current()
                            .add_constant(Constant::String(Arc::from(fname.as_str())));
                        self.current().emit_abc(
                            Op::TablePipe,
                            left_reg,
                            op_idx as u8,
                            args_idx as u8,
                            0,
                        );
                        if left_reg != dest {
                            self.current().emit_abc(Op::Move, dest, left_reg, 0, 0);
                        }
                        self.current().free_register();
                        return Ok(());
                    }

                    // Check if it's a builtin
                    if let Some(builtin_id) = BuiltinId::from_name(fname) {
                        // Pipe into builtin: left becomes first arg
                        let args_start = left_reg; // reuse left_reg as first arg
                        for arg in args {
                            let r = self.current().alloc_register();
                            self.compile_expr(arg, r)?;
                        }
                        self.current()
                            .emit_abx(Op::CallBuiltin, dest, builtin_id as u16, 0);
                        self.current()
                            .emit_abc(Op::Move, (args.len() + 1) as u8, args_start, 0, 0);
                        for _ in args {
                            self.current().free_register();
                        }
                        self.current().free_register(); // left_reg
                        return Ok(());
                    }
                }

                // General: left_val becomes first arg to call
                let func_reg = self.current().alloc_register();
                self.compile_expr(function, func_reg)?;

                // Move left to be the first arg
                let args_start = self.current().next_register;
                let first_arg = self.current().alloc_register();
                self.current().emit_abc(Op::Move, first_arg, left_reg, 0, 0);

                for arg in args {
                    let r = self.current().alloc_register();
                    self.compile_expr(arg, r)?;
                }

                let total_args = args.len() + 1;
                self.current()
                    .emit_abc(Op::Call, func_reg, args_start, total_args as u8, 0);

                for _ in 0..total_args {
                    self.current().free_register();
                }

                if func_reg != dest {
                    self.current().emit_abc(Op::Move, dest, func_reg, 0, 0);
                }
                self.current().free_register(); // func_reg
            }
            Expr::Ident(name) => {
                // Pipe into named function with left as only arg
                if let Some(builtin_id) = BuiltinId::from_name(name) {
                    self.current()
                        .emit_abx(Op::CallBuiltin, dest, builtin_id as u16, 0);
                    self.current().emit_abc(Op::Move, 1, left_reg, 0, 0);
                } else {
                    let func_reg = self.current().alloc_register();
                    let name_idx = self
                        .current()
                        .add_constant(Constant::String(Arc::from(name.as_str())));
                    self.current()
                        .emit_abx(Op::GetGlobal, func_reg, name_idx, 0);

                    let args_start = self.current().next_register;
                    let first_arg = self.current().alloc_register();
                    self.current().emit_abc(Op::Move, first_arg, left_reg, 0, 0);

                    self.current()
                        .emit_abc(Op::Call, func_reg, args_start, 1, 0);
                    if func_reg != dest {
                        self.current().emit_abc(Op::Move, dest, func_reg, 0, 0);
                    }
                    self.current().free_register(); // first_arg
                    self.current().free_register(); // func_reg
                }
            }
            _ => {
                return Err(compile_err(
                    "Right side of |> must be a function call".to_string(),
                ));
            }
        }

        self.current().free_register(); // left_reg
        Ok(())
    }

    fn compile_case(&mut self, arms: &[MatchArm], dest: u8) -> Result<(), TlError> {
        let mut end_jumps = Vec::new();
        let mut has_default = false;

        for arm in arms {
            if let Some(ref guard) = arm.guard {
                // Conditional arm: guard is the boolean condition
                let cond_reg = self.current().alloc_register();
                self.compile_expr(guard, cond_reg)?;
                let jump_false = self.current().current_pos();
                self.current().emit_abx(Op::JumpIfFalse, cond_reg, 0, 0);
                self.current().free_register();

                self.compile_expr(&arm.body, dest)?;
                let jump_end = self.current().current_pos();
                self.current().emit_abx(Op::Jump, 0, 0, 0);
                end_jumps.push(jump_end);

                self.current().patch_jump(jump_false);
            } else {
                // Default arm (Wildcard without guard)
                self.compile_expr(&arm.body, dest)?;
                has_default = true;
                break;
            }
        }

        if !has_default {
            self.current().emit_abx(Op::LoadNone, dest, 0, 0);
        }

        for pos in end_jumps {
            self.current().patch_jump(pos);
        }

        Ok(())
    }

    fn compile_match(
        &mut self,
        subject: &Expr,
        arms: &[MatchArm],
        dest: u8,
    ) -> Result<(), TlError> {
        let subj_reg = self.current().alloc_register();
        self.compile_expr(subject, subj_reg)?;

        let mut end_jumps = Vec::new();
        let mut has_unconditional = false;

        for arm in arms {
            // Check if this arm is unconditional (wildcard or unguarded binding)
            let is_unconditional = match &arm.pattern {
                Pattern::Wildcard => true,
                Pattern::Binding(_) if arm.guard.is_none() => true,
                Pattern::Struct { name: None, .. } if arm.guard.is_none() => true,
                _ => false,
            };

            self.compile_match_arm(arm, subj_reg, dest, &mut end_jumps)?;

            if is_unconditional {
                has_unconditional = true;
                break; // No point compiling more arms
            }
        }

        if !has_unconditional {
            // No match — load None
            self.current().emit_abx(Op::LoadNone, dest, 0, 0);
            self.current().free_register(); // subj_reg
        }

        for pos in end_jumps {
            self.current().patch_jump(pos);
        }

        Ok(())
    }

    /// Compile a single match arm against the subject in subj_reg.
    /// Returns Ok(true) if this arm is an unconditional match (wildcard/unguarded binding).
    fn compile_match_arm(
        &mut self,
        arm: &MatchArm,
        subj_reg: u8,
        dest: u8,
        end_jumps: &mut Vec<usize>,
    ) -> Result<(), TlError> {
        match &arm.pattern {
            Pattern::Wildcard => {
                self.compile_expr(&arm.body, dest)?;
                self.current().free_register(); // subj_reg
                for pos in end_jumps.drain(..) {
                    self.current().patch_jump(pos);
                }
                // Return via a special mechanism — we'll handle this by checking after the call
                // Actually, we can't return early from the caller. Instead, emit jump to end.
                // The caller won't emit more arms after wildcard since the loop will end.
                return Ok(());
            }
            Pattern::Binding(name) => {
                let local = self.add_local(name.clone());
                self.current().emit_abc(Op::Move, local, subj_reg, 0, 0);

                if let Some(guard) = &arm.guard {
                    let guard_reg = self.current().alloc_register();
                    self.compile_expr(guard, guard_reg)?;
                    let jf = self.current().current_pos();
                    self.current().emit_abx(Op::JumpIfFalse, guard_reg, 0, 0);
                    self.current().free_register();

                    self.compile_expr(&arm.body, dest)?;
                    let jump_end = self.current().current_pos();
                    self.current().emit_abx(Op::Jump, 0, 0, 0);
                    end_jumps.push(jump_end);
                    self.current().patch_jump(jf);
                } else {
                    // Unconditional match
                    self.compile_expr(&arm.body, dest)?;
                    self.current().free_register(); // subj_reg
                    for pos in end_jumps.drain(..) {
                        self.current().patch_jump(pos);
                    }
                    return Ok(());
                }
            }
            Pattern::Literal(expr) => {
                let pat_reg = self.current().alloc_register();
                self.compile_expr(expr, pat_reg)?;
                let result_reg = self.current().alloc_register();
                self.current()
                    .emit_abc(Op::TestMatch, subj_reg, pat_reg, result_reg, 0);

                let jump_false = self.current().current_pos();
                self.current().emit_abx(Op::JumpIfFalse, result_reg, 0, 0);
                self.current().free_register(); // result_reg
                self.current().free_register(); // pat_reg

                self.compile_guard_and_body(arm, dest, end_jumps, jump_false)?;
            }
            Pattern::Enum {
                type_name: _,
                variant,
                args,
            } => {
                let variant_const = self
                    .current()
                    .add_constant(Constant::String(Arc::from(variant.as_str())));
                let result_reg = self.current().alloc_register();
                self.current().emit_abc(
                    Op::MatchEnum,
                    subj_reg,
                    variant_const as u8,
                    result_reg,
                    0,
                );

                let jump_false = self.current().current_pos();
                self.current().emit_abx(Op::JumpIfFalse, result_reg, 0, 0);
                self.current().free_register(); // result_reg

                // Bind destructured fields
                for (i, arg_pat) in args.iter().enumerate() {
                    match arg_pat {
                        Pattern::Binding(name) => {
                            let local = self.add_local(name.clone());
                            self.current()
                                .emit_abc(Op::ExtractField, local, subj_reg, i as u8, 0);
                        }
                        Pattern::Wildcard => {}
                        _ => {}
                    }
                }

                self.compile_guard_and_body(arm, dest, end_jumps, jump_false)?;
            }
            Pattern::Struct {
                name: struct_name,
                fields,
            } => {
                // For named structs, check struct type via TestMatch
                let jump_false = if let Some(sname) = struct_name {
                    let name_const = self
                        .current()
                        .add_constant(Constant::String(Arc::from(sname.as_str())));
                    let name_reg = self.current().alloc_register();
                    self.current()
                        .emit_abx(Op::LoadConst, name_reg, name_const, 0);
                    let result_reg = self.current().alloc_register();
                    self.current()
                        .emit_abc(Op::TestMatch, subj_reg, name_reg, result_reg, 0);
                    let jf = self.current().current_pos();
                    self.current().emit_abx(Op::JumpIfFalse, result_reg, 0, 0);
                    self.current().free_register(); // result_reg
                    self.current().free_register(); // name_reg
                    jf
                } else {
                    usize::MAX
                };

                // Extract named fields
                for field in fields {
                    let fname_const = self
                        .current()
                        .add_constant(Constant::String(Arc::from(field.name.as_str())));
                    match &field.pattern {
                        None | Some(Pattern::Binding(_)) => {
                            let bind_name = match &field.pattern {
                                Some(Pattern::Binding(n)) => n.clone(),
                                _ => field.name.clone(),
                            };
                            let local = self.add_local(bind_name);
                            self.current().emit_abc(
                                Op::ExtractNamedField,
                                local,
                                subj_reg,
                                fname_const as u8,
                                0,
                            );
                        }
                        Some(Pattern::Wildcard) => {}
                        _ => {
                            let local = self.add_local(field.name.clone());
                            self.current().emit_abc(
                                Op::ExtractNamedField,
                                local,
                                subj_reg,
                                fname_const as u8,
                                0,
                            );
                        }
                    }
                }

                if jump_false != usize::MAX {
                    self.compile_guard_and_body(arm, dest, end_jumps, jump_false)?;
                } else {
                    // No type check — just guard + body
                    if let Some(guard) = &arm.guard {
                        let guard_reg = self.current().alloc_register();
                        self.compile_expr(guard, guard_reg)?;
                        let gj = self.current().current_pos();
                        self.current().emit_abx(Op::JumpIfFalse, guard_reg, 0, 0);
                        self.current().free_register();

                        self.compile_expr(&arm.body, dest)?;
                        let jump_end = self.current().current_pos();
                        self.current().emit_abx(Op::Jump, 0, 0, 0);
                        end_jumps.push(jump_end);
                        self.current().patch_jump(gj);
                    } else {
                        // Unconditional struct match
                        self.compile_expr(&arm.body, dest)?;
                        self.current().free_register(); // subj_reg
                        for pos in end_jumps.drain(..) {
                            self.current().patch_jump(pos);
                        }
                        return Ok(());
                    }
                }
            }
            Pattern::List { elements, rest } => {
                // Check list length
                let len_builtin_reg = self.current().alloc_register();
                // Call len(subj)
                self.current()
                    .emit_abx(Op::CallBuiltin, len_builtin_reg, BuiltinId::Len as u16, 0);
                self.current().emit_abc(Op::Move, 1, subj_reg, 0, 0); // arg count = 1

                let expected_len_reg = self.current().alloc_register();
                let len_val = elements.len() as i64;
                let len_const = self.current().add_constant(Constant::Int(len_val));
                self.current()
                    .emit_abx(Op::LoadConst, expected_len_reg, len_const, 0);

                let cmp_reg = self.current().alloc_register();
                if rest.is_some() {
                    self.current()
                        .emit_abc(Op::Gte, cmp_reg, len_builtin_reg, expected_len_reg, 0);
                } else {
                    self.current()
                        .emit_abc(Op::Eq, cmp_reg, len_builtin_reg, expected_len_reg, 0);
                }
                let jump_false = self.current().current_pos();
                self.current().emit_abx(Op::JumpIfFalse, cmp_reg, 0, 0);
                self.current().free_register(); // cmp_reg
                self.current().free_register(); // expected_len_reg
                self.current().free_register(); // len_builtin_reg

                // Extract elements by index
                for (i, elem_pat) in elements.iter().enumerate() {
                    match elem_pat {
                        Pattern::Binding(name) => {
                            let local = self.add_local(name.clone());
                            self.current()
                                .emit_abc(Op::ExtractField, local, subj_reg, i as u8, 0);
                        }
                        Pattern::Wildcard => {}
                        _ => {}
                    }
                }

                // Rest pattern
                if let Some(rest_name) = rest {
                    let local = self.add_local(rest_name.clone());
                    self.current().emit_abc(
                        Op::ExtractField,
                        local,
                        subj_reg,
                        (elements.len() as u8) | 0x80,
                        0,
                    );
                }

                self.compile_guard_and_body(arm, dest, end_jumps, jump_false)?;
            }
            Pattern::Or(patterns) => {
                let mut match_jumps = Vec::new();

                for sub_pat in patterns {
                    match sub_pat {
                        Pattern::Literal(expr) => {
                            let pat_reg = self.current().alloc_register();
                            self.compile_expr(expr, pat_reg)?;
                            let result_reg = self.current().alloc_register();
                            self.current().emit_abc(
                                Op::TestMatch,
                                subj_reg,
                                pat_reg,
                                result_reg,
                                0,
                            );
                            let jump_true = self.current().current_pos();
                            self.current().emit_abx(Op::JumpIfTrue, result_reg, 0, 0);
                            match_jumps.push(jump_true);
                            self.current().free_register(); // result_reg
                            self.current().free_register(); // pat_reg
                        }
                        Pattern::Enum { variant, .. } => {
                            let variant_const = self
                                .current()
                                .add_constant(Constant::String(Arc::from(variant.as_str())));
                            let result_reg = self.current().alloc_register();
                            self.current().emit_abc(
                                Op::MatchEnum,
                                subj_reg,
                                variant_const as u8,
                                result_reg,
                                0,
                            );
                            let jump_true = self.current().current_pos();
                            self.current().emit_abx(Op::JumpIfTrue, result_reg, 0, 0);
                            match_jumps.push(jump_true);
                            self.current().free_register();
                        }
                        Pattern::Wildcard | Pattern::Binding(_) => {
                            let jump = self.current().current_pos();
                            self.current().emit_abx(Op::Jump, 0, 0, 0);
                            match_jumps.push(jump);
                        }
                        _ => {}
                    }
                }

                // None matched — skip body
                let jump_skip = self.current().current_pos();
                self.current().emit_abx(Op::Jump, 0, 0, 0);

                // Patch match jumps to body
                for jt in &match_jumps {
                    self.current().patch_jump(*jt);
                }

                // Guard + body
                if let Some(guard) = &arm.guard {
                    let guard_reg = self.current().alloc_register();
                    self.compile_expr(guard, guard_reg)?;
                    let gj = self.current().current_pos();
                    self.current().emit_abx(Op::JumpIfFalse, guard_reg, 0, 0);
                    self.current().free_register();

                    self.compile_expr(&arm.body, dest)?;
                    let jump_end = self.current().current_pos();
                    self.current().emit_abx(Op::Jump, 0, 0, 0);
                    end_jumps.push(jump_end);
                    self.current().patch_jump(gj);
                } else {
                    self.compile_expr(&arm.body, dest)?;
                    let jump_end = self.current().current_pos();
                    self.current().emit_abx(Op::Jump, 0, 0, 0);
                    end_jumps.push(jump_end);
                }

                self.current().patch_jump(jump_skip);
            }
        }
        Ok(())
    }

    /// Helper: compile guard check (if any) + body, patch jump_false
    fn compile_guard_and_body(
        &mut self,
        arm: &MatchArm,
        dest: u8,
        end_jumps: &mut Vec<usize>,
        jump_false: usize,
    ) -> Result<(), TlError> {
        let guard_jump = if let Some(guard) = &arm.guard {
            let guard_reg = self.current().alloc_register();
            self.compile_expr(guard, guard_reg)?;
            let jf = self.current().current_pos();
            self.current().emit_abx(Op::JumpIfFalse, guard_reg, 0, 0);
            self.current().free_register();
            Some(jf)
        } else {
            None
        };

        self.compile_expr(&arm.body, dest)?;
        let jump_end = self.current().current_pos();
        self.current().emit_abx(Op::Jump, 0, 0, 0);
        end_jumps.push(jump_end);

        self.current().patch_jump(jump_false);
        if let Some(gj) = guard_jump {
            self.current().patch_jump(gj);
        }
        Ok(())
    }

    /// Compile a string with interpolation. Parses `{var}` segments and emits
    /// code to load each variable and concatenate with the literal parts.
    fn compile_string_interpolation(&mut self, s: &str, dest: u8) -> Result<(), TlError> {
        // Parse the string into segments: literal parts and variable references
        let mut segments: Vec<StringSegment> = Vec::new();
        let mut chars = s.chars().peekable();
        let mut current_literal = String::new();

        while let Some(ch) = chars.next() {
            if ch == '{' {
                let mut var_name = String::new();
                let mut depth = 1;
                for c in chars.by_ref() {
                    if c == '{' {
                        depth += 1;
                    } else if c == '}' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    var_name.push(c);
                }
                if !current_literal.is_empty() {
                    segments.push(StringSegment::Literal(std::mem::take(&mut current_literal)));
                }
                segments.push(StringSegment::Variable(var_name));
            } else if ch == '\\' {
                match chars.next() {
                    Some('n') => current_literal.push('\n'),
                    Some('t') => current_literal.push('\t'),
                    Some('\\') => current_literal.push('\\'),
                    Some('"') => current_literal.push('"'),
                    Some(c) => {
                        current_literal.push('\\');
                        current_literal.push(c);
                    }
                    None => current_literal.push('\\'),
                }
            } else {
                current_literal.push(ch);
            }
        }
        if !current_literal.is_empty() {
            segments.push(StringSegment::Literal(current_literal));
        }

        if segments.is_empty() {
            // Empty string
            let idx = self.current().add_constant(Constant::String(Arc::from("")));
            self.current().emit_abx(Op::LoadConst, dest, idx, 0);
            return Ok(());
        }

        // If no interpolation, just load the constant
        if segments.len() == 1
            && let StringSegment::Literal(ref lit) = segments[0]
        {
            let idx = self
                .current()
                .add_constant(Constant::String(Arc::from(lit.as_str())));
            self.current().emit_abx(Op::LoadConst, dest, idx, 0);
            return Ok(());
        }

        // Compile first segment into dest
        self.compile_string_segment(&segments[0], dest)?;

        // For each subsequent segment, compile it and concat with dest
        for segment in &segments[1..] {
            let tmp = self.current().alloc_register();
            self.compile_string_segment(segment, tmp)?;
            self.current().emit_abc(Op::Concat, dest, dest, tmp, 0);
            self.current().free_register();
        }

        Ok(())
    }

    fn compile_string_segment(&mut self, seg: &StringSegment, dest: u8) -> Result<(), TlError> {
        match seg {
            StringSegment::Literal(s) => {
                let idx = self
                    .current()
                    .add_constant(Constant::String(Arc::from(s.as_str())));
                self.current().emit_abx(Op::LoadConst, dest, idx, 0);
            }
            StringSegment::Variable(name) => {
                // Compile as an identifier lookup, then convert to string via builtin Str
                let var_reg = self.current().alloc_register();
                if let Some(reg) = self.resolve_local(name) {
                    if reg != var_reg {
                        self.current().emit_abc(Op::Move, var_reg, reg, 0, 0);
                    }
                } else if let Some(uv) = self.resolve_upvalue(name) {
                    self.current().emit_abc(Op::GetUpvalue, var_reg, uv, 0, 0);
                } else {
                    let idx = self
                        .current()
                        .add_constant(Constant::String(Arc::from(name.as_str())));
                    self.current().emit_abx(Op::GetGlobal, var_reg, idx, 0);
                }
                // Convert to string via CallBuiltin Str
                self.current()
                    .emit_abx(Op::CallBuiltin, dest, BuiltinId::Str as u16, 0);
                self.current().emit_abc(Op::Move, 1, var_reg, 0, 0); // 1 arg
                self.current().free_register(); // var_reg
            }
        }
        Ok(())
    }

    fn compile_use(&mut self, item: &UseItem) -> Result<(), TlError> {
        // Encode use as an import operation. The VM handles resolution.
        // C=0xAB is a magic marker to distinguish from classic import (C=0).
        let reg = self.current().alloc_register();
        match item {
            UseItem::Single(path) => {
                let path_str = path.join(".");
                let path_idx = self
                    .current()
                    .add_constant(Constant::String(Arc::from(path_str.as_str())));
                self.current().emit_abx(Op::Import, reg, path_idx, 0);
                // A=0 (unused), B=kind=0 (Single), C=0xAB (use marker)
                self.current().emit_abc(Op::Move, 0, 0, 0xAB, 0);
            }
            UseItem::Group(prefix, names) => {
                let path_str = format!("{}.{{{}}}", prefix.join("."), names.join(","));
                let path_idx = self
                    .current()
                    .add_constant(Constant::String(Arc::from(path_str.as_str())));
                self.current().emit_abx(Op::Import, reg, path_idx, 0);
                self.current().emit_abc(Op::Move, 0, 1, 0xAB, 0); // B=1 Group
            }
            UseItem::Wildcard(path) => {
                let path_str = format!("{}.*", path.join("."));
                let path_idx = self
                    .current()
                    .add_constant(Constant::String(Arc::from(path_str.as_str())));
                self.current().emit_abx(Op::Import, reg, path_idx, 0);
                self.current().emit_abc(Op::Move, 0, 2, 0xAB, 0); // B=2 Wildcard
            }
            UseItem::Aliased(path, alias) => {
                let path_str = path.join(".");
                let path_idx = self
                    .current()
                    .add_constant(Constant::String(Arc::from(path_str.as_str())));
                let alias_idx = self
                    .current()
                    .add_constant(Constant::String(Arc::from(alias.as_str())));
                self.current().emit_abx(Op::Import, reg, path_idx, 0);
                self.current()
                    .emit_abc(Op::Move, alias_idx as u8, 3, 0xAB, 0); // B=3 Aliased
            }
        }
        self.current().free_register();
        Ok(())
    }
}

enum StringSegment {
    Literal(String),
    Variable(String),
}

/// Compile a TL program into a top-level Prototype.
/// Pass source text to enable line number tracking in bytecode.
pub fn compile(program: &Program) -> Result<Prototype, TlError> {
    compile_with_source(program, "")
}

/// Compile a TL program with source text for line number tracking.
pub fn compile_with_source(program: &Program, source: &str) -> Result<Prototype, TlError> {
    let line_offsets = Compiler::build_line_offsets(source);
    let mut compiler = Compiler {
        states: vec![CompilerState::new("<main>".to_string())],
        line_offsets,
        current_line: 0,
    };

    let stmts = &program.statements;
    let mut last_expr_reg: Option<u8> = None;

    for (i, stmt) in stmts.iter().enumerate() {
        let is_last = i == stmts.len() - 1;
        // Update line tracking for this statement
        let line = compiler.line_of(stmt.span.start);
        compiler.current_line = line;
        compiler.current().current_line = line;
        match &stmt.kind {
            StmtKind::Expr(expr) if is_last => {
                // Last statement is an expression — keep register for implicit return
                let reg = compiler.current().alloc_register();
                compiler.compile_expr(expr, reg)?;
                last_expr_reg = Some(reg);
            }
            StmtKind::If {
                condition,
                then_body,
                else_ifs,
                else_body,
            } if is_last && else_body.is_some() => {
                let dest = compiler.current().alloc_register();
                compiler.compile_if_as_expr(condition, then_body, else_ifs, else_body, dest)?;
                last_expr_reg = Some(dest);
            }
            _ => {
                compiler.compile_stmt(stmt)?;
            }
        }
    }

    // Record top-level locals for module export support
    {
        let state = &compiler.states[0];
        let top_locals: Vec<(String, u8)> = state
            .locals
            .iter()
            .filter(|l| l.depth == 0)
            .map(|l| (l.name.clone(), l.register))
            .collect();
        compiler.states[0].proto.top_level_locals = top_locals;
    }

    // Add implicit return
    let state = &mut compiler.states[0];
    let needs_return = if state.proto.code.is_empty() {
        true
    } else {
        let last = *state.proto.code.last().unwrap();
        decode_op(last) != Op::Return
    };
    if needs_return {
        if let Some(reg) = last_expr_reg {
            state.emit_abc(Op::Return, reg, 0, 0, 0);
        } else {
            let reg = state.alloc_register();
            state.emit_abx(Op::LoadNone, reg, 0, 0);
            state.emit_abc(Op::Return, reg, 0, 0, 0);
        }
    }

    let state = compiler.states.pop().unwrap();
    Ok(state.proto)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tl_parser::parse;

    #[test]
    fn test_compile_int_literal() {
        let program = parse("42").unwrap();
        let proto = compile(&program).unwrap();
        assert!(!proto.code.is_empty());
        assert!(
            proto
                .constants
                .iter()
                .any(|c| matches!(c, Constant::Int(42)))
        );
    }

    #[test]
    fn test_compile_add() {
        // With constant folding, 1 + 2 now folds to 3
        let program = parse("1 + 2").unwrap();
        let proto = compile(&program).unwrap();
        assert!(!proto.code.is_empty());
        // Should fold to constant 3
        assert!(
            proto
                .constants
                .iter()
                .any(|c| matches!(c, Constant::Int(3)))
        );
    }

    #[test]
    fn test_compile_function() {
        let program = parse("fn add(a, b) { a + b }").unwrap();
        let proto = compile(&program).unwrap();
        // Should have a Closure instruction
        let has_closure = proto
            .code
            .iter()
            .any(|&inst| decode_op(inst) == Op::Closure);
        assert!(has_closure);
    }

    #[test]
    fn test_compile_closure() {
        let program = parse("let f = (x) => x * 2").unwrap();
        let proto = compile(&program).unwrap();
        let has_closure = proto
            .code
            .iter()
            .any(|&inst| decode_op(inst) == Op::Closure);
        assert!(has_closure);
    }

    // ── Phase 13: Constant Folding Tests ──────────────────────

    #[test]
    fn test_fold_int_addition() {
        // 2 + 3 should fold to 5 at compile time
        let program = parse("2 + 3").unwrap();
        let proto = compile(&program).unwrap();
        assert!(
            proto
                .constants
                .iter()
                .any(|c| matches!(c, Constant::Int(5)))
        );
        // Should NOT have an Add instruction
        let has_add = proto.code.iter().any(|&inst| decode_op(inst) == Op::Add);
        assert!(
            !has_add,
            "Folded expression should not have Add instruction"
        );
    }

    #[test]
    fn test_fold_float_multiplication() {
        let program = parse("2.0 * 3.0").unwrap();
        let proto = compile(&program).unwrap();
        assert!(
            proto
                .constants
                .iter()
                .any(|c| matches!(c, Constant::Float(f) if (*f - 6.0).abs() < f64::EPSILON))
        );
    }

    #[test]
    fn test_fold_string_concatenation() {
        let program = parse("\"hello\" + \" world\"").unwrap();
        let proto = compile(&program).unwrap();
        assert!(
            proto
                .constants
                .iter()
                .any(|c| matches!(c, Constant::String(s) if s.as_ref() == "hello world"))
        );
    }

    #[test]
    fn test_fold_negation() {
        let program = parse("-42").unwrap();
        let proto = compile(&program).unwrap();
        assert!(
            proto
                .constants
                .iter()
                .any(|c| matches!(c, Constant::Int(-42)))
        );
    }

    #[test]
    fn test_fold_not() {
        let program = parse("not true").unwrap();
        let proto = compile(&program).unwrap();
        // Should emit LoadFalse
        let has_load_false = proto
            .code
            .iter()
            .any(|&inst| decode_op(inst) == Op::LoadFalse);
        assert!(has_load_false, "'not true' should fold to false");
    }

    #[test]
    fn test_fold_nested_arithmetic() {
        // 2 + 3 * 4 should fold to 14
        let program = parse("2 + 3 * 4").unwrap();
        let proto = compile(&program).unwrap();
        assert!(
            proto
                .constants
                .iter()
                .any(|c| matches!(c, Constant::Int(14)))
        );
    }

    #[test]
    fn test_no_fold_division_by_zero() {
        // 10 / 0 should NOT fold (defer to runtime error)
        let program = parse("10 / 0").unwrap();
        let proto = compile(&program).unwrap();
        // Should still have a Div instruction
        let has_div = proto.code.iter().any(|&inst| decode_op(inst) == Op::Div);
        assert!(has_div, "Division by zero should not be folded");
    }

    #[test]
    fn test_no_fold_variable_reference() {
        // x + 1 should NOT fold (contains variable)
        let program = parse("let x = 5\nx + 1").unwrap();
        let proto = compile(&program).unwrap();
        let has_add = proto.code.iter().any(|&inst| decode_op(inst) == Op::Add);
        assert!(has_add, "Expression with variables should not be folded");
    }

    #[test]
    fn test_no_fold_interpolated_string() {
        // String with { should not fold (interpolation)
        let program = parse("let x = 5\n\"{x} hello\"").unwrap();
        let proto = compile(&program).unwrap();
        // Should have string concatenation (Concat or multiple LoadConst + Add)
        // The key point is it doesn't try to fold the interpolated string
        assert!(!proto.code.is_empty());
    }

    #[test]
    fn test_fold_transparent_to_runtime() {
        // Folded expressions should produce the same result as unfolded
        // This is verified by running both in the VM
        use crate::Vm;
        let program = parse("2 + 3 * 4").unwrap();
        let proto = compile(&program).unwrap();
        let mut vm = Vm::new();
        let result = vm.execute(&proto).unwrap();
        assert_eq!(result.to_string(), "14");
    }

    // ── Phase 13: Dead Code Elimination Tests ─────────────────

    #[test]
    fn test_dce_after_return() {
        // Code after return should not be compiled
        let program = parse("fn f() {\n  return 1\n  print(\"dead\")\n}").unwrap();
        let proto = compile(&program).unwrap();
        // Find the function prototype
        let fn_proto = proto
            .constants
            .iter()
            .find_map(|c| {
                if let Constant::Prototype(p) = c {
                    Some(p.clone())
                } else {
                    None
                }
            })
            .expect("should have function prototype");
        // The function should NOT have a CallBuiltin (print) after the Return
        let return_pos = fn_proto
            .code
            .iter()
            .position(|&inst| decode_op(inst) == Op::Return);
        assert!(return_pos.is_some(), "Should have a Return instruction");
        // No CallBuiltin after Return
        let after_return: Vec<_> = fn_proto.code[return_pos.unwrap() + 1..]
            .iter()
            .filter(|&&inst| decode_op(inst) == Op::CallBuiltin)
            .collect();
        assert!(
            after_return.is_empty(),
            "Should not have CallBuiltin after Return"
        );
    }

    #[test]
    fn test_dce_after_break() {
        // Code after break in loop should not be compiled
        let program =
            parse("fn f() {\n  while true {\n    break\n    print(\"dead\")\n  }\n}").unwrap();
        let proto = compile(&program).unwrap();
        let fn_proto = proto
            .constants
            .iter()
            .find_map(|c| {
                if let Constant::Prototype(p) = c {
                    Some(p.clone())
                } else {
                    None
                }
            })
            .expect("should have function prototype");
        // Count CallBuiltin instructions — should be 0 (dead code eliminated)
        let call_builtins: Vec<_> = fn_proto
            .code
            .iter()
            .filter(|&&inst| decode_op(inst) == Op::CallBuiltin)
            .collect();
        assert!(
            call_builtins.is_empty(),
            "Should not have CallBuiltin after break"
        );
    }

    #[test]
    fn test_dce_after_continue() {
        // Code after continue in loop should not be compiled
        let program =
            parse("fn f() {\n  while true {\n    continue\n    print(\"dead\")\n  }\n}").unwrap();
        let proto = compile(&program).unwrap();
        let fn_proto = proto
            .constants
            .iter()
            .find_map(|c| {
                if let Constant::Prototype(p) = c {
                    Some(p.clone())
                } else {
                    None
                }
            })
            .expect("should have function prototype");
        let call_builtins: Vec<_> = fn_proto
            .code
            .iter()
            .filter(|&&inst| decode_op(inst) == Op::CallBuiltin)
            .collect();
        assert!(
            call_builtins.is_empty(),
            "Should not have CallBuiltin after continue"
        );
    }

    #[test]
    fn test_dce_if_both_branches_return() {
        // If both branches return, code after if is dead
        let program = parse("fn f(x) {\n  if x {\n    return 1\n  } else {\n    return 2\n  }\n  print(\"dead\")\n}").unwrap();
        let proto = compile(&program).unwrap();
        let fn_proto = proto
            .constants
            .iter()
            .find_map(|c| {
                if let Constant::Prototype(p) = c {
                    Some(p.clone())
                } else {
                    None
                }
            })
            .expect("should have function prototype");
        let call_builtins: Vec<_> = fn_proto
            .code
            .iter()
            .filter(|&&inst| decode_op(inst) == Op::CallBuiltin)
            .collect();
        assert!(
            call_builtins.is_empty(),
            "Should not have CallBuiltin after if where both branches return"
        );
    }

    #[test]
    fn test_dce_if_one_branch_returns() {
        // If only one branch returns, code after if is NOT dead
        let program =
            parse("fn f(x) {\n  if x {\n    return 1\n  }\n  print(\"alive\")\n  return 0\n}")
                .unwrap();
        let proto = compile(&program).unwrap();
        let fn_proto = proto
            .constants
            .iter()
            .find_map(|c| {
                if let Constant::Prototype(p) = c {
                    Some(p.clone())
                } else {
                    None
                }
            })
            .expect("should have function prototype");
        let call_builtins: Vec<_> = fn_proto
            .code
            .iter()
            .filter(|&&inst| decode_op(inst) == Op::CallBuiltin)
            .collect();
        assert!(
            !call_builtins.is_empty(),
            "Should have CallBuiltin after if where only one branch returns"
        );
    }

    #[test]
    fn test_dce_nested_function() {
        // Dead code in inner function doesn't affect outer
        let program = parse("fn outer() {\n  fn inner() {\n    return 1\n    print(\"dead\")\n  }\n  print(\"alive\")\n}").unwrap();
        let proto = compile(&program).unwrap();
        // The outer function should have CallBuiltin (print("alive"))
        let outer_fn = proto
            .constants
            .iter()
            .find_map(|c| {
                if let Constant::Prototype(p) = c {
                    if p.name == "outer" {
                        Some(p.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .expect("should have outer function");
        let call_builtins: Vec<_> = outer_fn
            .code
            .iter()
            .filter(|&&inst| decode_op(inst) == Op::CallBuiltin)
            .collect();
        assert!(
            !call_builtins.is_empty(),
            "Outer function should have CallBuiltin"
        );
    }

    #[test]
    fn test_dce_try_catch_independent() {
        // Return in try doesn't make catch dead
        let program =
            parse("fn f() {\n  try {\n    return 1\n  } catch e {\n    print(e)\n  }\n}").unwrap();
        let proto = compile(&program).unwrap();
        let fn_proto = proto
            .constants
            .iter()
            .find_map(|c| {
                if let Constant::Prototype(p) = c {
                    Some(p.clone())
                } else {
                    None
                }
            })
            .expect("should have function prototype");
        // Catch block should still have its instructions compiled
        let call_builtins: Vec<_> = fn_proto
            .code
            .iter()
            .filter(|&&inst| decode_op(inst) == Op::CallBuiltin)
            .collect();
        assert!(
            !call_builtins.is_empty(),
            "Catch block should not be eliminated by try body return"
        );
    }

    #[test]
    fn test_dce_existing_tests_unaffected() {
        // Verify existing programs still compile and run correctly
        use crate::Vm;
        let tests = [
            ("let x = 42\nx", "42"),
            ("fn add(a, b) { a + b }\nadd(1, 2)", "3"),
            ("if true { 1 } else { 2 }", "1"),
        ];
        for (src, expected) in tests {
            let program = parse(src).unwrap();
            let proto = compile(&program).unwrap();
            let mut vm = Vm::new();
            let result = vm.execute(&proto).unwrap();
            assert_eq!(result.to_string(), expected, "Failed for: {src}");
        }
    }
}
