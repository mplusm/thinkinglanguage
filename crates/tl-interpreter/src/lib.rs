// ThinkingLanguage — Tree-Walking Interpreter
// Licensed under MIT OR Apache-2.0
//
// Phase 0: Executes TL programs by walking the AST directly.
// This is slow but correct — used for REPL and initial development.
// Will be replaced by compiled execution in Phase 2.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tl_ast::*;
use tl_errors::{RuntimeError, TlError};
use tl_data::translate::{translate_expr, LocalValue, TranslateContext};
use tl_data::{
    ArrowDataType, ArrowField, ArrowSchema,
    DataFrame, DataEngine, JoinType, col,
};
use tl_stream::{ConnectorConfig, PipelineDef, PipelineRunner, PipelineStatus, StreamDef};

/// Wrapper around DataFusion DataFrame that implements Debug + Clone.
#[derive(Clone)]
pub struct TlTable {
    pub df: DataFrame,
}

impl fmt::Debug for TlTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<table>")
    }
}

/// Schema definition: column names and Arrow types.
#[derive(Debug, Clone)]
pub struct TlSchema {
    pub name: String,
    pub arrow_schema: Arc<ArrowSchema>,
}

/// Runtime value
#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    List(Vec<Value>),
    None,
    /// A function defined in TL code
    Function {
        name: String,
        params: Vec<Param>,
        body: Vec<Stmt>,
    },
    /// A built-in function
    Builtin(String),
    /// A closure (anonymous function with captured environment)
    Closure {
        params: Vec<Param>,
        body: Box<Expr>,
        captured_env: Vec<HashMap<String, Value>>,
    },
    /// A lazy DataFusion table (DataFrame)
    Table(TlTable),
    /// A schema definition
    Schema(TlSchema),
    /// A tensor (ndarray)
    Tensor(tl_ai::TlTensor),
    /// A trained model
    Model(tl_ai::TlModel),
    /// A connector configuration
    Connector(ConnectorConfig),
    /// A pipeline definition
    Pipeline(PipelineDef),
    /// A stream definition
    Stream(StreamDef),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int(n) => write!(f, "{n}"),
            Value::Float(n) => {
                if n.fract() == 0.0 {
                    write!(f, "{n:.1}")
                } else {
                    write!(f, "{n}")
                }
            }
            Value::String(s) => write!(f, "{s}"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::List(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "]")
            }
            Value::None => write!(f, "none"),
            Value::Function { name, .. } => write!(f, "<fn {name}>"),
            Value::Builtin(name) => write!(f, "<builtin {name}>"),
            Value::Closure { .. } => write!(f, "<closure>"),
            Value::Table(_) => write!(f, "<table>"),
            Value::Schema(s) => write!(f, "<schema {}>", s.name),
            Value::Tensor(t) => write!(f, "{t}"),
            Value::Model(m) => write!(f, "{m}"),
            Value::Connector(c) => write!(f, "{c}"),
            Value::Pipeline(p) => write!(f, "{p}"),
            Value::Stream(s) => write!(f, "{s}"),
        }
    }
}

impl Value {
    pub fn is_truthy(&self) -> bool {
        match self {
            Value::Bool(b) => *b,
            Value::Int(n) => *n != 0,
            Value::Float(n) => *n != 0.0,
            Value::String(s) => !s.is_empty(),
            Value::List(items) => !items.is_empty(),
            Value::None => false,
            _ => true,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Int(_) => "int64",
            Value::Float(_) => "float64",
            Value::String(_) => "string",
            Value::Bool(_) => "bool",
            Value::List(_) => "list",
            Value::None => "none",
            Value::Function { .. } => "function",
            Value::Builtin(_) => "builtin",
            Value::Closure { .. } => "closure",
            Value::Table(_) => "table",
            Value::Schema(_) => "schema",
            Value::Tensor(_) => "tensor",
            Value::Model(_) => "model",
            Value::Connector(_) => "connector",
            Value::Pipeline(_) => "pipeline",
            Value::Stream(_) => "stream",
        }
    }
}

/// Control flow signals
enum Signal {
    None,
    Return(Value),
    Break,
    Continue,
}

/// Variable environment (scope chain)
#[derive(Debug, Clone)]
pub struct Environment {
    scopes: Vec<HashMap<String, Value>>,
}

impl Environment {
    pub fn new() -> Self {
        let mut global = HashMap::new();
        // Register builtins
        global.insert("print".to_string(), Value::Builtin("print".to_string()));
        global.insert("println".to_string(), Value::Builtin("println".to_string()));
        global.insert("len".to_string(), Value::Builtin("len".to_string()));
        global.insert("str".to_string(), Value::Builtin("str".to_string()));
        global.insert("int".to_string(), Value::Builtin("int".to_string()));
        global.insert("float".to_string(), Value::Builtin("float".to_string()));
        global.insert("abs".to_string(), Value::Builtin("abs".to_string()));
        global.insert("min".to_string(), Value::Builtin("min".to_string()));
        global.insert("max".to_string(), Value::Builtin("max".to_string()));
        global.insert("range".to_string(), Value::Builtin("range".to_string()));
        global.insert("push".to_string(), Value::Builtin("push".to_string()));
        global.insert("type_of".to_string(), Value::Builtin("type_of".to_string()));
        global.insert("map".to_string(), Value::Builtin("map".to_string()));
        global.insert("filter".to_string(), Value::Builtin("filter".to_string()));
        global.insert("reduce".to_string(), Value::Builtin("reduce".to_string()));
        global.insert("sum".to_string(), Value::Builtin("sum".to_string()));
        global.insert("any".to_string(), Value::Builtin("any".to_string()));
        global.insert("all".to_string(), Value::Builtin("all".to_string()));
        // Data engine builtins
        global.insert("read_csv".to_string(), Value::Builtin("read_csv".to_string()));
        global.insert("read_parquet".to_string(), Value::Builtin("read_parquet".to_string()));
        global.insert("write_csv".to_string(), Value::Builtin("write_csv".to_string()));
        global.insert("write_parquet".to_string(), Value::Builtin("write_parquet".to_string()));
        global.insert("collect".to_string(), Value::Builtin("collect".to_string()));
        global.insert("show".to_string(), Value::Builtin("show".to_string()));
        global.insert("describe".to_string(), Value::Builtin("describe".to_string()));
        global.insert("head".to_string(), Value::Builtin("head".to_string()));
        global.insert("postgres".to_string(), Value::Builtin("postgres".to_string()));
        // AI builtins
        global.insert("tensor".to_string(), Value::Builtin("tensor".to_string()));
        global.insert("tensor_zeros".to_string(), Value::Builtin("tensor_zeros".to_string()));
        global.insert("tensor_ones".to_string(), Value::Builtin("tensor_ones".to_string()));
        global.insert("tensor_shape".to_string(), Value::Builtin("tensor_shape".to_string()));
        global.insert("tensor_reshape".to_string(), Value::Builtin("tensor_reshape".to_string()));
        global.insert("tensor_transpose".to_string(), Value::Builtin("tensor_transpose".to_string()));
        global.insert("tensor_sum".to_string(), Value::Builtin("tensor_sum".to_string()));
        global.insert("tensor_mean".to_string(), Value::Builtin("tensor_mean".to_string()));
        global.insert("tensor_dot".to_string(), Value::Builtin("tensor_dot".to_string()));
        global.insert("predict".to_string(), Value::Builtin("predict".to_string()));
        global.insert("embed".to_string(), Value::Builtin("embed".to_string()));
        global.insert("similarity".to_string(), Value::Builtin("similarity".to_string()));
        global.insert("ai_complete".to_string(), Value::Builtin("ai_complete".to_string()));
        global.insert("ai_chat".to_string(), Value::Builtin("ai_chat".to_string()));
        global.insert("model_save".to_string(), Value::Builtin("model_save".to_string()));
        global.insert("model_load".to_string(), Value::Builtin("model_load".to_string()));
        global.insert("model_register".to_string(), Value::Builtin("model_register".to_string()));
        global.insert("model_list".to_string(), Value::Builtin("model_list".to_string()));
        global.insert("model_get".to_string(), Value::Builtin("model_get".to_string()));
        // Streaming builtins
        global.insert("alert_slack".to_string(), Value::Builtin("alert_slack".to_string()));
        global.insert("alert_webhook".to_string(), Value::Builtin("alert_webhook".to_string()));
        global.insert("emit".to_string(), Value::Builtin("emit".to_string()));
        global.insert("lineage".to_string(), Value::Builtin("lineage".to_string()));
        global.insert("run_pipeline".to_string(), Value::Builtin("run_pipeline".to_string()));

        Self {
            scopes: vec![global],
        }
    }

    pub fn push_scope(&mut self) {
        self.scopes.push(HashMap::new());
    }

    pub fn pop_scope(&mut self) {
        self.scopes.pop();
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        for scope in self.scopes.iter().rev() {
            if let Some(val) = scope.get(name) {
                return Some(val);
            }
        }
        None
    }

    pub fn set(&mut self, name: String, value: Value) {
        if let Some(scope) = self.scopes.last_mut() {
            scope.insert(name, value);
        }
    }

    /// Update an existing variable in the nearest scope that contains it
    pub fn update(&mut self, name: &str, value: Value) -> bool {
        for scope in self.scopes.iter_mut().rev() {
            if scope.contains_key(name) {
                scope.insert(name.to_string(), value);
                return true;
            }
        }
        false
    }
}

impl Default for Environment {
    fn default() -> Self {
        Self::new()
    }
}

/// The interpreter
pub struct Interpreter {
    pub env: Environment,
    /// Captured output (for testing)
    pub output: Vec<String>,
    /// Track last expression value for REPL display
    last_expr_value: Option<Value>,
    /// Data engine (lazily initialized)
    data_engine: Option<DataEngine>,
}

impl Interpreter {
    pub fn new() -> Self {
        Self {
            env: Environment::new(),
            output: Vec::new(),
            last_expr_value: None,
            data_engine: None,
        }
    }

    /// Get or create the DataEngine (lazy init).
    fn engine(&mut self) -> &DataEngine {
        if self.data_engine.is_none() {
            self.data_engine = Some(DataEngine::new());
        }
        self.data_engine.as_ref().unwrap()
    }

    /// Execute a complete program
    pub fn execute(&mut self, program: &Program) -> Result<Value, TlError> {
        let mut last = Value::None;
        for stmt in &program.statements {
            match self.exec_stmt(stmt)? {
                Signal::Return(val) => return Ok(val),
                Signal::None => {}
                Signal::Break | Signal::Continue => {
                    return Err(TlError::Runtime(RuntimeError {
                        message: "break/continue outside of loop".to_string(),
                        span: None,
                    }))
                }
            }
            // Track last expression value for REPL
            if let Stmt::Expr(_) = stmt {
                last = self.last_expr_value.clone().unwrap_or(Value::None);
            }
        }
        Ok(last)
    }

    /// Execute a single statement (for REPL)
    pub fn execute_stmt(&mut self, stmt: &Stmt) -> Result<Value, TlError> {
        self.last_expr_value = None;
        match self.exec_stmt(stmt)? {
            Signal::Return(val) => Ok(val),
            _ => Ok(self.last_expr_value.clone().unwrap_or(Value::None)),
        }
    }

}

impl Default for Interpreter {
    fn default() -> Self {
        Self::new()
    }
}

// ── Statement execution ──────────────────────────────────

impl Interpreter {
    fn exec_stmt(&mut self, stmt: &Stmt) -> Result<Signal, TlError> {
        match stmt {
            Stmt::Let {
                name,
                value,
                ..
            } => {
                let val = self.eval_expr(value)?;
                self.env.set(name.clone(), val);
                Ok(Signal::None)
            }
            Stmt::FnDecl {
                name,
                params,
                body,
                ..
            } => {
                let func = Value::Function {
                    name: name.clone(),
                    params: params.clone(),
                    body: body.clone(),
                };
                self.env.set(name.clone(), func);
                Ok(Signal::None)
            }
            Stmt::Expr(expr) => {
                let val = self.eval_expr(expr)?;
                self.last_expr_value = Some(val);
                Ok(Signal::None)
            }
            Stmt::Return(expr) => {
                let val = match expr {
                    Some(e) => self.eval_expr(e)?,
                    None => Value::None,
                };
                Ok(Signal::Return(val))
            }
            Stmt::If {
                condition,
                then_body,
                else_ifs,
                else_body,
            } => {
                let cond = self.eval_expr(condition)?;
                if cond.is_truthy() {
                    return self.exec_block(then_body);
                }
                for (cond_expr, body) in else_ifs {
                    let cond = self.eval_expr(cond_expr)?;
                    if cond.is_truthy() {
                        return self.exec_block(body);
                    }
                }
                if let Some(body) = else_body {
                    return self.exec_block(body);
                }
                Ok(Signal::None)
            }
            Stmt::While { condition, body } => {
                loop {
                    let cond = self.eval_expr(condition)?;
                    if !cond.is_truthy() {
                        break;
                    }
                    match self.exec_block(body)? {
                        Signal::Break => break,
                        Signal::Return(v) => return Ok(Signal::Return(v)),
                        Signal::Continue | Signal::None => continue,
                    }
                }
                Ok(Signal::None)
            }
            Stmt::For { name, iter, body } => {
                let iter_val = self.eval_expr(iter)?;
                let items = match iter_val {
                    Value::List(items) => items,
                    _ => {
                        return Err(TlError::Runtime(RuntimeError {
                            message: format!("Cannot iterate over {}", iter_val.type_name()),
                            span: None,
                        }))
                    }
                };
                for item in items {
                    self.env.push_scope();
                    self.env.set(name.clone(), item);
                    let signal = self.exec_block(body)?;
                    self.env.pop_scope();
                    match signal {
                        Signal::Break => break,
                        Signal::Return(v) => return Ok(Signal::Return(v)),
                        Signal::Continue | Signal::None => continue,
                    }
                }
                Ok(Signal::None)
            }
            Stmt::Schema { name, fields } => {
                let arrow_fields: Vec<ArrowField> = fields
                    .iter()
                    .map(|f| {
                        let dt = tl_type_to_arrow(&f.type_ann);
                        ArrowField::new(&f.name, dt, true)
                    })
                    .collect();
                let schema = TlSchema {
                    name: name.clone(),
                    arrow_schema: Arc::new(ArrowSchema::new(arrow_fields)),
                };
                self.env.set(name.clone(), Value::Schema(schema));
                Ok(Signal::None)
            }
            Stmt::Train { name, algorithm, config } => {
                self.exec_train(name, algorithm, config)
            }
            Stmt::Pipeline { name, extract, transform, load, schedule, timeout, retries, on_failure, on_success } => {
                self.exec_pipeline(name, extract, transform, load, schedule, timeout, retries, on_failure, on_success)
            }
            Stmt::StreamDecl { name, source, transform, sink, window, watermark } => {
                self.exec_stream_decl(name, source, transform, sink, window, watermark)
            }
            Stmt::SourceDecl { name, connector_type, config } => {
                self.exec_source_decl(name, connector_type, config)
            }
            Stmt::SinkDecl { name, connector_type, config } => {
                self.exec_sink_decl(name, connector_type, config)
            }
            Stmt::Break => Ok(Signal::Break),
            Stmt::Continue => Ok(Signal::Continue),
        }
    }

    fn exec_block(&mut self, stmts: &[Stmt]) -> Result<Signal, TlError> {
        self.env.push_scope();
        let mut result = Signal::None;
        for stmt in stmts {
            result = self.exec_stmt(stmt)?;
            match &result {
                Signal::Return(_) | Signal::Break | Signal::Continue => {
                    self.env.pop_scope();
                    return Ok(result);
                }
                Signal::None => {}
            }
        }
        self.env.pop_scope();
        Ok(result)
    }

    // ── Expression evaluation ────────────────────────────────

    fn eval_expr(&mut self, expr: &Expr) -> Result<Value, TlError> {
        match expr {
            Expr::Int(n) => Ok(Value::Int(*n)),
            Expr::Float(n) => Ok(Value::Float(*n)),
            Expr::String(s) => Ok(Value::String(self.interpolate_string(s)?)),
            Expr::Bool(b) => Ok(Value::Bool(*b)),
            Expr::None => Ok(Value::None),

            Expr::Ident(name) => self.env.get(name).cloned().ok_or_else(|| {
                TlError::Runtime(RuntimeError {
                    message: format!("Undefined variable: `{name}`"),
                    span: None,
                })
            }),

            Expr::BinOp { left, op, right } => {
                let l = self.eval_expr(left)?;
                let r = self.eval_expr(right)?;
                self.eval_binop(&l, op, &r)
            }

            Expr::UnaryOp { op, expr } => {
                let val = self.eval_expr(expr)?;
                match op {
                    UnaryOp::Neg => match val {
                        Value::Int(n) => Ok(Value::Int(-n)),
                        Value::Float(n) => Ok(Value::Float(-n)),
                        _ => Err(runtime_err(format!(
                            "Cannot negate {}",
                            val.type_name()
                        ))),
                    },
                    UnaryOp::Not => Ok(Value::Bool(!val.is_truthy())),
                }
            }

            Expr::Call { function, args } => {
                let func = self.eval_expr(function)?;
                let mut eval_args = Vec::new();
                for arg in args {
                    eval_args.push(self.eval_expr(arg)?);
                }
                self.call_function(&func, &eval_args)
            }

            Expr::Pipe { left, right } => {
                let left_val = self.eval_expr(left)?;
                // Table-aware pipe: if left is a Table, dispatch to table operations
                if let Value::Table(ref tl_table) = left_val {
                    return self.eval_table_pipe(tl_table.df.clone(), right);
                }
                // Regular pipe: left_val becomes the first argument to the right-side call
                match right.as_ref() {
                    Expr::Call { function, args } => {
                        let func = self.eval_expr(function)?;
                        let mut all_args = vec![left_val];
                        for arg in args {
                            all_args.push(self.eval_expr(arg)?);
                        }
                        self.call_function(&func, &all_args)
                    }
                    Expr::Ident(name) => {
                        let func = self.env.get(name).cloned().ok_or_else(|| {
                            TlError::Runtime(RuntimeError {
                                message: format!("Undefined function: `{name}`"),
                                span: None,
                            })
                        })?;
                        self.call_function(&func, &[left_val])
                    }
                    _ => Err(runtime_err(
                        "Right side of |> must be a function call".to_string(),
                    )),
                }
            }

            Expr::List(elements) => {
                let mut items = Vec::new();
                for el in elements {
                    items.push(self.eval_expr(el)?);
                }
                Ok(Value::List(items))
            }

            Expr::Index { object, index } => {
                let obj = self.eval_expr(object)?;
                let idx = self.eval_expr(index)?;
                match (&obj, &idx) {
                    (Value::List(items), Value::Int(i)) => {
                        let i = *i as usize;
                        items.get(i).cloned().ok_or_else(|| {
                            runtime_err(format!(
                                "Index {i} out of bounds for list of length {}",
                                items.len()
                            ))
                        })
                    }
                    _ => Err(runtime_err(format!(
                        "Cannot index {} with {}",
                        obj.type_name(),
                        idx.type_name()
                    ))),
                }
            }

            Expr::Case { arms } => {
                for (pattern, body) in arms {
                    // Wildcard _ always matches
                    if matches!(pattern, Expr::Ident(s) if s == "_") {
                        return self.eval_expr(body);
                    }
                    let val = self.eval_expr(pattern)?;
                    if val.is_truthy() {
                        return self.eval_expr(body);
                    }
                }
                Ok(Value::None)
            }

            Expr::Match { subject, arms } => {
                let subject_val = self.eval_expr(subject)?;
                for (pattern, body) in arms {
                    // Wildcard _ always matches
                    if matches!(pattern, Expr::Ident(s) if s == "_") {
                        return self.eval_expr(body);
                    }
                    let pattern_val = self.eval_expr(pattern)?;
                    let matched = match (&subject_val, &pattern_val) {
                        (Value::Int(a), Value::Int(b)) => a == b,
                        (Value::Float(a), Value::Float(b)) => a == b,
                        (Value::String(a), Value::String(b)) => a == b,
                        (Value::Bool(a), Value::Bool(b)) => a == b,
                        (Value::None, Value::None) => true,
                        _ => false, // type mismatch = no match
                    };
                    if matched {
                        return self.eval_expr(body);
                    }
                }
                Ok(Value::None)
            }

            Expr::NullCoalesce { expr, default } => {
                let val = self.eval_expr(expr)?;
                if matches!(val, Value::None) {
                    self.eval_expr(default)
                } else {
                    Ok(val)
                }
            }

            Expr::Closure { params, body } => {
                Ok(Value::Closure {
                    params: params.clone(),
                    body: Box::new(body.as_ref().clone()),
                    captured_env: self.env.scopes.clone(),
                })
            }

            Expr::Assign { target, value } => {
                let val = self.eval_expr(value)?;
                if let Expr::Ident(name) = target.as_ref() {
                    if self.env.update(name, val.clone()) {
                        Ok(val)
                    } else {
                        Err(runtime_err(format!("Undefined variable: `{name}`")))
                    }
                } else {
                    Err(runtime_err("Invalid assignment target".to_string()))
                }
            }

            _ => Err(runtime_err(format!("Unsupported expression: {expr:?}"))),
        }
    }

    fn eval_binop(&self, left: &Value, op: &BinOp, right: &Value) -> Result<Value, TlError> {
        match (left, right) {
            // Int operations
            (Value::Int(a), Value::Int(b)) => match op {
                BinOp::Add => Ok(Value::Int(a + b)),
                BinOp::Sub => Ok(Value::Int(a - b)),
                BinOp::Mul => Ok(Value::Int(a * b)),
                BinOp::Div => {
                    if *b == 0 {
                        Err(runtime_err("Division by zero".to_string()))
                    } else {
                        Ok(Value::Int(a / b))
                    }
                }
                BinOp::Mod => {
                    if *b == 0 {
                        Err(runtime_err("Modulo by zero".to_string()))
                    } else {
                        Ok(Value::Int(a % b))
                    }
                }
                BinOp::Pow => Ok(Value::Int(a.pow(*b as u32)),),
                BinOp::Eq => Ok(Value::Bool(a == b)),
                BinOp::Neq => Ok(Value::Bool(a != b)),
                BinOp::Lt => Ok(Value::Bool(a < b)),
                BinOp::Gt => Ok(Value::Bool(a > b)),
                BinOp::Lte => Ok(Value::Bool(a <= b)),
                BinOp::Gte => Ok(Value::Bool(a >= b)),
                BinOp::And => Ok(Value::Bool(*a != 0 && *b != 0)),
                BinOp::Or => Ok(Value::Bool(*a != 0 || *b != 0)),
            },

            // Float operations
            (Value::Float(a), Value::Float(b)) => match op {
                BinOp::Add => Ok(Value::Float(a + b)),
                BinOp::Sub => Ok(Value::Float(a - b)),
                BinOp::Mul => Ok(Value::Float(a * b)),
                BinOp::Div => Ok(Value::Float(a / b)),
                BinOp::Mod => Ok(Value::Float(a % b)),
                BinOp::Pow => Ok(Value::Float(a.powf(*b))),
                BinOp::Eq => Ok(Value::Bool(a == b)),
                BinOp::Neq => Ok(Value::Bool(a != b)),
                BinOp::Lt => Ok(Value::Bool(a < b)),
                BinOp::Gt => Ok(Value::Bool(a > b)),
                BinOp::Lte => Ok(Value::Bool(a <= b)),
                BinOp::Gte => Ok(Value::Bool(a >= b)),
                _ => Err(runtime_err(format!("Unsupported op: float {op} float"))),
            },

            // Int-Float mixed (promote int to float)
            (Value::Int(a), Value::Float(b)) => {
                self.eval_binop(&Value::Float(*a as f64), op, &Value::Float(*b))
            }
            (Value::Float(a), Value::Int(b)) => {
                self.eval_binop(&Value::Float(*a), op, &Value::Float(*b as f64))
            }

            // String concatenation
            (Value::String(a), Value::String(b)) if *op == BinOp::Add => {
                Ok(Value::String(format!("{a}{b}")))
            }

            // String repeat
            (Value::String(a), Value::Int(b)) if *op == BinOp::Mul => {
                Ok(Value::String(a.repeat(*b as usize)))
            }

            // Boolean logic
            (Value::Bool(a), Value::Bool(b)) => match op {
                BinOp::And => Ok(Value::Bool(*a && *b)),
                BinOp::Or => Ok(Value::Bool(*a || *b)),
                BinOp::Eq => Ok(Value::Bool(a == b)),
                BinOp::Neq => Ok(Value::Bool(a != b)),
                _ => Err(runtime_err(format!("Unsupported op: bool {op} bool"))),
            },

            // String equality
            (Value::String(a), Value::String(b)) => match op {
                BinOp::Eq => Ok(Value::Bool(a == b)),
                BinOp::Neq => Ok(Value::Bool(a != b)),
                _ => Err(runtime_err(format!(
                    "Unsupported op: string {op} string"
                ))),
            },

            // Tensor arithmetic
            (Value::Tensor(a), Value::Tensor(b)) => match op {
                BinOp::Add => {
                    let result = a.add(b).map_err(|e| runtime_err(e))?;
                    Ok(Value::Tensor(result))
                }
                BinOp::Sub => {
                    let result = a.sub(b).map_err(|e| runtime_err(e))?;
                    Ok(Value::Tensor(result))
                }
                BinOp::Mul => {
                    let result = a.mul(b).map_err(|e| runtime_err(e))?;
                    Ok(Value::Tensor(result))
                }
                BinOp::Div => {
                    let result = a.div(b).map_err(|e| runtime_err(e))?;
                    Ok(Value::Tensor(result))
                }
                _ => Err(runtime_err(format!("Unsupported op: tensor {op} tensor"))),
            },

            // Tensor * scalar
            (Value::Tensor(t), Value::Float(s)) | (Value::Float(s), Value::Tensor(t)) if *op == BinOp::Mul => {
                Ok(Value::Tensor(t.scale(*s)))
            }

            _ => Err(runtime_err(format!(
                "Cannot apply `{op}` to {} and {}",
                left.type_name(),
                right.type_name()
            ))),
        }
    }

    fn call_function(&mut self, func: &Value, args: &[Value]) -> Result<Value, TlError> {
        match func {
            Value::Builtin(name) => self.call_builtin(name, args),
            Value::Function {
                params, body, ..
            } => {
                if args.len() != params.len() {
                    return Err(runtime_err(format!(
                        "Expected {} arguments, got {}",
                        params.len(),
                        args.len()
                    )));
                }
                self.env.push_scope();
                for (param, arg) in params.iter().zip(args) {
                    self.env.set(param.name.clone(), arg.clone());
                }
                let mut result = Value::None;
                for stmt in body {
                    match self.exec_stmt(stmt)? {
                        Signal::Return(val) => {
                            result = val;
                            break;
                        }
                        Signal::None => {
                            if let Some(val) = &self.last_expr_value {
                                result = val.clone();
                            }
                        }
                        _ => {}
                    }
                }
                self.env.pop_scope();
                Ok(result)
            }
            Value::Closure {
                params,
                body,
                captured_env,
            } => {
                if args.len() != params.len() {
                    return Err(runtime_err(format!(
                        "Closure expected {} arguments, got {}",
                        params.len(),
                        args.len()
                    )));
                }
                // Save current env, swap in captured env
                let saved_env = std::mem::replace(&mut self.env.scopes, captured_env.clone());
                self.env.push_scope();
                for (param, arg) in params.iter().zip(args) {
                    self.env.set(param.name.clone(), arg.clone());
                }
                let result = self.eval_expr(body);
                // Restore original env
                self.env.scopes = saved_env;
                result
            }
            _ => Err(runtime_err(format!(
                "Cannot call {}",
                func.type_name()
            ))),
        }
    }

    fn call_builtin(&mut self, name: &str, args: &[Value]) -> Result<Value, TlError> {
        match name {
            "print" | "println" => {
                // If any arg is a table, auto-collect and display it
                let mut parts = Vec::new();
                for a in args {
                    match a {
                        Value::Table(t) => {
                            let batches = self.engine().collect(t.df.clone()).map_err(|e| runtime_err(e))?;
                            let formatted = DataEngine::format_batches(&batches).map_err(|e| runtime_err(e))?;
                            parts.push(formatted);
                        }
                        _ => parts.push(format!("{a}")),
                    }
                }
                let line = parts.join(" ");
                println!("{line}");
                self.output.push(line);
                Ok(Value::None)
            }
            "len" => match args.first() {
                Some(Value::String(s)) => Ok(Value::Int(s.len() as i64)),
                Some(Value::List(l)) => Ok(Value::Int(l.len() as i64)),
                _ => Err(runtime_err("len() expects a string or list".to_string())),
            },
            "str" => Ok(Value::String(
                args.first().map(|v| format!("{v}")).unwrap_or_default(),
            )),
            "int" => match args.first() {
                Some(Value::Float(f)) => Ok(Value::Int(*f as i64)),
                Some(Value::String(s)) => s
                    .parse::<i64>()
                    .map(Value::Int)
                    .map_err(|_| runtime_err(format!("Cannot convert '{s}' to int"))),
                Some(Value::Int(n)) => Ok(Value::Int(*n)),
                _ => Err(runtime_err("int() expects a number or string".to_string())),
            },
            "float" => match args.first() {
                Some(Value::Int(n)) => Ok(Value::Float(*n as f64)),
                Some(Value::String(s)) => s
                    .parse::<f64>()
                    .map(Value::Float)
                    .map_err(|_| runtime_err(format!("Cannot convert '{s}' to float"))),
                Some(Value::Float(n)) => Ok(Value::Float(*n)),
                _ => Err(runtime_err("float() expects a number or string".to_string())),
            },
            "abs" => match args.first() {
                Some(Value::Int(n)) => Ok(Value::Int(n.abs())),
                Some(Value::Float(n)) => Ok(Value::Float(n.abs())),
                _ => Err(runtime_err("abs() expects a number".to_string())),
            },
            "min" => {
                if args.len() == 2 {
                    match (&args[0], &args[1]) {
                        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(*a.min(b))),
                        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a.min(*b))),
                        _ => Err(runtime_err("min() expects two numbers".to_string())),
                    }
                } else {
                    Err(runtime_err("min() expects 2 arguments".to_string()))
                }
            }
            "max" => {
                if args.len() == 2 {
                    match (&args[0], &args[1]) {
                        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(*a.max(b))),
                        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a.max(*b))),
                        _ => Err(runtime_err("max() expects two numbers".to_string())),
                    }
                } else {
                    Err(runtime_err("max() expects 2 arguments".to_string()))
                }
            }
            "range" => {
                if args.len() == 1 {
                    if let Value::Int(n) = &args[0] {
                        Ok(Value::List((0..*n).map(Value::Int).collect()))
                    } else {
                        Err(runtime_err("range() expects an integer".to_string()))
                    }
                } else if args.len() == 2 {
                    if let (Value::Int(start), Value::Int(end)) = (&args[0], &args[1]) {
                        Ok(Value::List((*start..*end).map(Value::Int).collect()))
                    } else {
                        Err(runtime_err("range() expects integers".to_string()))
                    }
                } else {
                    Err(runtime_err("range() expects 1 or 2 arguments".to_string()))
                }
            }
            "push" => {
                if args.len() == 2 {
                    if let Value::List(mut items) = args[0].clone() {
                        items.push(args[1].clone());
                        Ok(Value::List(items))
                    } else {
                        Err(runtime_err("push() first arg must be a list".to_string()))
                    }
                } else {
                    Err(runtime_err("push() expects 2 arguments".to_string()))
                }
            }
            "type_of" => Ok(Value::String(
                args.first()
                    .map(|v| v.type_name().to_string())
                    .unwrap_or_else(|| "none".to_string()),
            )),
            "map" => {
                if args.len() != 2 {
                    return Err(runtime_err("map() expects 2 arguments (list, fn)".to_string()));
                }
                let items = match &args[0] {
                    Value::List(items) => items.clone(),
                    _ => return Err(runtime_err("map() first arg must be a list".to_string())),
                };
                let func = args[1].clone();
                let mut result = Vec::new();
                for item in items {
                    result.push(self.call_function(&func, &[item])?);
                }
                Ok(Value::List(result))
            }
            "filter" => {
                if args.len() != 2 {
                    return Err(runtime_err("filter() expects 2 arguments (list, fn)".to_string()));
                }
                let items = match &args[0] {
                    Value::List(items) => items.clone(),
                    _ => return Err(runtime_err("filter() first arg must be a list".to_string())),
                };
                let func = args[1].clone();
                let mut result = Vec::new();
                for item in items {
                    let val = self.call_function(&func, &[item.clone()])?;
                    if val.is_truthy() {
                        result.push(item);
                    }
                }
                Ok(Value::List(result))
            }
            "reduce" => {
                if args.len() != 3 {
                    return Err(runtime_err("reduce() expects 3 arguments (list, init, fn)".to_string()));
                }
                let items = match &args[0] {
                    Value::List(items) => items.clone(),
                    _ => return Err(runtime_err("reduce() first arg must be a list".to_string())),
                };
                let mut acc = args[1].clone();
                let func = args[2].clone();
                for item in items {
                    acc = self.call_function(&func, &[acc, item])?;
                }
                Ok(acc)
            }
            "sum" => {
                if args.len() != 1 {
                    return Err(runtime_err("sum() expects 1 argument (list)".to_string()));
                }
                let items = match &args[0] {
                    Value::List(items) => items.clone(),
                    _ => return Err(runtime_err("sum() expects a list".to_string())),
                };
                let mut total: i64 = 0;
                let mut is_float = false;
                let mut total_f: f64 = 0.0;
                for item in &items {
                    match item {
                        Value::Int(n) => {
                            if is_float {
                                total_f += *n as f64;
                            } else {
                                total += n;
                            }
                        }
                        Value::Float(n) => {
                            if !is_float {
                                total_f = total as f64;
                                is_float = true;
                            }
                            total_f += n;
                        }
                        _ => return Err(runtime_err("sum() list must contain numbers".to_string())),
                    }
                }
                if is_float {
                    Ok(Value::Float(total_f))
                } else {
                    Ok(Value::Int(total))
                }
            }
            "any" => {
                if args.len() != 2 {
                    return Err(runtime_err("any() expects 2 arguments (list, fn)".to_string()));
                }
                let items = match &args[0] {
                    Value::List(items) => items.clone(),
                    _ => return Err(runtime_err("any() first arg must be a list".to_string())),
                };
                let func = args[1].clone();
                for item in items {
                    let val = self.call_function(&func, &[item])?;
                    if val.is_truthy() {
                        return Ok(Value::Bool(true));
                    }
                }
                Ok(Value::Bool(false))
            }
            "all" => {
                if args.len() != 2 {
                    return Err(runtime_err("all() expects 2 arguments (list, fn)".to_string()));
                }
                let items = match &args[0] {
                    Value::List(items) => items.clone(),
                    _ => return Err(runtime_err("all() first arg must be a list".to_string())),
                };
                let func = args[1].clone();
                for item in items {
                    let val = self.call_function(&func, &[item])?;
                    if !val.is_truthy() {
                        return Ok(Value::Bool(false));
                    }
                }
                Ok(Value::Bool(true))
            }
            // ── Data engine builtins ──
            "read_csv" => {
                if args.len() != 1 {
                    return Err(runtime_err("read_csv() expects 1 argument (path)".into()));
                }
                let path = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err("read_csv() path must be a string".into())),
                };
                let df = self.engine().read_csv(&path).map_err(|e| runtime_err(e))?;
                Ok(Value::Table(TlTable { df }))
            }
            "read_parquet" => {
                if args.len() != 1 {
                    return Err(runtime_err("read_parquet() expects 1 argument (path)".into()));
                }
                let path = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err("read_parquet() path must be a string".into())),
                };
                let df = self.engine().read_parquet(&path).map_err(|e| runtime_err(e))?;
                Ok(Value::Table(TlTable { df }))
            }
            "write_csv" => {
                if args.len() != 2 {
                    return Err(runtime_err("write_csv() expects 2 arguments (table, path)".into()));
                }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("write_csv() first arg must be a table".into())),
                };
                let path = match &args[1] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err("write_csv() path must be a string".into())),
                };
                self.engine().write_csv(df, &path).map_err(|e| runtime_err(e))?;
                Ok(Value::None)
            }
            "write_parquet" => {
                if args.len() != 2 {
                    return Err(runtime_err("write_parquet() expects 2 arguments (table, path)".into()));
                }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("write_parquet() first arg must be a table".into())),
                };
                let path = match &args[1] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err("write_parquet() path must be a string".into())),
                };
                self.engine().write_parquet(df, &path).map_err(|e| runtime_err(e))?;
                Ok(Value::None)
            }
            "collect" => {
                if args.len() != 1 {
                    return Err(runtime_err("collect() expects 1 argument (table)".into()));
                }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("collect() expects a table".into())),
                };
                let batches = self.engine().collect(df).map_err(|e| runtime_err(e))?;
                let formatted = DataEngine::format_batches(&batches).map_err(|e| runtime_err(e))?;
                Ok(Value::String(formatted))
            }
            "show" => {
                let df = match args.first() {
                    Some(Value::Table(t)) => t.df.clone(),
                    _ => return Err(runtime_err("show() expects a table".into())),
                };
                let limit = match args.get(1) {
                    Some(Value::Int(n)) => *n as usize,
                    None => 20,
                    _ => return Err(runtime_err("show() second arg must be an int".into())),
                };
                let limited = df.limit(0, Some(limit)).map_err(|e| runtime_err(format!("{e}")))?;
                let batches = self.engine().collect(limited).map_err(|e| runtime_err(e))?;
                let formatted = DataEngine::format_batches(&batches).map_err(|e| runtime_err(e))?;
                println!("{formatted}");
                self.output.push(formatted.clone());
                Ok(Value::None)
            }
            "describe" => {
                if args.len() != 1 {
                    return Err(runtime_err("describe() expects 1 argument (table)".into()));
                }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("describe() expects a table".into())),
                };
                let schema = df.schema();
                let mut lines = Vec::new();
                lines.push("Columns:".to_string());
                for (qualifier, field) in schema.iter() {
                    let prefix = match qualifier {
                        Some(q) => format!("{q}."),
                        None => String::new(),
                    };
                    lines.push(format!("  {}{}: {}", prefix, field.name(), field.data_type()));
                }
                let output = lines.join("\n");
                println!("{output}");
                self.output.push(output.clone());
                Ok(Value::String(output))
            }
            "head" => {
                if args.is_empty() {
                    return Err(runtime_err("head() expects at least 1 argument (table)".into()));
                }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("head() first arg must be a table".into())),
                };
                let n = match args.get(1) {
                    Some(Value::Int(n)) => *n as usize,
                    None => 10,
                    _ => return Err(runtime_err("head() second arg must be an int".into())),
                };
                let limited = df.limit(0, Some(n)).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(Value::Table(TlTable { df: limited }))
            }
            "postgres" => {
                if args.len() != 2 {
                    return Err(runtime_err("postgres() expects 2 arguments (conn_str, table_name)".into()));
                }
                let conn_str = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err("postgres() conn_str must be a string".into())),
                };
                let table_name = match &args[1] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err("postgres() table_name must be a string".into())),
                };
                let df = self.engine().read_postgres(&conn_str, &table_name)
                    .map_err(|e| runtime_err(e))?;
                Ok(Value::Table(TlTable { df }))
            }
            // ── AI builtins ──
            "tensor" => self.builtin_tensor(args),
            "tensor_zeros" => self.builtin_tensor_zeros(args),
            "tensor_ones" => self.builtin_tensor_ones(args),
            "tensor_shape" => self.builtin_tensor_shape(args),
            "tensor_reshape" => self.builtin_tensor_reshape(args),
            "tensor_transpose" => self.builtin_tensor_transpose(args),
            "tensor_sum" => self.builtin_tensor_sum(args),
            "tensor_mean" => self.builtin_tensor_mean(args),
            "tensor_dot" => self.builtin_tensor_dot(args),
            "predict" => self.builtin_predict(args),
            "similarity" => self.builtin_similarity(args),
            "ai_complete" => self.builtin_ai_complete(args),
            "ai_chat" => self.builtin_ai_chat(args),
            "model_save" => self.builtin_model_save(args),
            "model_load" => self.builtin_model_load(args),
            "model_register" => self.builtin_model_register(args),
            "model_list" => self.builtin_model_list(args),
            "model_get" => self.builtin_model_get(args),
            "embed" | _ if name == "embed" => {
                Err(runtime_err("embed() requires an API key. Set TL_OPENAI_KEY env var.".to_string()))
            }
            // Streaming builtins
            "alert_slack" => {
                if args.len() != 2 {
                    return Err(runtime_err("alert_slack(url, message) requires 2 args".to_string()));
                }
                let url = match &args[0] { Value::String(s) => s.clone(), _ => return Err(runtime_err("alert_slack: url must be a string".to_string())) };
                let msg = match &args[1] { Value::String(s) => s.clone(), _ => format!("{}", args[1]) };
                tl_stream::send_alert(&tl_stream::AlertTarget::Slack(url), &msg)
                    .map_err(|e| runtime_err(e))?;
                Ok(Value::None)
            }
            "alert_webhook" => {
                if args.len() != 2 {
                    return Err(runtime_err("alert_webhook(url, message) requires 2 args".to_string()));
                }
                let url = match &args[0] { Value::String(s) => s.clone(), _ => return Err(runtime_err("alert_webhook: url must be a string".to_string())) };
                let msg = match &args[1] { Value::String(s) => s.clone(), _ => format!("{}", args[1]) };
                tl_stream::send_alert(&tl_stream::AlertTarget::Webhook(url), &msg)
                    .map_err(|e| runtime_err(e))?;
                Ok(Value::None)
            }
            "emit" => {
                // emit(value) — output a value in a stream context
                if args.is_empty() {
                    return Err(runtime_err("emit() requires at least 1 argument".to_string()));
                }
                let val = &args[0];
                self.output.push(format!("emit: {val}"));
                Ok(val.clone())
            }
            "lineage" => {
                // lineage() — create a new lineage tracker
                // For now, return a string representation
                Ok(Value::String("lineage_tracker".to_string()))
            }
            "run_pipeline" => {
                if args.is_empty() {
                    return Err(runtime_err("run_pipeline() requires a pipeline name".to_string()));
                }
                if let Value::Pipeline(ref def) = args[0] {
                    Ok(Value::String(format!("Pipeline '{}' triggered", def.name)))
                } else {
                    Err(runtime_err("run_pipeline: argument must be a pipeline".to_string()))
                }
            }
            _ => Err(runtime_err(format!("Unknown builtin: {name}"))),
        }
    }

    /// Simple string interpolation: replace {expr} with evaluated value
    fn interpolate_string(&mut self, s: &str) -> Result<String, TlError> {
        let mut result = String::new();
        let mut chars = s.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '{' {
                let mut expr_str = String::new();
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
                    expr_str.push(c);
                }
                // Look up the variable in the environment
                if let Some(val) = self.env.get(&expr_str) {
                    result.push_str(&format!("{val}"));
                } else {
                    result.push('{');
                    result.push_str(&expr_str);
                    result.push('}');
                }
            } else if ch == '\\' {
                // Handle escape sequences
                match chars.next() {
                    Some('n') => result.push('\n'),
                    Some('t') => result.push('\t'),
                    Some('\\') => result.push('\\'),
                    Some('"') => result.push('"'),
                    Some(c) => {
                        result.push('\\');
                        result.push(c);
                    }
                    None => result.push('\\'),
                }
            } else {
                result.push(ch);
            }
        }
        Ok(result)
    }

    // ── Table-aware pipe evaluation ─────────────────────────

    /// Evaluate `table |> operation(args)` — dispatches to table operations.
    fn eval_table_pipe(&mut self, df: DataFrame, right: &Expr) -> Result<Value, TlError> {
        match right {
            Expr::Call { function, args } => {
                let fname = match function.as_ref() {
                    Expr::Ident(name) => name.as_str(),
                    _ => {
                        // Fall through to regular call with table as first arg
                        let func = self.eval_expr(function)?;
                        let mut all_args = vec![Value::Table(TlTable { df })];
                        for arg in args {
                            all_args.push(self.eval_expr(arg)?);
                        }
                        return self.call_function(&func, &all_args);
                    }
                };
                match fname {
                    "filter" => self.table_filter(df, args),
                    "select" => self.table_select(df, args),
                    "sort" => self.table_sort(df, args),
                    "with" => self.table_with(df, args),
                    "aggregate" => self.table_aggregate(df, args),
                    "join" => self.table_join(df, args),
                    "head" => self.table_limit(df, args),
                    "limit" => self.table_limit(df, args),
                    "collect" => {
                        let batches = self.engine().collect(df).map_err(|e| runtime_err(e))?;
                        let formatted = DataEngine::format_batches(&batches).map_err(|e| runtime_err(e))?;
                        Ok(Value::String(formatted))
                    }
                    "show" => {
                        let limit = match args.first() {
                            Some(expr) => {
                                let val = self.eval_expr(expr)?;
                                match val {
                                    Value::Int(n) => n as usize,
                                    _ => 20,
                                }
                            }
                            None => 20,
                        };
                        let limited = df.limit(0, Some(limit)).map_err(|e| runtime_err(format!("{e}")))?;
                        let batches = self.engine().collect(limited).map_err(|e| runtime_err(e))?;
                        let formatted = DataEngine::format_batches(&batches).map_err(|e| runtime_err(e))?;
                        println!("{formatted}");
                        self.output.push(formatted);
                        Ok(Value::None)
                    }
                    "describe" => {
                        let schema = df.schema();
                        let mut lines = Vec::new();
                        lines.push("Columns:".to_string());
                        for field in schema.fields() {
                            lines.push(format!("  {}: {}", field.name(), field.data_type()));
                        }
                        let output = lines.join("\n");
                        println!("{output}");
                        self.output.push(output.clone());
                        Ok(Value::String(output))
                    }
                    "write_csv" => {
                        if args.len() != 1 {
                            return Err(runtime_err("write_csv() expects 1 argument (path)".into()));
                        }
                        let path = match self.eval_expr(&args[0])? {
                            Value::String(s) => s,
                            _ => return Err(runtime_err("write_csv() path must be a string".into())),
                        };
                        self.engine().write_csv(df, &path).map_err(|e| runtime_err(e))?;
                        Ok(Value::None)
                    }
                    "write_parquet" => {
                        if args.len() != 1 {
                            return Err(runtime_err("write_parquet() expects 1 argument (path)".into()));
                        }
                        let path = match self.eval_expr(&args[0])? {
                            Value::String(s) => s,
                            _ => return Err(runtime_err("write_parquet() path must be a string".into())),
                        };
                        self.engine().write_parquet(df, &path).map_err(|e| runtime_err(e))?;
                        Ok(Value::None)
                    }
                    // Unknown table op: fall through to regular call
                    _ => {
                        let func = self.env.get(fname).cloned().ok_or_else(|| {
                            runtime_err(format!("Unknown table operation: `{fname}`"))
                        })?;
                        let mut all_args = vec![Value::Table(TlTable { df })];
                        for arg in args {
                            all_args.push(self.eval_expr(arg)?);
                        }
                        self.call_function(&func, &all_args)
                    }
                }
            }
            Expr::Ident(name) => {
                let func = self.env.get(name).cloned().ok_or_else(|| {
                    runtime_err(format!("Unknown table operation: `{name}`"))
                })?;
                self.call_function(&func, &[Value::Table(TlTable { df })])
            }
            _ => Err(runtime_err("Right side of |> must be a function call".into())),
        }
    }

    /// Build a TranslateContext from current interpreter locals.
    fn build_translate_context(&self) -> TranslateContext {
        let mut ctx = TranslateContext::new();
        for scope in &self.env.scopes {
            for (name, val) in scope {
                let local = match val {
                    Value::Int(n) => Some(LocalValue::Int(*n)),
                    Value::Float(f) => Some(LocalValue::Float(*f)),
                    Value::String(s) => Some(LocalValue::String(s.clone())),
                    Value::Bool(b) => Some(LocalValue::Bool(*b)),
                    _ => None,
                };
                if let Some(local) = local {
                    ctx.locals.insert(name.clone(), local);
                }
            }
        }
        ctx
    }

    /// `table |> filter(predicate)`
    fn table_filter(&mut self, df: DataFrame, args: &[Expr]) -> Result<Value, TlError> {
        if args.len() != 1 {
            return Err(runtime_err("filter() expects 1 argument (predicate)".into()));
        }
        let ctx = self.build_translate_context();
        let pred = translate_expr(&args[0], &ctx).map_err(|e| runtime_err(e))?;
        let filtered = df.filter(pred).map_err(|e| runtime_err(format!("{e}")))?;
        Ok(Value::Table(TlTable { df: filtered }))
    }

    /// `table |> select(col1, col2, name: expr)`
    fn table_select(&mut self, df: DataFrame, args: &[Expr]) -> Result<Value, TlError> {
        if args.is_empty() {
            return Err(runtime_err("select() expects at least 1 argument".into()));
        }
        let ctx = self.build_translate_context();
        let mut select_exprs = Vec::new();
        for arg in args {
            match arg {
                Expr::Ident(name) => {
                    select_exprs.push(col(name.as_str()));
                }
                Expr::NamedArg { name, value } => {
                    let expr = translate_expr(value, &ctx).map_err(|e| runtime_err(e))?;
                    select_exprs.push(expr.alias(name));
                }
                Expr::String(name) => {
                    select_exprs.push(col(name.as_str()));
                }
                _ => {
                    let expr = translate_expr(arg, &ctx).map_err(|e| runtime_err(e))?;
                    select_exprs.push(expr);
                }
            }
        }
        let selected = df.select(select_exprs).map_err(|e| runtime_err(format!("{e}")))?;
        Ok(Value::Table(TlTable { df: selected }))
    }

    /// `table |> sort(col, "desc")` or `table |> sort(col)` (default asc)
    fn table_sort(&mut self, df: DataFrame, args: &[Expr]) -> Result<Value, TlError> {
        if args.is_empty() {
            return Err(runtime_err("sort() expects at least 1 argument (column)".into()));
        }
        let mut sort_exprs = Vec::new();
        let mut i = 0;
        while i < args.len() {
            let col_name = match &args[i] {
                Expr::Ident(name) => name.clone(),
                Expr::String(name) => name.clone(),
                _ => return Err(runtime_err("sort() column must be an identifier or string".into())),
            };
            i += 1;
            // Check for optional "asc"/"desc" direction
            let ascending = if i < args.len() {
                match &args[i] {
                    Expr::String(dir) if dir == "desc" || dir == "DESC" => {
                        i += 1;
                        false
                    }
                    Expr::String(dir) if dir == "asc" || dir == "ASC" => {
                        i += 1;
                        true
                    }
                    _ => true,
                }
            } else {
                true
            };
            sort_exprs.push(
                col(col_name.as_str()).sort(ascending, true) // nulls last
            );
        }
        let sorted = df.sort(sort_exprs).map_err(|e| runtime_err(format!("{e}")))?;
        Ok(Value::Table(TlTable { df: sorted }))
    }

    /// `table |> with { col_name = expr, ... }` — add derived columns
    fn table_with(&mut self, df: DataFrame, args: &[Expr]) -> Result<Value, TlError> {
        if args.len() != 1 {
            return Err(runtime_err("with() expects 1 argument (map of column definitions)".into()));
        }
        let pairs = match &args[0] {
            Expr::Map(pairs) => pairs,
            _ => return Err(runtime_err("with() expects a map { col = expr, ... }".into())),
        };
        let ctx = self.build_translate_context();
        let mut result_df = df;
        for (key, value_expr) in pairs {
            let col_name = match key {
                Expr::String(s) => s.clone(),
                Expr::Ident(s) => s.clone(),
                _ => return Err(runtime_err("with() key must be a string or identifier".into())),
            };
            let df_expr = translate_expr(value_expr, &ctx).map_err(|e| runtime_err(e))?;
            result_df = result_df
                .with_column(&col_name, df_expr)
                .map_err(|e| runtime_err(format!("{e}")))?;
        }
        Ok(Value::Table(TlTable { df: result_df }))
    }

    /// `table |> aggregate(by: "col", total: sum(amount), n: count())`
    fn table_aggregate(&mut self, df: DataFrame, args: &[Expr]) -> Result<Value, TlError> {
        let ctx = self.build_translate_context();
        let mut group_by_cols: Vec<tl_data::datafusion::prelude::Expr> = Vec::new();
        let mut agg_exprs: Vec<tl_data::datafusion::prelude::Expr> = Vec::new();

        for arg in args {
            match arg {
                Expr::NamedArg { name, value } if name == "by" => {
                    // by: "col" or by: col
                    match value.as_ref() {
                        Expr::String(col_name) => {
                            group_by_cols.push(col(col_name.as_str()));
                        }
                        Expr::Ident(col_name) => {
                            group_by_cols.push(col(col_name.as_str()));
                        }
                        Expr::List(items) => {
                            for item in items {
                                match item {
                                    Expr::String(s) => group_by_cols.push(col(s.as_str())),
                                    Expr::Ident(s) => group_by_cols.push(col(s.as_str())),
                                    _ => return Err(runtime_err("by: list items must be strings or identifiers".into())),
                                }
                            }
                        }
                        _ => return Err(runtime_err("by: must be a column name or list".into())),
                    }
                }
                Expr::NamedArg { name, value } => {
                    // Named aggregate: total: sum(amount)
                    let agg_expr = translate_expr(value, &ctx).map_err(|e| runtime_err(e))?;
                    agg_exprs.push(agg_expr.alias(name));
                }
                _ => {
                    // Positional aggregate
                    let agg_expr = translate_expr(arg, &ctx).map_err(|e| runtime_err(e))?;
                    agg_exprs.push(agg_expr);
                }
            }
        }

        let aggregated = df
            .aggregate(group_by_cols, agg_exprs)
            .map_err(|e| runtime_err(format!("{e}")))?;
        Ok(Value::Table(TlTable { df: aggregated }))
    }

    /// `table |> join(right_table, on: left_col == right_col, kind: "inner")`
    fn table_join(&mut self, df: DataFrame, args: &[Expr]) -> Result<Value, TlError> {
        if args.is_empty() {
            return Err(runtime_err("join() expects at least 1 argument (right table)".into()));
        }

        // First positional arg: right table (evaluate it)
        let right_table = self.eval_expr(&args[0])?;
        let right_df = match right_table {
            Value::Table(t) => t.df,
            _ => return Err(runtime_err("join() first arg must be a table".into())),
        };

        let mut left_cols: Vec<&str> = Vec::new();
        let mut right_cols: Vec<&str> = Vec::new();
        let mut join_type = JoinType::Inner;
        let mut on_col_names: Vec<(String, String)> = Vec::new();

        for arg in &args[1..] {
            match arg {
                Expr::NamedArg { name, value } if name == "on" => {
                    // on: left_col == right_col
                    match value.as_ref() {
                        Expr::BinOp { left, op: BinOp::Eq, right } => {
                            let left_col = match left.as_ref() {
                                Expr::Ident(s) => s.clone(),
                                Expr::String(s) => s.clone(),
                                _ => return Err(runtime_err("on: left side must be a column name".into())),
                            };
                            let right_col = match right.as_ref() {
                                Expr::Ident(s) => s.clone(),
                                Expr::String(s) => s.clone(),
                                _ => return Err(runtime_err("on: right side must be a column name".into())),
                            };
                            on_col_names.push((left_col, right_col));
                        }
                        _ => return Err(runtime_err("on: must be an equality expression (col1 == col2)".into())),
                    }
                }
                Expr::NamedArg { name, value } if name == "kind" => {
                    let kind_val = self.eval_expr(value)?;
                    let kind_str = match &kind_val {
                        Value::String(s) => s.as_str(),
                        _ => return Err(runtime_err("kind: must be a string".into())),
                    };
                    join_type = match kind_str {
                        "inner" => JoinType::Inner,
                        "left" => JoinType::Left,
                        "right" => JoinType::Right,
                        "full" => JoinType::Full,
                        _ => return Err(runtime_err(format!("Unknown join type: {kind_str}"))),
                    };
                }
                _ => {} // ignore other args
            }
        }

        // Build column references
        for (l, r) in &on_col_names {
            left_cols.push(l.as_str());
            right_cols.push(r.as_str());
        }

        let joined = df
            .join(right_df, join_type, &left_cols, &right_cols, None)
            .map_err(|e| runtime_err(format!("{e}")))?;
        Ok(Value::Table(TlTable { df: joined }))
    }

    /// `table |> head(n)` or `table |> limit(n)`
    fn table_limit(&mut self, df: DataFrame, args: &[Expr]) -> Result<Value, TlError> {
        let n = match args.first() {
            Some(expr) => {
                let val = self.eval_expr(expr)?;
                match val {
                    Value::Int(n) => n as usize,
                    _ => return Err(runtime_err("head/limit expects an integer".into())),
                }
            }
            None => 10,
        };
        let limited = df.limit(0, Some(n)).map_err(|e| runtime_err(format!("{e}")))?;
        Ok(Value::Table(TlTable { df: limited }))
    }
}

/// Convert TL type annotations to Arrow DataTypes.
fn tl_type_to_arrow(ty: &TypeExpr) -> ArrowDataType {
    match ty {
        TypeExpr::Named(name) => match name.as_str() {
            "int64" | "int" => ArrowDataType::Int64,
            "int32" => ArrowDataType::Int32,
            "int16" => ArrowDataType::Int16,
            "float64" | "float" => ArrowDataType::Float64,
            "float32" => ArrowDataType::Float32,
            "string" | "str" | "text" => ArrowDataType::Utf8,
            "bool" | "boolean" => ArrowDataType::Boolean,
            _ => ArrowDataType::Utf8, // fallback
        },
        TypeExpr::Optional(inner) => tl_type_to_arrow(inner), // nullable is always true in Arrow
        _ => ArrowDataType::Utf8, // fallback for complex types
    }
}

fn runtime_err(message: String) -> TlError {
    TlError::Runtime(RuntimeError {
        message,
        span: None,
    })
}

// ── AI builtin implementations ──────────────────────────

impl Interpreter {
    fn value_to_f64_list(&self, v: &Value) -> Result<Vec<f64>, TlError> {
        match v {
            Value::List(items) => {
                let mut result = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Value::Int(n) => result.push(*n as f64),
                        Value::Float(f) => result.push(*f),
                        _ => return Err(runtime_err(format!("Expected number in list, got {}", item.type_name()))),
                    }
                }
                Ok(result)
            }
            _ => Err(runtime_err(format!("Expected list, got {}", v.type_name()))),
        }
    }

    fn value_to_usize_list(&self, v: &Value) -> Result<Vec<usize>, TlError> {
        match v {
            Value::List(items) => {
                let mut result = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Value::Int(n) => result.push(*n as usize),
                        _ => return Err(runtime_err(format!("Expected int in shape, got {}", item.type_name()))),
                    }
                }
                Ok(result)
            }
            _ => Err(runtime_err(format!("Expected list for shape, got {}", v.type_name()))),
        }
    }

    fn builtin_tensor(&mut self, args: &[Value]) -> Result<Value, TlError> {
        match args.first() {
            Some(Value::List(_)) => {
                let data = self.value_to_f64_list(&args[0])?;
                if args.len() > 1 {
                    let shape = self.value_to_usize_list(&args[1])?;
                    let t = tl_ai::TlTensor::from_vec(data, &shape)
                        .map_err(|e| runtime_err(e))?;
                    Ok(Value::Tensor(t))
                } else {
                    Ok(Value::Tensor(tl_ai::TlTensor::from_list(data)))
                }
            }
            _ => Err(runtime_err("tensor() expects a list of numbers".to_string())),
        }
    }

    fn builtin_tensor_zeros(&mut self, args: &[Value]) -> Result<Value, TlError> {
        let shape = self.value_to_usize_list(args.first().ok_or_else(|| runtime_err("tensor_zeros() expects a shape".to_string()))?)?;
        Ok(Value::Tensor(tl_ai::TlTensor::zeros(&shape)))
    }

    fn builtin_tensor_ones(&mut self, args: &[Value]) -> Result<Value, TlError> {
        let shape = self.value_to_usize_list(args.first().ok_or_else(|| runtime_err("tensor_ones() expects a shape".to_string()))?)?;
        Ok(Value::Tensor(tl_ai::TlTensor::ones(&shape)))
    }

    fn builtin_tensor_shape(&mut self, args: &[Value]) -> Result<Value, TlError> {
        match args.first() {
            Some(Value::Tensor(t)) => {
                let shape = t.shape();
                Ok(Value::List(shape.into_iter().map(|s| Value::Int(s as i64)).collect()))
            }
            _ => Err(runtime_err("tensor_shape() expects a tensor".to_string())),
        }
    }

    fn builtin_tensor_reshape(&mut self, args: &[Value]) -> Result<Value, TlError> {
        if args.len() != 2 {
            return Err(runtime_err("tensor_reshape() expects (tensor, shape)".to_string()));
        }
        match &args[0] {
            Value::Tensor(t) => {
                let shape = self.value_to_usize_list(&args[1])?;
                let reshaped = t.reshape(&shape).map_err(|e| runtime_err(e))?;
                Ok(Value::Tensor(reshaped))
            }
            _ => Err(runtime_err("tensor_reshape() expects a tensor as first argument".to_string())),
        }
    }

    fn builtin_tensor_transpose(&mut self, args: &[Value]) -> Result<Value, TlError> {
        match args.first() {
            Some(Value::Tensor(t)) => {
                let transposed = t.transpose().map_err(|e| runtime_err(e))?;
                Ok(Value::Tensor(transposed))
            }
            _ => Err(runtime_err("tensor_transpose() expects a tensor".to_string())),
        }
    }

    fn builtin_tensor_sum(&mut self, args: &[Value]) -> Result<Value, TlError> {
        match args.first() {
            Some(Value::Tensor(t)) => Ok(Value::Float(t.sum())),
            _ => Err(runtime_err("tensor_sum() expects a tensor".to_string())),
        }
    }

    fn builtin_tensor_mean(&mut self, args: &[Value]) -> Result<Value, TlError> {
        match args.first() {
            Some(Value::Tensor(t)) => Ok(Value::Float(t.mean())),
            _ => Err(runtime_err("tensor_mean() expects a tensor".to_string())),
        }
    }

    fn builtin_tensor_dot(&mut self, args: &[Value]) -> Result<Value, TlError> {
        if args.len() != 2 {
            return Err(runtime_err("tensor_dot() expects 2 tensors".to_string()));
        }
        match (&args[0], &args[1]) {
            (Value::Tensor(a), Value::Tensor(b)) => {
                let result = a.dot(b).map_err(|e| runtime_err(e))?;
                Ok(Value::Tensor(result))
            }
            _ => Err(runtime_err("tensor_dot() expects two tensors".to_string())),
        }
    }

    fn builtin_predict(&mut self, args: &[Value]) -> Result<Value, TlError> {
        if args.len() != 2 {
            return Err(runtime_err("predict() expects (model, input)".to_string()));
        }
        match (&args[0], &args[1]) {
            (Value::Model(m), Value::Tensor(t)) => {
                let result = tl_ai::predict(m, t).map_err(|e| runtime_err(e))?;
                Ok(Value::Tensor(result))
            }
            _ => Err(runtime_err("predict() expects (model, tensor)".to_string())),
        }
    }

    fn builtin_similarity(&mut self, args: &[Value]) -> Result<Value, TlError> {
        if args.len() != 2 {
            return Err(runtime_err("similarity() expects 2 tensors".to_string()));
        }
        match (&args[0], &args[1]) {
            (Value::Tensor(a), Value::Tensor(b)) => {
                let sim = tl_ai::similarity(a, b).map_err(|e| runtime_err(e))?;
                Ok(Value::Float(sim))
            }
            _ => Err(runtime_err("similarity() expects two tensors".to_string())),
        }
    }

    fn builtin_ai_complete(&mut self, args: &[Value]) -> Result<Value, TlError> {
        let prompt = match args.first() {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(runtime_err("ai_complete() expects a string prompt".to_string())),
        };
        let model = args.get(1).and_then(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        });
        let result = tl_ai::ai_complete(&prompt, model, None, None)
            .map_err(|e| runtime_err(e))?;
        Ok(Value::String(result))
    }

    fn builtin_ai_chat(&mut self, args: &[Value]) -> Result<Value, TlError> {
        let model = match args.first() {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(runtime_err("ai_chat() expects (model, system?, messages)".to_string())),
        };
        let system = args.get(1).and_then(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        });
        // Messages as list of [role, content] pairs
        let messages = match args.last() {
            Some(Value::List(msgs)) => {
                let mut result = Vec::new();
                for msg in msgs {
                    if let Value::List(pair) = msg {
                        if pair.len() == 2 {
                            if let (Value::String(role), Value::String(content)) = (&pair[0], &pair[1]) {
                                result.push((role.clone(), content.clone()));
                            }
                        }
                    }
                }
                result
            }
            _ => Vec::new(),
        };
        let result = tl_ai::ai_chat(&model, system, &messages)
            .map_err(|e| runtime_err(e))?;
        Ok(Value::String(result))
    }

    fn builtin_model_save(&mut self, args: &[Value]) -> Result<Value, TlError> {
        if args.len() != 2 {
            return Err(runtime_err("model_save() expects (model, path)".to_string()));
        }
        match (&args[0], &args[1]) {
            (Value::Model(m), Value::String(path)) => {
                m.save(std::path::Path::new(path)).map_err(|e| runtime_err(e))?;
                Ok(Value::None)
            }
            _ => Err(runtime_err("model_save() expects (model, string_path)".to_string())),
        }
    }

    fn builtin_model_load(&mut self, args: &[Value]) -> Result<Value, TlError> {
        match args.first() {
            Some(Value::String(path)) => {
                let model = tl_ai::TlModel::load(std::path::Path::new(path))
                    .map_err(|e| runtime_err(e))?;
                Ok(Value::Model(model))
            }
            _ => Err(runtime_err("model_load() expects a path string".to_string())),
        }
    }

    fn builtin_model_register(&mut self, args: &[Value]) -> Result<Value, TlError> {
        if args.len() != 2 {
            return Err(runtime_err("model_register() expects (name, model)".to_string()));
        }
        match (&args[0], &args[1]) {
            (Value::String(name), Value::Model(m)) => {
                let registry = tl_ai::ModelRegistry::default_location();
                registry.register(name, m).map_err(|e| runtime_err(e))?;
                Ok(Value::None)
            }
            _ => Err(runtime_err("model_register() expects (string, model)".to_string())),
        }
    }

    fn builtin_model_list(&mut self, _args: &[Value]) -> Result<Value, TlError> {
        let registry = tl_ai::ModelRegistry::default_location();
        let names = registry.list();
        Ok(Value::List(names.into_iter().map(Value::String).collect()))
    }

    fn builtin_model_get(&mut self, args: &[Value]) -> Result<Value, TlError> {
        match args.first() {
            Some(Value::String(name)) => {
                let registry = tl_ai::ModelRegistry::default_location();
                let model = registry.get(name).map_err(|e| runtime_err(e))?;
                Ok(Value::Model(model))
            }
            _ => Err(runtime_err("model_get() expects a model name string".to_string())),
        }
    }

    fn exec_train(&mut self, name: &str, algorithm: &str, config: &[(String, Expr)]) -> Result<Signal, TlError> {
        // Extract config values
        let mut features_val = None;
        let mut target_val = None;
        let mut feature_names = Vec::new();
        let mut target_name = String::new();

        for (key, expr) in config {
            let val = self.eval_expr(expr)?;
            match key.as_str() {
                "data" => features_val = Some(val),
                "target" => {
                    if let Value::String(s) = &val {
                        target_name = s.clone();
                    }
                    target_val = Some(val);
                }
                "features" => {
                    if let Value::List(items) = &val {
                        for item in items {
                            if let Value::String(s) = item {
                                feature_names.push(s.clone());
                            }
                        }
                    }
                }
                _ => {} // ignore unknown config keys for now
            }
        }

        // When data is a Table, extract features and target from it
        if let Some(Value::Table(ref tbl)) = features_val {
            let engine = tl_data::DataEngine::new();
            let batches = engine.collect(tbl.df.clone())
                .map_err(|e| runtime_err(e))?;
            if batches.is_empty() {
                return Err(runtime_err("train: empty dataset".to_string()));
            }
            let batch = &batches[0];
            let schema = batch.schema();

            // Determine feature columns
            if feature_names.is_empty() {
                for field in schema.fields() {
                    if field.name() != &target_name {
                        feature_names.push(field.name().clone());
                    }
                }
            }

            let n_rows = batch.num_rows();
            let n_features = feature_names.len();

            // Extract feature columns
            let mut col_data: Vec<Vec<f64>> = Vec::new();
            for col_name in &feature_names {
                let col_idx = schema.index_of(col_name)
                    .map_err(|_| runtime_err(format!("Column not found: {col_name}")))?;
                let arr = batch.column(col_idx);
                let mut vals = Vec::with_capacity(n_rows);
                Self::extract_f64_col(arr, &mut vals)?;
                col_data.push(vals);
            }

            // Convert to row-major
            let mut row_major = Vec::with_capacity(n_rows * n_features);
            for row in 0..n_rows {
                for col in &col_data {
                    row_major.push(col[row]);
                }
            }
            let features_tensor = tl_ai::TlTensor::from_vec(row_major, &[n_rows, n_features])
                .map_err(|e| runtime_err(e))?;

            // Extract target column
            let target_idx = schema.index_of(&target_name)
                .map_err(|_| runtime_err(format!("Target column not found: {target_name}")))?;
            let target_arr = batch.column(target_idx);
            let mut target_data = Vec::with_capacity(n_rows);
            Self::extract_f64_col(target_arr, &mut target_data)?;
            let target_tensor = tl_ai::TlTensor::from_list(target_data);

            let train_config = tl_ai::TrainConfig {
                features: features_tensor,
                target: target_tensor,
                feature_names,
                target_name,
                model_name: name.to_string(),
                split_ratio: 1.0,
                hyperparams: std::collections::HashMap::new(),
            };

            let model = tl_ai::train(algorithm, &train_config)
                .map_err(|e| runtime_err(e))?;

            if let Some(meta) = model.metadata() {
                let metrics_str: Vec<String> = meta.metrics.iter()
                    .map(|(k, v)| format!("{k}={v:.4}"))
                    .collect();
                if !metrics_str.is_empty() {
                    let msg = format!("Trained model '{}' ({algorithm}): {}", name, metrics_str.join(", "));
                    println!("{msg}");
                    self.output.push(msg);
                }
            }

            self.env.set(name.to_string(), Value::Model(model));
            return Ok(Signal::None);
        }

        // Convert data to tensors (non-table path)
        let features_tensor = match features_val {
            Some(Value::Tensor(t)) => t,
            Some(Value::List(items)) => {
                // Treat as 2D list of lists or flat list
                let mut all_data = Vec::new();
                let mut n_cols = 0;
                for item in &items {
                    match item {
                        Value::List(row) => {
                            if n_cols == 0 {
                                n_cols = row.len();
                            }
                            for v in row {
                                match v {
                                    Value::Int(n) => all_data.push(*n as f64),
                                    Value::Float(f) => all_data.push(*f),
                                    _ => return Err(runtime_err("Training data must be numeric".to_string())),
                                }
                            }
                        }
                        Value::Int(n) => all_data.push(*n as f64),
                        Value::Float(f) => all_data.push(*f),
                        _ => return Err(runtime_err("Training data must be numeric".to_string())),
                    }
                }
                if n_cols == 0 {
                    n_cols = 1;
                }
                let n_rows = all_data.len() / n_cols;
                tl_ai::TlTensor::from_vec(all_data, &[n_rows, n_cols])
                    .map_err(|e| runtime_err(e))?
            }
            _ => return Err(runtime_err("train requires 'data' config key".to_string())),
        };

        let target_tensor = match target_val {
            Some(Value::Tensor(t)) => t,
            Some(Value::List(items)) => {
                let data: Result<Vec<f64>, _> = items.iter().map(|v| match v {
                    Value::Int(n) => Ok(*n as f64),
                    Value::Float(f) => Ok(*f),
                    _ => Err(runtime_err("Target values must be numeric".to_string())),
                }).collect();
                tl_ai::TlTensor::from_list(data?)
            }
            Some(Value::String(_)) => {
                return Err(runtime_err("String target column requires table data. Pass data as a table.".to_string()));
            }
            _ => return Err(runtime_err("train requires 'target' config key with numeric data".to_string())),
        };

        if feature_names.is_empty() {
            let n_features = features_tensor.shape().get(1).copied().unwrap_or(1);
            feature_names = (0..n_features).map(|i| format!("x{i}")).collect();
        }

        let train_config = tl_ai::TrainConfig {
            features: features_tensor,
            target: target_tensor,
            feature_names,
            target_name,
            model_name: name.to_string(),
            split_ratio: 1.0,
            hyperparams: std::collections::HashMap::new(),
        };

        let model = tl_ai::train(algorithm, &train_config)
            .map_err(|e| runtime_err(e))?;

        // Print training metrics
        if let Some(meta) = model.metadata() {
            let metrics_str: Vec<String> = meta.metrics.iter()
                .map(|(k, v)| format!("{k}={v:.4}"))
                .collect();
            if !metrics_str.is_empty() {
                let msg = format!("Trained model '{}' ({algorithm}): {}", name, metrics_str.join(", "));
                println!("{msg}");
                self.output.push(msg);
            }
        }

        self.env.set(name.to_string(), Value::Model(model));
        Ok(Signal::None)
    }

    fn extract_f64_col(col: &Arc<dyn tl_data::datafusion::arrow::array::Array>, out: &mut Vec<f64>) -> Result<(), TlError> {
        use tl_data::datafusion::arrow::array::{Float64Array, Int64Array, Float32Array, Int32Array, Array};
        let len = col.len();
        if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
            for i in 0..len {
                out.push(if arr.is_null(i) { 0.0 } else { arr.value(i) });
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            for i in 0..len {
                out.push(if arr.is_null(i) { 0.0 } else { arr.value(i) as f64 });
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
            for i in 0..len {
                out.push(if arr.is_null(i) { 0.0 } else { arr.value(i) as f64 });
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
            for i in 0..len {
                out.push(if arr.is_null(i) { 0.0 } else { arr.value(i) as f64 });
            }
        } else {
            return Err(runtime_err("Column must be numeric (int32, int64, float32, float64)".to_string()));
        }
        Ok(())
    }
}

// ── Streaming & Pipeline execution ──────────────────────────

impl Interpreter {
    fn exec_pipeline(
        &mut self,
        name: &str,
        extract: &[Stmt],
        transform: &[Stmt],
        load: &[Stmt],
        schedule: &Option<String>,
        timeout: &Option<String>,
        retries: &Option<i64>,
        on_failure: &Option<Vec<Stmt>>,
        on_success: &Option<Vec<Stmt>>,
    ) -> Result<Signal, TlError> {
        let timeout_ms = timeout
            .as_ref()
            .and_then(|t| tl_stream::parse_duration(t).ok());

        let def = PipelineDef {
            name: name.to_string(),
            schedule: schedule.clone(),
            timeout_ms,
            retries: retries.unwrap_or(0) as u32,
        };

        let runner = PipelineRunner::new(def.clone());

        // Clone what we need for the closure
        let extract = extract.to_vec();
        let transform = transform.to_vec();
        let load = load.to_vec();

        // Run the pipeline blocks with shared scope and retry logic
        let max_attempts = def.retries + 1;
        let mut last_error = String::new();
        let mut succeeded = false;

        for _attempt in 0..max_attempts {
            // Push a shared scope for all pipeline blocks
            self.env.push_scope();
            let mut attempt_ok = true;

            // Execute extract block
            for stmt in &extract {
                match self.exec_stmt(stmt) {
                    Ok(Signal::Return(v)) => {
                        self.env.pop_scope();
                        return Ok(Signal::Return(v));
                    }
                    Err(e) => {
                        last_error = format!("{e}");
                        attempt_ok = false;
                        break;
                    }
                    _ => {}
                }
            }

            // Execute transform block
            if attempt_ok {
                for stmt in &transform {
                    match self.exec_stmt(stmt) {
                        Ok(Signal::Return(v)) => {
                            self.env.pop_scope();
                            return Ok(Signal::Return(v));
                        }
                        Err(e) => {
                            last_error = format!("{e}");
                            attempt_ok = false;
                            break;
                        }
                        _ => {}
                    }
                }
            }

            // Execute load block
            if attempt_ok {
                for stmt in &load {
                    match self.exec_stmt(stmt) {
                        Ok(Signal::Return(v)) => {
                            self.env.pop_scope();
                            return Ok(Signal::Return(v));
                        }
                        Err(e) => {
                            last_error = format!("{e}");
                            attempt_ok = false;
                            break;
                        }
                        _ => {}
                    }
                }
            }

            self.env.pop_scope();

            if attempt_ok {
                succeeded = true;
                break;
            }
        }

        if succeeded {
            if let Some(success_block) = on_success {
                self.exec_block(success_block)?;
            }
            // Store pipeline result
            let _result = tl_stream::PipelineResult {
                name: name.to_string(),
                status: PipelineStatus::Success,
                started_at: String::new(),
                ended_at: String::new(),
                rows_processed: 0,
                attempts: 1,
            };
            self.output.push(format!("Pipeline '{}': success", name));
            let _ = runner; // use the runner to suppress warnings
        } else {
            if let Some(failure_block) = on_failure {
                self.exec_block(failure_block)?;
            }
            self.output.push(format!("Pipeline '{}': failed — {}", name, last_error));
        }

        // Store pipeline def in env
        self.env.set(name.to_string(), Value::Pipeline(def));
        Ok(Signal::None)
    }

    fn exec_stream_decl(
        &mut self,
        name: &str,
        source: &Expr,
        _transform: &[Stmt],
        sink: &Option<Expr>,
        window: &Option<tl_ast::WindowSpec>,
        watermark: &Option<String>,
    ) -> Result<Signal, TlError> {
        let _source_val = self.eval_expr(source)?;

        let window_type = window.as_ref().map(|w| match w {
            tl_ast::WindowSpec::Tumbling(dur) => {
                let ms = tl_stream::parse_duration(dur).unwrap_or(0);
                tl_stream::window::WindowType::Tumbling { duration_ms: ms }
            }
            tl_ast::WindowSpec::Sliding(win, slide) => {
                let wms = tl_stream::parse_duration(win).unwrap_or(0);
                let sms = tl_stream::parse_duration(slide).unwrap_or(0);
                tl_stream::window::WindowType::Sliding { window_ms: wms, slide_ms: sms }
            }
            tl_ast::WindowSpec::Session(gap) => {
                let ms = tl_stream::parse_duration(gap).unwrap_or(0);
                tl_stream::window::WindowType::Session { gap_ms: ms }
            }
        });

        let watermark_ms = watermark
            .as_ref()
            .and_then(|w| tl_stream::parse_duration(w).ok());

        let def = StreamDef {
            name: name.to_string(),
            window: window_type,
            watermark_ms,
        };

        // Evaluate sink if provided
        if let Some(_sink_expr) = sink {
            // sink is evaluated but not used until stream is started
        }

        // Store stream definition
        self.env.set(name.to_string(), Value::Stream(def));
        self.output.push(format!("Stream '{}' declared", name));
        Ok(Signal::None)
    }

    fn exec_source_decl(
        &mut self,
        name: &str,
        connector_type: &str,
        config: &[(String, Expr)],
    ) -> Result<Signal, TlError> {
        let mut properties = std::collections::HashMap::new();
        for (key, expr) in config {
            let val = self.eval_expr(expr)?;
            properties.insert(key.clone(), format!("{val}"));
        }

        let config = ConnectorConfig {
            name: name.to_string(),
            connector_type: connector_type.to_string(),
            properties,
        };

        self.env.set(name.to_string(), Value::Connector(config));
        Ok(Signal::None)
    }

    fn exec_sink_decl(
        &mut self,
        name: &str,
        connector_type: &str,
        config: &[(String, Expr)],
    ) -> Result<Signal, TlError> {
        let mut properties = std::collections::HashMap::new();
        for (key, expr) in config {
            let val = self.eval_expr(expr)?;
            properties.insert(key.clone(), format!("{val}"));
        }

        let config = ConnectorConfig {
            name: name.to_string(),
            connector_type: connector_type.to_string(),
            properties,
        };

        self.env.set(name.to_string(), Value::Connector(config));
        Ok(Signal::None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tl_parser::parse;

    fn run(source: &str) -> Result<Value, TlError> {
        let program = parse(source)?;
        let mut interp = Interpreter::new();
        interp.execute(&program)
    }

    fn run_output(source: &str) -> Vec<String> {
        let program = parse(source).unwrap();
        let mut interp = Interpreter::new();
        interp.execute(&program).unwrap();
        interp.output
    }

    #[test]
    fn test_arithmetic() {
        assert!(matches!(run("1 + 2").unwrap(), Value::Int(3)));
        assert!(matches!(run("10 - 3").unwrap(), Value::Int(7)));
        assert!(matches!(run("4 * 5").unwrap(), Value::Int(20)));
        assert!(matches!(run("10 / 3").unwrap(), Value::Int(3)));
        assert!(matches!(run("10 % 3").unwrap(), Value::Int(1)));
        assert!(matches!(run("2 ** 10").unwrap(), Value::Int(1024)));
    }

    #[test]
    fn test_precedence() {
        assert!(matches!(run("2 + 3 * 4").unwrap(), Value::Int(14)));
        assert!(matches!(run("(2 + 3) * 4").unwrap(), Value::Int(20)));
    }

    #[test]
    fn test_let_and_variable() {
        let output = run_output("let x = 42\nprint(x)");
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_function() {
        let output = run_output(
            "fn double(n: int64) -> int64 { n * 2 }\nlet result = double(21)\nprint(result)",
        );
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_pipe() {
        let output =
            run_output("fn double(n: int64) -> int64 { n * 2 }\nlet x = 5 |> double()\nprint(x)");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_if_else() {
        let output = run_output("let x = 10\nif x > 5 { print(\"big\") } else { print(\"small\") }");
        assert_eq!(output, vec!["big"]);
    }

    #[test]
    fn test_string_interpolation() {
        let output = run_output("let name = \"TL\"\nprint(\"Hello {name}!\")");
        assert_eq!(output, vec!["Hello TL!"]);
    }

    #[test]
    fn test_list() {
        let output = run_output("let items = [1, 2, 3]\nprint(len(items))");
        assert_eq!(output, vec!["3"]);
    }

    #[test]
    fn test_comparison() {
        assert!(matches!(run("5 > 3").unwrap(), Value::Bool(true)));
        assert!(matches!(run("5 < 3").unwrap(), Value::Bool(false)));
        assert!(matches!(run("5 == 5").unwrap(), Value::Bool(true)));
    }

    #[test]
    fn test_match_int() {
        let output = run_output("let x = 2\nprint(match x { 1 => \"one\", 2 => \"two\", _ => \"other\" })");
        assert_eq!(output, vec!["two"]);
    }

    #[test]
    fn test_match_wildcard() {
        let output = run_output("let x = 99\nprint(match x { 1 => \"one\", _ => \"fallback\" })");
        assert_eq!(output, vec!["fallback"]);
    }

    #[test]
    fn test_match_string() {
        let output = run_output("let s = \"hi\"\nprint(match s { \"hello\" => 1, \"hi\" => 2, _ => 0 })");
        assert_eq!(output, vec!["2"]);
    }

    #[test]
    fn test_closure() {
        let output = run_output("let double = (x) => x * 2\nprint(double(5))");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_closure_capture() {
        let output = run_output("let factor = 3\nlet mul = (x) => x * factor\nprint(mul(7))");
        assert_eq!(output, vec!["21"]);
    }

    #[test]
    fn test_for_loop() {
        let output = run_output(
            "let sum = 0\nfor i in range(5) { sum = sum + i }\nprint(sum)",
        );
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_map_builtin() {
        let output = run_output("let nums = [1, 2, 3]\nlet doubled = map(nums, (x) => x * 2)\nprint(doubled)");
        assert_eq!(output, vec!["[2, 4, 6]"]);
    }

    #[test]
    fn test_filter_builtin() {
        let output = run_output("let nums = [1, 2, 3, 4, 5]\nlet evens = filter(nums, (x) => x % 2 == 0)\nprint(evens)");
        assert_eq!(output, vec!["[2, 4]"]);
    }

    #[test]
    fn test_pipe_with_closure() {
        let output = run_output("let result = [1, 2, 3] |> map((x) => x + 10)\nprint(result)");
        assert_eq!(output, vec!["[11, 12, 13]"]);
    }

    #[test]
    fn test_sum_builtin() {
        let output = run_output("print(sum([1, 2, 3, 4]))");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_reduce_builtin() {
        let output = run_output("let product = reduce([1, 2, 3, 4], 1, (acc, x) => acc * x)\nprint(product)");
        assert_eq!(output, vec!["24"]);
    }
}