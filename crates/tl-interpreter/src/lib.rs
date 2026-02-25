// ThinkingLanguage — Tree-Walking Interpreter
// Licensed under MIT OR Apache-2.0
//
// Phase 0: Executes TL programs by walking the AST directly.
// This is slow but correct — used for REPL and initial development.
// Will be replaced by compiled execution in Phase 2.

use std::collections::HashMap;
use std::fmt;
use tl_ast::*;
use tl_errors::{RuntimeError, TlError};

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
}

impl Interpreter {
    pub fn new() -> Self {
        Self {
            env: Environment::new(),
            output: Vec::new(),
            last_expr_value: None,
        }
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
                // Pipe: left_val becomes the first argument to the right-side call
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
                        // `x |> double` is sugar for `double(x)`
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
            "print" => {
                let output: Vec<String> = args.iter().map(|a| format!("{a}")).collect();
                let line = output.join(" ");
                println!("{line}");
                self.output.push(line);
                Ok(Value::None)
            }
            "println" => {
                let output: Vec<String> = args.iter().map(|a| format!("{a}")).collect();
                let line = output.join(" ");
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
}

fn runtime_err(message: String) -> TlError {
    TlError::Runtime(RuntimeError {
        message,
        span: None,
    })
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