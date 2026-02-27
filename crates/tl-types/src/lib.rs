// ThinkingLanguage — Type System
// Licensed under MIT OR Apache-2.0
//
// Provides the internal Type representation, type environment,
// and type checker for gradual static typing.

pub mod checker;
pub mod convert;
pub mod infer;

use std::fmt;

/// Internal type representation used by the type checker.
/// Separate from `TypeExpr` (AST surface syntax).
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    /// Gradual typing: compatible with everything
    Any,
    /// Void return type
    Unit,
    /// Primitive types
    Int,
    Float,
    String,
    Bool,
    None,
    /// Composite types
    List(Box<Type>),
    Map(Box<Type>),
    Set(Box<Type>),
    /// Option: T or None
    Option(Box<Type>),
    /// Result: Ok(T) or Err(E)
    Result(Box<Type>, Box<Type>),
    /// Function type
    Function {
        params: Vec<Type>,
        ret: Box<Type>,
    },
    /// Named struct type
    Struct(std::string::String),
    /// Named enum type
    Enum(std::string::String),
    /// Table (optional schema name)
    Table(Option<std::string::String>),
    /// Generator yielding T
    Generator(Box<Type>),
    /// Task returning T
    Task(Box<Type>),
    /// Channel carrying T
    Channel(Box<Type>),
    /// Inference variable (unresolved)
    Var(u32),
    /// Poison type — suppresses further errors
    Error,
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Any => write!(f, "any"),
            Type::Unit => write!(f, "unit"),
            Type::Int => write!(f, "int"),
            Type::Float => write!(f, "float"),
            Type::String => write!(f, "string"),
            Type::Bool => write!(f, "bool"),
            Type::None => write!(f, "none"),
            Type::List(t) => write!(f, "list<{t}>"),
            Type::Map(t) => write!(f, "map<{t}>"),
            Type::Set(t) => write!(f, "set<{t}>"),
            Type::Option(t) => write!(f, "{t}?"),
            Type::Result(ok, err) => write!(f, "result<{ok}, {err}>"),
            Type::Function { params, ret } => {
                write!(f, "fn(")?;
                for (i, p) in params.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{p}")?;
                }
                write!(f, ") -> {ret}")
            }
            Type::Struct(name) => write!(f, "{name}"),
            Type::Enum(name) => write!(f, "{name}"),
            Type::Table(Some(name)) => write!(f, "table<{name}>"),
            Type::Table(None) => write!(f, "table"),
            Type::Generator(t) => write!(f, "generator<{t}>"),
            Type::Task(t) => write!(f, "task<{t}>"),
            Type::Channel(t) => write!(f, "channel<{t}>"),
            Type::Var(id) => write!(f, "?T{id}"),
            Type::Error => write!(f, "<error>"),
        }
    }
}

/// Type environment — tracks variable types across scopes.
pub struct TypeEnv {
    scopes: Vec<Scope>,
    /// Function signatures: name -> (param types, return type)
    functions: std::collections::HashMap<std::string::String, FnSig>,
    /// Struct definitions: name -> field types
    structs: std::collections::HashMap<std::string::String, Vec<(std::string::String, Type)>>,
    /// Enum definitions: name -> variant list
    enums: std::collections::HashMap<std::string::String, Vec<(std::string::String, Vec<Type>)>>,
    /// Next inference variable ID
    next_var: u32,
}

/// A function signature.
#[derive(Debug, Clone)]
pub struct FnSig {
    pub params: Vec<(std::string::String, Type)>,
    pub ret: Type,
}

struct Scope {
    vars: std::collections::HashMap<std::string::String, Type>,
}

impl TypeEnv {
    pub fn new() -> Self {
        TypeEnv {
            scopes: vec![Scope {
                vars: std::collections::HashMap::new(),
            }],
            functions: std::collections::HashMap::new(),
            structs: std::collections::HashMap::new(),
            enums: std::collections::HashMap::new(),
            next_var: 0,
        }
    }

    pub fn push_scope(&mut self) {
        self.scopes.push(Scope {
            vars: std::collections::HashMap::new(),
        });
    }

    pub fn pop_scope(&mut self) {
        if self.scopes.len() > 1 {
            self.scopes.pop();
        }
    }

    pub fn define(&mut self, name: std::string::String, ty: Type) {
        if let Some(scope) = self.scopes.last_mut() {
            scope.vars.insert(name, ty);
        }
    }

    pub fn lookup(&self, name: &str) -> Option<&Type> {
        for scope in self.scopes.iter().rev() {
            if let Some(ty) = scope.vars.get(name) {
                return Some(ty);
            }
        }
        None
    }

    pub fn define_fn(&mut self, name: std::string::String, sig: FnSig) {
        self.functions.insert(name, sig);
    }

    pub fn lookup_fn(&self, name: &str) -> Option<&FnSig> {
        self.functions.get(name)
    }

    pub fn define_struct(
        &mut self,
        name: std::string::String,
        fields: Vec<(std::string::String, Type)>,
    ) {
        self.structs.insert(name, fields);
    }

    pub fn lookup_struct(&self, name: &str) -> Option<&Vec<(std::string::String, Type)>> {
        self.structs.get(name)
    }

    pub fn define_enum(
        &mut self,
        name: std::string::String,
        variants: Vec<(std::string::String, Vec<Type>)>,
    ) {
        self.enums.insert(name, variants);
    }

    pub fn lookup_enum(&self, name: &str) -> Option<&Vec<(std::string::String, Vec<Type>)>> {
        self.enums.get(name)
    }

    pub fn fresh_var(&mut self) -> Type {
        let id = self.next_var;
        self.next_var += 1;
        Type::Var(id)
    }
}

impl Default for TypeEnv {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if two types are compatible under gradual typing.
/// `any` is compatible with everything. `none` is compatible with `option<T>`.
pub fn is_compatible(expected: &Type, found: &Type) -> bool {
    // Any is compatible with everything (both directions)
    if matches!(expected, Type::Any) || matches!(found, Type::Any) {
        return true;
    }
    // Error poison type suppresses further errors
    if matches!(expected, Type::Error) || matches!(found, Type::Error) {
        return true;
    }
    // Same type
    if expected == found {
        return true;
    }
    // int promotes to float
    if matches!(expected, Type::Float) && matches!(found, Type::Int) {
        return true;
    }
    // none is compatible with option<T>
    if matches!(found, Type::None) && matches!(expected, Type::Option(_)) {
        return true;
    }
    // T is compatible with option<T>
    if let Type::Option(inner) = expected {
        if is_compatible(inner, found) {
            return true;
        }
    }
    // Structural compatibility for compound types
    match (expected, found) {
        (Type::List(a), Type::List(b)) => is_compatible(a, b),
        (Type::Map(a), Type::Map(b)) => is_compatible(a, b),
        (Type::Set(a), Type::Set(b)) => is_compatible(a, b),
        (Type::Option(a), Type::Option(b)) => is_compatible(a, b),
        (Type::Result(ok1, err1), Type::Result(ok2, err2)) => {
            is_compatible(ok1, ok2) && is_compatible(err1, err2)
        }
        (Type::Generator(a), Type::Generator(b)) => is_compatible(a, b),
        (Type::Task(a), Type::Task(b)) => is_compatible(a, b),
        (Type::Channel(a), Type::Channel(b)) => is_compatible(a, b),
        (
            Type::Function {
                params: p1,
                ret: r1,
            },
            Type::Function {
                params: p2,
                ret: r2,
            },
        ) => {
            p1.len() == p2.len()
                && p1.iter().zip(p2.iter()).all(|(a, b)| is_compatible(a, b))
                && is_compatible(r1, r2)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_display() {
        assert_eq!(Type::Int.to_string(), "int");
        assert_eq!(Type::Option(Box::new(Type::Int)).to_string(), "int?");
        assert_eq!(
            Type::Result(Box::new(Type::Int), Box::new(Type::String)).to_string(),
            "result<int, string>"
        );
        assert_eq!(Type::List(Box::new(Type::Any)).to_string(), "list<any>");
    }

    #[test]
    fn test_type_equality() {
        assert_eq!(Type::Int, Type::Int);
        assert_ne!(Type::Int, Type::Float);
        assert_eq!(
            Type::List(Box::new(Type::Int)),
            Type::List(Box::new(Type::Int))
        );
        assert_ne!(
            Type::List(Box::new(Type::Int)),
            Type::List(Box::new(Type::Float))
        );
    }

    #[test]
    fn test_type_env_push_pop_scope() {
        let mut env = TypeEnv::new();
        env.define("x".into(), Type::Int);
        assert_eq!(env.lookup("x"), Some(&Type::Int));

        env.push_scope();
        env.define("y".into(), Type::String);
        assert_eq!(env.lookup("y"), Some(&Type::String));
        assert_eq!(env.lookup("x"), Some(&Type::Int)); // parent scope visible

        env.pop_scope();
        assert_eq!(env.lookup("y"), None); // y gone
        assert_eq!(env.lookup("x"), Some(&Type::Int));
    }

    #[test]
    fn test_type_env_variable_shadowing() {
        let mut env = TypeEnv::new();
        env.define("x".into(), Type::Int);
        env.push_scope();
        env.define("x".into(), Type::String);
        assert_eq!(env.lookup("x"), Some(&Type::String)); // shadowed

        env.pop_scope();
        assert_eq!(env.lookup("x"), Some(&Type::Int)); // original restored
    }

    #[test]
    fn test_compatibility_any() {
        assert!(is_compatible(&Type::Any, &Type::Int));
        assert!(is_compatible(&Type::Int, &Type::Any));
        assert!(is_compatible(&Type::Any, &Type::Any));
    }

    #[test]
    fn test_compatibility_option_none() {
        assert!(is_compatible(
            &Type::Option(Box::new(Type::Int)),
            &Type::None
        ));
        assert!(is_compatible(
            &Type::Option(Box::new(Type::Int)),
            &Type::Int
        ));
        assert!(!is_compatible(&Type::Int, &Type::None));
    }

    #[test]
    fn test_compatibility_int_float_promotion() {
        assert!(is_compatible(&Type::Float, &Type::Int));
        assert!(!is_compatible(&Type::Int, &Type::Float));
    }

    #[test]
    fn test_compatibility_error_poison() {
        assert!(is_compatible(&Type::Error, &Type::Int));
        assert!(is_compatible(&Type::Int, &Type::Error));
    }
}
