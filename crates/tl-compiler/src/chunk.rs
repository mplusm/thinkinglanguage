// ThinkingLanguage — Bytecode chunk (compiled function)

use std::sync::Arc;
use tl_ast::Expr as AstExpr;

/// A compiled function / top-level script.
#[derive(Debug, Clone)]
pub struct Prototype {
    /// Bytecode instructions
    pub code: Vec<u32>,
    /// Constant pool
    pub constants: Vec<Constant>,
    /// Source line for each instruction (for error reporting)
    pub lines: Vec<u32>,
    /// Number of parameters
    pub arity: u8,
    /// Number of local variable slots needed
    pub num_locals: u8,
    /// Number of registers needed
    pub num_registers: u8,
    /// Upvalue definitions (how to capture from enclosing scope)
    pub upvalue_defs: Vec<UpvalueDef>,
    /// Function name (for debugging)
    pub name: String,
    /// Whether this function contains yield (is a generator)
    pub is_generator: bool,
    /// Top-level local bindings: (name, register_index) — used for module exports
    pub top_level_locals: Vec<(String, u8)>,
}

impl Prototype {
    pub fn new(name: String) -> Self {
        Prototype {
            code: Vec::new(),
            constants: Vec::new(),
            lines: Vec::new(),
            arity: 0,
            num_locals: 0,
            num_registers: 0,
            upvalue_defs: Vec::new(),
            name,
            is_generator: false,
            top_level_locals: Vec::new(),
        }
    }

    /// Disassemble this prototype into a human-readable string.
    pub fn disassemble(&self) -> String {
        use crate::opcode::*;

        let mut out = String::new();
        let label = if self.name.is_empty() {
            "<script>"
        } else {
            &self.name
        };
        out.push_str(&format!("=== {label} ===\n"));
        out.push_str(&format!(
            "  arity={} locals={} registers={}{}\n",
            self.arity,
            self.num_locals,
            self.num_registers,
            if self.is_generator {
                " [generator]"
            } else {
                ""
            }
        ));

        let mut offset = 0usize;
        while offset < self.code.len() {
            let inst_offset = offset; // save for display
            let inst = self.code[offset];
            let op = decode_op(inst);
            let a = decode_a(inst);
            let b = decode_b(inst);
            let c = decode_c(inst);
            let bx = decode_bx(inst);
            let sbx = decode_sbx(inst);

            // Line number
            let line_num = if inst_offset < self.lines.len() {
                let ln = self.lines[inst_offset];
                if ln == 0 {
                    "----".to_string()
                } else {
                    format!("{ln:04}")
                }
            } else {
                "----".to_string()
            };

            let args = match op {
                // ABx format: constant load
                Op::LoadConst => {
                    let val = self.format_constant(bx as usize);
                    format!("R{a} = K{bx} ({val})")
                }
                // A-only: no args
                Op::LoadNone => format!("R{a}"),
                Op::LoadTrue => format!("R{a}"),
                Op::LoadFalse => format!("R{a}"),

                // AB: register copy
                Op::Move => format!("R{a} = R{b}"),

                // AB: locals
                Op::GetLocal => format!("R{a} = L{b}"),
                Op::SetLocal => format!("L{b} = R{a}"),

                // ABx: globals (constant index for name)
                Op::GetGlobal => {
                    let name = self.format_constant(bx as usize);
                    format!("R{a} = G[{name}]")
                }
                Op::SetGlobal => {
                    let name = self.format_constant(bx as usize);
                    format!("G[{name}] = R{a}")
                }

                // AB: upvalues
                Op::GetUpvalue => format!("R{a} = U{b}"),
                Op::SetUpvalue => format!("U{b} = R{a}"),

                // ABC: arithmetic/comparison/logic
                Op::Add
                | Op::Sub
                | Op::Mul
                | Op::Div
                | Op::Mod
                | Op::Pow
                | Op::Eq
                | Op::Neq
                | Op::Lt
                | Op::Gt
                | Op::Lte
                | Op::Gte
                | Op::And
                | Op::Or
                | Op::Concat => {
                    let sym = match op {
                        Op::Add => "+",
                        Op::Sub => "-",
                        Op::Mul => "*",
                        Op::Div => "/",
                        Op::Mod => "%",
                        Op::Pow => "**",
                        Op::Eq => "==",
                        Op::Neq => "!=",
                        Op::Lt => "<",
                        Op::Gt => ">",
                        Op::Lte => "<=",
                        Op::Gte => ">=",
                        Op::And => "and",
                        Op::Or => "or",
                        Op::Concat => "..",
                        _ => "?",
                    };
                    format!("R{a} = R{b} {sym} R{c}")
                }

                // AB: unary
                Op::Neg => format!("R{a} = -R{b}"),
                Op::Not => format!("R{a} = not R{b}"),

                // ABx: jumps (signed offset)
                Op::Jump => {
                    let target = offset as i32 + 1 + sbx as i32;
                    format!("-> {target:04}")
                }
                Op::JumpIfFalse => {
                    let target = offset as i32 + 1 + sbx as i32;
                    format!("R{a} ? -> {target:04}")
                }
                Op::JumpIfTrue => {
                    let target = offset as i32 + 1 + sbx as i32;
                    format!("R{a} ? -> {target:04}")
                }

                // ABC: call
                Op::Call => {
                    if c == 0 {
                        format!("R{a} = call R{b}()")
                    } else {
                        format!("R{a} = call R{b}(R{}..R{})", b + 1, b + c)
                    }
                }

                // A: return
                Op::Return => format!("R{a}"),

                // ABx: closure
                Op::Closure => {
                    let val = self.format_constant(bx as usize);
                    format!("R{a} = closure K{bx} ({val})")
                }

                // ABC: data structures
                Op::NewList => format!("R{a} = list(R{}..R{})", b, b + c),
                Op::GetIndex => format!("R{a} = R{b}[R{c}]"),
                Op::SetIndex => format!("R{b}[R{c}] = R{a}"),
                Op::NewMap => format!("R{a} = map(R{b}, {c} pairs)"),

                // ABC: table ops
                Op::TablePipe => format!("R{a} = table_pipe(K{b}, R{c})"),

                // ABx: builtin call (Bx=builtin_id, followed by extra word for arg count + first arg)
                Op::CallBuiltin => {
                    let builtin_id = decode_bx(inst);
                    let builtin_name = BuiltinId::try_from(builtin_id)
                        .map(|b| b.name())
                        .unwrap_or("<unknown>");
                    // Next instruction: A=arg_count, B=first_arg_reg
                    let (argc, first_arg) = if offset + 1 < self.code.len() {
                        let next = self.code[offset + 1];
                        (decode_a(next), decode_b(next))
                    } else {
                        (0, 0)
                    };
                    offset += 1; // skip the extra word
                    format!("R{a} = {builtin_name}(R{first_arg}, argc={argc})")
                }

                // AB: iteration
                Op::ForIter => {
                    // Next instruction has jump offset in Bx
                    let jump_word = if offset + 1 < self.code.len() {
                        self.code[offset + 1]
                    } else {
                        0
                    };
                    let jump_sbx = decode_sbx(jump_word);
                    let target = (offset + 2) as i32 + jump_sbx as i32;
                    offset += 1;
                    format!("R{b} = next(R{a}), done -> {target:04}")
                }
                Op::ForPrep => format!("R{a} = iter(R{b})"),

                // ABC: pattern matching
                Op::TestMatch => format!("R{c} = (R{a} matches R{b})"),

                // AB: null coalesce
                Op::NullCoalesce => format!("R{a} = R{a} ?? R{b}"),

                // ABC: member access
                Op::GetMember => {
                    let field = self.format_constant(c as usize);
                    format!("R{a} = R{b}.{field}")
                }

                // Interpolate: A=dest, B=template const, C=values start
                Op::Interpolate => {
                    let argc = if offset + 1 < self.code.len() {
                        decode_a(self.code[offset + 1])
                    } else {
                        0
                    };
                    offset += 1;
                    format!("R{a} = interpolate(K{b}, R{c}, argc={argc})")
                }

                // Special domain ops
                Op::Train => format!("R{a} = train(K{b}, K{c})"),
                Op::PipelineExec => format!("R{a} = pipeline(K{b}, K{c})"),
                Op::StreamExec => format!("R{a} = stream(K{b}, R{c})"),
                Op::ConnectorDecl => format!("R{a} = connector(K{b}, K{c})"),

                // Phase 5: structs, enums, methods, exceptions, imports
                Op::NewStruct => {
                    let type_name = self.format_constant(b as usize);
                    if c & 0x80 != 0 {
                        format!("R{a} = struct_decl({type_name})")
                    } else {
                        format!("R{a} = new {type_name}({c} fields)")
                    }
                }
                Op::SetMember => {
                    let field = self.format_constant(b as usize);
                    format!("R{a}.{field} = R{c}")
                }
                Op::NewEnum => {
                    let variant = self.format_constant(b as usize);
                    let argc = if offset + 1 < self.code.len() {
                        decode_a(self.code[offset + 1])
                    } else {
                        0
                    };
                    offset += 1;
                    format!("R{a} = enum {variant}(R{c}, argc={argc})")
                }
                Op::MatchEnum => {
                    let variant = self.format_constant(b as usize);
                    format!("R{c} = (R{a} is {variant})")
                }
                Op::MethodCall => {
                    let method = self.format_constant(c as usize);
                    let extra = if offset + 1 < self.code.len() {
                        let w = self.code[offset + 1];
                        let args_start = decode_a(w);
                        let arg_count = decode_b(w);
                        offset += 1;
                        format!("(R{args_start}, argc={arg_count})")
                    } else {
                        String::new()
                    };
                    format!("R{a} = R{b}.{method}{extra}")
                }
                Op::Throw => format!("throw R{a}"),
                Op::TryBegin => {
                    let target = offset as i32 + 1 + sbx as i32;
                    format!("catch -> {target:04}")
                }
                Op::TryEnd => "end_try".to_string(),
                Op::Import => {
                    let path = self.format_constant(bx as usize);
                    format!("R{a} = import({path})")
                }

                // Phase 7: concurrency
                Op::Await => format!("R{a} = await R{b}"),

                // Phase 8: generators
                Op::Yield => format!("yield R{a}"),

                // Phase 10: type system
                Op::TryPropagate => format!("R{a} = try R{b}"),

                // Phase 17: pattern matching
                Op::ExtractField => format!("R{a} = R{b}[{c}]"),
                Op::ExtractNamedField => {
                    let field_name = self.format_constant(c as usize);
                    format!("R{a} = R{b}.{field_name}")
                }

                // Phase 28: ownership & move semantics
                Op::LoadMoved => format!("R{a} = <moved>"),
                Op::MakeRef => format!("R{a} = &R{b}"),
                Op::ParallelFor => {
                    let body = self.format_constant(b as usize);
                    format!("parallel_for R{a}, {body}")
                }
                // Phase 34: AI Agent Framework
                Op::AgentExec => format!("R{a} = agent(K{b}, K{c})"),
            };

            out.push_str(&format!(
                "{line_num}  {inst_offset:04}    {:<16}{args}\n",
                op.name()
            ));

            offset += 1;
        }

        // Disassemble child prototypes
        for (i, constant) in self.constants.iter().enumerate() {
            if let Constant::Prototype(child) = constant {
                out.push_str(&format!("\n--- K{i} ---\n"));
                out.push_str(&child.disassemble());
            }
        }

        out
    }

    /// Format a constant value for display, truncating strings to 20 chars.
    fn format_constant(&self, index: usize) -> String {
        if index >= self.constants.len() {
            return format!("K{index}?");
        }
        match &self.constants[index] {
            Constant::Int(n) => format!("{n}"),
            Constant::Float(f) => format!("{f}"),
            Constant::String(s) => {
                if s.len() > 20 {
                    format!("\"{}...\"", &s[..20])
                } else {
                    format!("\"{s}\"")
                }
            }
            Constant::Prototype(p) => {
                let name = if p.name.is_empty() { "<anon>" } else { &p.name };
                format!("fn {name}")
            }
            Constant::Decimal(s) => format!("{s}d"),
            Constant::AstExpr(_) => "<ast_expr>".to_string(),
            Constant::AstExprList(_) => "<ast_expr_list>".to_string(),
        }
    }
}

/// Constant pool entry.
#[derive(Debug, Clone)]
pub enum Constant {
    Int(i64),
    Float(f64),
    String(Arc<str>),
    /// Decimal literal string — parsed to rust_decimal::Decimal at runtime
    Decimal(Arc<str>),
    /// A nested function prototype
    Prototype(Arc<Prototype>),
    /// Raw AST expression — used for table pipe operations
    /// so the VM can pass it to translate_expr at runtime
    AstExpr(Box<AstExpr>),
    /// A list of AST expressions (for table op args)
    AstExprList(Vec<AstExpr>),
}

/// How to capture an upvalue from the enclosing function.
#[derive(Debug, Clone, Copy)]
pub struct UpvalueDef {
    /// If true, capture from enclosing function's locals.
    /// If false, capture from enclosing function's upvalues.
    pub is_local: bool,
    /// Index into enclosing function's locals or upvalues.
    pub index: u8,
}

/// Builtin function identifiers — avoids string comparisons in the VM hot loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum BuiltinId {
    Print = 0,
    Println = 1,
    Len = 2,
    Str = 3,
    Int = 4,
    Float = 5,
    Abs = 6,
    Min = 7,
    Max = 8,
    Range = 9,
    Push = 10,
    TypeOf = 11,
    Map = 12,
    Filter = 13,
    Reduce = 14,
    Sum = 15,
    Any = 16,
    All = 17,
    ReadCsv = 18,
    ReadParquet = 19,
    WriteCsv = 20,
    WriteParquet = 21,
    Collect = 22,
    Show = 23,
    Describe = 24,
    Head = 25,
    Postgres = 26,
    // AI builtins
    Tensor = 27,
    TensorZeros = 28,
    TensorOnes = 29,
    TensorShape = 30,
    TensorReshape = 31,
    TensorTranspose = 32,
    TensorSum = 33,
    TensorMean = 34,
    TensorDot = 35,
    Predict = 36,
    Similarity = 37,
    AiComplete = 38,
    AiChat = 39,
    ModelSave = 40,
    ModelLoad = 41,
    ModelRegister = 42,
    ModelList = 43,
    ModelGet = 44,
    // Streaming builtins
    AlertSlack = 45,
    AlertWebhook = 46,
    Emit = 47,
    Lineage = 48,
    RunPipeline = 49,
    // Phase 5: Math builtins
    Sqrt = 50,
    Pow = 51,
    Floor = 52,
    Ceil = 53,
    Round = 54,
    Sin = 55,
    Cos = 56,
    Tan = 57,
    Log = 58,
    Log2 = 59,
    Log10 = 60,
    Join = 61,
    // Phase 5: HTTP builtins
    HttpGet = 62,
    HttpPost = 63,
    // Phase 5: Assert builtins
    Assert = 64,
    AssertEq = 65,
    // Phase 6: Stdlib & Ecosystem
    JsonParse = 66,
    JsonStringify = 67,
    MapFrom = 68,
    ReadFile = 69,
    WriteFile = 70,
    AppendFile = 71,
    FileExists = 72,
    ListDir = 73,
    EnvGet = 74,
    EnvSet = 75,
    RegexMatch = 76,
    RegexFind = 77,
    RegexReplace = 78,
    Now = 79,
    DateFormat = 80,
    DateParse = 81,
    Zip = 82,
    Enumerate = 83,
    Bool = 84,
    // Phase 7: Concurrency
    Spawn = 85,
    Sleep = 86,
    Channel = 87,
    Send = 88,
    Recv = 89,
    TryRecv = 90,
    AwaitAll = 91,
    Pmap = 92,
    Timeout = 93,
    // Phase 8: Iterators & Generators
    Next = 94,
    IsGenerator = 95,
    Iter = 96,
    Take = 97,
    Skip_ = 98,
    GenCollect = 99,
    GenMap = 100,
    GenFilter = 101,
    Chain = 102,
    GenZip = 103,
    GenEnumerate = 104,
    // Phase 10: Type system
    Ok = 105,
    Err_ = 106,
    IsOk = 107,
    IsErr = 108,
    Unwrap = 109,
    SetFrom = 110,
    SetAdd = 111,
    SetRemove = 112,
    SetContains = 113,
    SetUnion = 114,
    SetIntersection = 115,
    SetDifference = 116,
    // Phase 15: Data Quality & Connectors
    FillNull = 117,
    DropNull = 118,
    Dedup = 119,
    Clamp = 120,
    DataProfile = 121,
    RowCount = 122,
    NullRate = 123,
    IsUnique = 124,
    IsEmail = 125,
    IsUrl = 126,
    IsPhone = 127,
    IsBetween = 128,
    Levenshtein = 129,
    Soundex = 130,
    ReadMysql = 131,
    RedisConnect = 132,
    RedisGet = 133,
    RedisSet = 134,
    RedisDel = 135,
    GraphqlQuery = 136,
    RegisterS3 = 137,
    // Phase 20: Python FFI
    PyImport = 138,
    PyCall = 139,
    PyEval = 140,
    PyGetAttr = 141,
    PySetAttr = 142,
    PyToTl = 143,
    // Phase 21: Schema Evolution
    SchemaRegister = 144,
    SchemaGet = 145,
    SchemaLatest = 146,
    SchemaHistory = 147,
    SchemaCheck = 148,
    SchemaDiff = 149,
    SchemaApplyMigration = 150,
    SchemaVersions = 151,
    SchemaFields = 152,
    // Phase 22: Advanced Types
    Decimal = 153,
    // Phase 23: Security & Access Control
    SecretGet = 154,
    SecretSet = 155,
    SecretDelete = 156,
    SecretList = 157,
    CheckPermission = 158,
    MaskEmail = 159,
    MaskPhone = 160,
    MaskCreditCard = 161,
    Redact = 162,
    Hash = 163,
    // Phase 24: Async/Await
    AsyncReadFile = 164,
    AsyncWriteFile = 165,
    AsyncHttpGet = 166,
    AsyncHttpPost = 167,
    AsyncSleep = 168,
    Select = 169,
    AsyncMap = 170,
    AsyncFilter = 171,
    RaceAll = 172,
    // Phase 27: Data Error Hierarchy
    IsError = 173,
    ErrorType = 174,
    // Phase 32: GPU Tensor Support
    GpuAvailable = 175,
    ToGpu = 176,
    ToCpu = 177,
    GpuMatmul = 178,
    GpuBatchPredict = 179,
    // Phase 33: SQLite
    ReadSqlite = 180,
    WriteSqlite = 181,
    // Phase 34: AI Agent Framework
    Embed = 182,
    HttpRequest = 183,
    RunAgent = 184,
    // Phase E5: Random & Sampling
    Random = 185,
    RandomInt = 186,
    Sample = 187,
    // Phase E6: Math builtins
    Exp = 188,
    IsNan = 189,
    IsInfinite = 190,
    Sign = 191,
    // Phase E8: Table assertion
    AssertTableEq = 192,
    // Phase F1: Date/Time builtins
    Today = 193,
    DateAdd = 194,
    DateDiff = 195,
    DateTrunc = 196,
    DateExtract = 197,
    StreamAgent = 198,
    // Aliases & new builtins
    PostgresQuery = 199,
    Fold = 200,
    TlConfigResolve = 201,
}

impl TryFrom<u16> for BuiltinId {
    type Error = u16;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(BuiltinId::Print),
            1 => Ok(BuiltinId::Println),
            2 => Ok(BuiltinId::Len),
            3 => Ok(BuiltinId::Str),
            4 => Ok(BuiltinId::Int),
            5 => Ok(BuiltinId::Float),
            6 => Ok(BuiltinId::Abs),
            7 => Ok(BuiltinId::Min),
            8 => Ok(BuiltinId::Max),
            9 => Ok(BuiltinId::Range),
            10 => Ok(BuiltinId::Push),
            11 => Ok(BuiltinId::TypeOf),
            12 => Ok(BuiltinId::Map),
            13 => Ok(BuiltinId::Filter),
            14 => Ok(BuiltinId::Reduce),
            15 => Ok(BuiltinId::Sum),
            16 => Ok(BuiltinId::Any),
            17 => Ok(BuiltinId::All),
            18 => Ok(BuiltinId::ReadCsv),
            19 => Ok(BuiltinId::ReadParquet),
            20 => Ok(BuiltinId::WriteCsv),
            21 => Ok(BuiltinId::WriteParquet),
            22 => Ok(BuiltinId::Collect),
            23 => Ok(BuiltinId::Show),
            24 => Ok(BuiltinId::Describe),
            25 => Ok(BuiltinId::Head),
            26 => Ok(BuiltinId::Postgres),
            27 => Ok(BuiltinId::Tensor),
            28 => Ok(BuiltinId::TensorZeros),
            29 => Ok(BuiltinId::TensorOnes),
            30 => Ok(BuiltinId::TensorShape),
            31 => Ok(BuiltinId::TensorReshape),
            32 => Ok(BuiltinId::TensorTranspose),
            33 => Ok(BuiltinId::TensorSum),
            34 => Ok(BuiltinId::TensorMean),
            35 => Ok(BuiltinId::TensorDot),
            36 => Ok(BuiltinId::Predict),
            37 => Ok(BuiltinId::Similarity),
            38 => Ok(BuiltinId::AiComplete),
            39 => Ok(BuiltinId::AiChat),
            40 => Ok(BuiltinId::ModelSave),
            41 => Ok(BuiltinId::ModelLoad),
            42 => Ok(BuiltinId::ModelRegister),
            43 => Ok(BuiltinId::ModelList),
            44 => Ok(BuiltinId::ModelGet),
            45 => Ok(BuiltinId::AlertSlack),
            46 => Ok(BuiltinId::AlertWebhook),
            47 => Ok(BuiltinId::Emit),
            48 => Ok(BuiltinId::Lineage),
            49 => Ok(BuiltinId::RunPipeline),
            50 => Ok(BuiltinId::Sqrt),
            51 => Ok(BuiltinId::Pow),
            52 => Ok(BuiltinId::Floor),
            53 => Ok(BuiltinId::Ceil),
            54 => Ok(BuiltinId::Round),
            55 => Ok(BuiltinId::Sin),
            56 => Ok(BuiltinId::Cos),
            57 => Ok(BuiltinId::Tan),
            58 => Ok(BuiltinId::Log),
            59 => Ok(BuiltinId::Log2),
            60 => Ok(BuiltinId::Log10),
            61 => Ok(BuiltinId::Join),
            62 => Ok(BuiltinId::HttpGet),
            63 => Ok(BuiltinId::HttpPost),
            64 => Ok(BuiltinId::Assert),
            65 => Ok(BuiltinId::AssertEq),
            66 => Ok(BuiltinId::JsonParse),
            67 => Ok(BuiltinId::JsonStringify),
            68 => Ok(BuiltinId::MapFrom),
            69 => Ok(BuiltinId::ReadFile),
            70 => Ok(BuiltinId::WriteFile),
            71 => Ok(BuiltinId::AppendFile),
            72 => Ok(BuiltinId::FileExists),
            73 => Ok(BuiltinId::ListDir),
            74 => Ok(BuiltinId::EnvGet),
            75 => Ok(BuiltinId::EnvSet),
            76 => Ok(BuiltinId::RegexMatch),
            77 => Ok(BuiltinId::RegexFind),
            78 => Ok(BuiltinId::RegexReplace),
            79 => Ok(BuiltinId::Now),
            80 => Ok(BuiltinId::DateFormat),
            81 => Ok(BuiltinId::DateParse),
            82 => Ok(BuiltinId::Zip),
            83 => Ok(BuiltinId::Enumerate),
            84 => Ok(BuiltinId::Bool),
            85 => Ok(BuiltinId::Spawn),
            86 => Ok(BuiltinId::Sleep),
            87 => Ok(BuiltinId::Channel),
            88 => Ok(BuiltinId::Send),
            89 => Ok(BuiltinId::Recv),
            90 => Ok(BuiltinId::TryRecv),
            91 => Ok(BuiltinId::AwaitAll),
            92 => Ok(BuiltinId::Pmap),
            93 => Ok(BuiltinId::Timeout),
            94 => Ok(BuiltinId::Next),
            95 => Ok(BuiltinId::IsGenerator),
            96 => Ok(BuiltinId::Iter),
            97 => Ok(BuiltinId::Take),
            98 => Ok(BuiltinId::Skip_),
            99 => Ok(BuiltinId::GenCollect),
            100 => Ok(BuiltinId::GenMap),
            101 => Ok(BuiltinId::GenFilter),
            102 => Ok(BuiltinId::Chain),
            103 => Ok(BuiltinId::GenZip),
            104 => Ok(BuiltinId::GenEnumerate),
            105 => Ok(BuiltinId::Ok),
            106 => Ok(BuiltinId::Err_),
            107 => Ok(BuiltinId::IsOk),
            108 => Ok(BuiltinId::IsErr),
            109 => Ok(BuiltinId::Unwrap),
            110 => Ok(BuiltinId::SetFrom),
            111 => Ok(BuiltinId::SetAdd),
            112 => Ok(BuiltinId::SetRemove),
            113 => Ok(BuiltinId::SetContains),
            114 => Ok(BuiltinId::SetUnion),
            115 => Ok(BuiltinId::SetIntersection),
            116 => Ok(BuiltinId::SetDifference),
            117 => Ok(BuiltinId::FillNull),
            118 => Ok(BuiltinId::DropNull),
            119 => Ok(BuiltinId::Dedup),
            120 => Ok(BuiltinId::Clamp),
            121 => Ok(BuiltinId::DataProfile),
            122 => Ok(BuiltinId::RowCount),
            123 => Ok(BuiltinId::NullRate),
            124 => Ok(BuiltinId::IsUnique),
            125 => Ok(BuiltinId::IsEmail),
            126 => Ok(BuiltinId::IsUrl),
            127 => Ok(BuiltinId::IsPhone),
            128 => Ok(BuiltinId::IsBetween),
            129 => Ok(BuiltinId::Levenshtein),
            130 => Ok(BuiltinId::Soundex),
            131 => Ok(BuiltinId::ReadMysql),
            132 => Ok(BuiltinId::RedisConnect),
            133 => Ok(BuiltinId::RedisGet),
            134 => Ok(BuiltinId::RedisSet),
            135 => Ok(BuiltinId::RedisDel),
            136 => Ok(BuiltinId::GraphqlQuery),
            137 => Ok(BuiltinId::RegisterS3),
            138 => Ok(BuiltinId::PyImport),
            139 => Ok(BuiltinId::PyCall),
            140 => Ok(BuiltinId::PyEval),
            141 => Ok(BuiltinId::PyGetAttr),
            142 => Ok(BuiltinId::PySetAttr),
            143 => Ok(BuiltinId::PyToTl),
            144 => Ok(BuiltinId::SchemaRegister),
            145 => Ok(BuiltinId::SchemaGet),
            146 => Ok(BuiltinId::SchemaLatest),
            147 => Ok(BuiltinId::SchemaHistory),
            148 => Ok(BuiltinId::SchemaCheck),
            149 => Ok(BuiltinId::SchemaDiff),
            150 => Ok(BuiltinId::SchemaApplyMigration),
            151 => Ok(BuiltinId::SchemaVersions),
            152 => Ok(BuiltinId::SchemaFields),
            153 => Ok(BuiltinId::Decimal),
            154 => Ok(BuiltinId::SecretGet),
            155 => Ok(BuiltinId::SecretSet),
            156 => Ok(BuiltinId::SecretDelete),
            157 => Ok(BuiltinId::SecretList),
            158 => Ok(BuiltinId::CheckPermission),
            159 => Ok(BuiltinId::MaskEmail),
            160 => Ok(BuiltinId::MaskPhone),
            161 => Ok(BuiltinId::MaskCreditCard),
            162 => Ok(BuiltinId::Redact),
            163 => Ok(BuiltinId::Hash),
            164 => Ok(BuiltinId::AsyncReadFile),
            165 => Ok(BuiltinId::AsyncWriteFile),
            166 => Ok(BuiltinId::AsyncHttpGet),
            167 => Ok(BuiltinId::AsyncHttpPost),
            168 => Ok(BuiltinId::AsyncSleep),
            169 => Ok(BuiltinId::Select),
            170 => Ok(BuiltinId::AsyncMap),
            171 => Ok(BuiltinId::AsyncFilter),
            172 => Ok(BuiltinId::RaceAll),
            173 => Ok(BuiltinId::IsError),
            174 => Ok(BuiltinId::ErrorType),
            175 => Ok(BuiltinId::GpuAvailable),
            176 => Ok(BuiltinId::ToGpu),
            177 => Ok(BuiltinId::ToCpu),
            178 => Ok(BuiltinId::GpuMatmul),
            179 => Ok(BuiltinId::GpuBatchPredict),
            180 => Ok(BuiltinId::ReadSqlite),
            181 => Ok(BuiltinId::WriteSqlite),
            182 => Ok(BuiltinId::Embed),
            183 => Ok(BuiltinId::HttpRequest),
            184 => Ok(BuiltinId::RunAgent),
            185 => Ok(BuiltinId::Random),
            186 => Ok(BuiltinId::RandomInt),
            187 => Ok(BuiltinId::Sample),
            188 => Ok(BuiltinId::Exp),
            189 => Ok(BuiltinId::IsNan),
            190 => Ok(BuiltinId::IsInfinite),
            191 => Ok(BuiltinId::Sign),
            192 => Ok(BuiltinId::AssertTableEq),
            193 => Ok(BuiltinId::Today),
            194 => Ok(BuiltinId::DateAdd),
            195 => Ok(BuiltinId::DateDiff),
            196 => Ok(BuiltinId::DateTrunc),
            197 => Ok(BuiltinId::DateExtract),
            198 => Ok(BuiltinId::StreamAgent),
            199 => Ok(BuiltinId::PostgresQuery),
            200 => Ok(BuiltinId::Fold),
            201 => Ok(BuiltinId::TlConfigResolve),
            _ => Err(value),
        }
    }
}

impl BuiltinId {
    pub fn from_name(name: &str) -> Option<BuiltinId> {
        match name {
            "print" => Some(BuiltinId::Print),
            "println" => Some(BuiltinId::Println),
            "len" => Some(BuiltinId::Len),
            "str" => Some(BuiltinId::Str),
            "int" => Some(BuiltinId::Int),
            "float" => Some(BuiltinId::Float),
            "abs" => Some(BuiltinId::Abs),
            "min" => Some(BuiltinId::Min),
            "max" => Some(BuiltinId::Max),
            "range" => Some(BuiltinId::Range),
            "push" => Some(BuiltinId::Push),
            "type_of" => Some(BuiltinId::TypeOf),
            "map" => Some(BuiltinId::Map),
            "filter" => Some(BuiltinId::Filter),
            "reduce" => Some(BuiltinId::Reduce),
            "sum" => Some(BuiltinId::Sum),
            "any" => Some(BuiltinId::Any),
            "all" => Some(BuiltinId::All),
            "read_csv" => Some(BuiltinId::ReadCsv),
            "read_parquet" => Some(BuiltinId::ReadParquet),
            "write_csv" => Some(BuiltinId::WriteCsv),
            "write_parquet" => Some(BuiltinId::WriteParquet),
            "collect" => Some(BuiltinId::Collect),
            "show" => Some(BuiltinId::Show),
            "describe" => Some(BuiltinId::Describe),
            "head" => Some(BuiltinId::Head),
            "postgres" => Some(BuiltinId::Postgres),
            "tensor" => Some(BuiltinId::Tensor),
            "tensor_zeros" => Some(BuiltinId::TensorZeros),
            "tensor_ones" => Some(BuiltinId::TensorOnes),
            "tensor_shape" => Some(BuiltinId::TensorShape),
            "tensor_reshape" => Some(BuiltinId::TensorReshape),
            "tensor_transpose" => Some(BuiltinId::TensorTranspose),
            "tensor_sum" => Some(BuiltinId::TensorSum),
            "tensor_mean" => Some(BuiltinId::TensorMean),
            "tensor_dot" => Some(BuiltinId::TensorDot),
            "predict" => Some(BuiltinId::Predict),
            "similarity" => Some(BuiltinId::Similarity),
            "ai_complete" => Some(BuiltinId::AiComplete),
            "ai_chat" => Some(BuiltinId::AiChat),
            "model_save" => Some(BuiltinId::ModelSave),
            "model_load" => Some(BuiltinId::ModelLoad),
            "model_register" => Some(BuiltinId::ModelRegister),
            "model_list" => Some(BuiltinId::ModelList),
            "model_get" => Some(BuiltinId::ModelGet),
            "alert_slack" => Some(BuiltinId::AlertSlack),
            "alert_webhook" => Some(BuiltinId::AlertWebhook),
            "emit" => Some(BuiltinId::Emit),
            "lineage" => Some(BuiltinId::Lineage),
            "run_pipeline" => Some(BuiltinId::RunPipeline),
            "sqrt" => Some(BuiltinId::Sqrt),
            "pow" => Some(BuiltinId::Pow),
            "floor" => Some(BuiltinId::Floor),
            "ceil" => Some(BuiltinId::Ceil),
            "round" => Some(BuiltinId::Round),
            "sin" => Some(BuiltinId::Sin),
            "cos" => Some(BuiltinId::Cos),
            "tan" => Some(BuiltinId::Tan),
            "log" => Some(BuiltinId::Log),
            "log2" => Some(BuiltinId::Log2),
            "log10" => Some(BuiltinId::Log10),
            "join" => Some(BuiltinId::Join),
            "http_get" => Some(BuiltinId::HttpGet),
            "http_post" => Some(BuiltinId::HttpPost),
            "assert" => Some(BuiltinId::Assert),
            "assert_eq" => Some(BuiltinId::AssertEq),
            "json_parse" => Some(BuiltinId::JsonParse),
            "json_stringify" => Some(BuiltinId::JsonStringify),
            "map_from" => Some(BuiltinId::MapFrom),
            "read_file" => Some(BuiltinId::ReadFile),
            "write_file" => Some(BuiltinId::WriteFile),
            "append_file" => Some(BuiltinId::AppendFile),
            "file_exists" => Some(BuiltinId::FileExists),
            "list_dir" => Some(BuiltinId::ListDir),
            "env_get" => Some(BuiltinId::EnvGet),
            "env_set" => Some(BuiltinId::EnvSet),
            "regex_match" => Some(BuiltinId::RegexMatch),
            "regex_find" => Some(BuiltinId::RegexFind),
            "regex_replace" => Some(BuiltinId::RegexReplace),
            "now" => Some(BuiltinId::Now),
            "date_format" => Some(BuiltinId::DateFormat),
            "date_parse" => Some(BuiltinId::DateParse),
            "zip" => Some(BuiltinId::Zip),
            "enumerate" => Some(BuiltinId::Enumerate),
            "bool" => Some(BuiltinId::Bool),
            "spawn" => Some(BuiltinId::Spawn),
            "sleep" => Some(BuiltinId::Sleep),
            "channel" => Some(BuiltinId::Channel),
            "send" => Some(BuiltinId::Send),
            "recv" => Some(BuiltinId::Recv),
            "try_recv" => Some(BuiltinId::TryRecv),
            "await_all" => Some(BuiltinId::AwaitAll),
            "pmap" => Some(BuiltinId::Pmap),
            "timeout" => Some(BuiltinId::Timeout),
            "next" => Some(BuiltinId::Next),
            "is_generator" => Some(BuiltinId::IsGenerator),
            "iter" => Some(BuiltinId::Iter),
            "take" => Some(BuiltinId::Take),
            "skip" => Some(BuiltinId::Skip_),
            "gen_collect" => Some(BuiltinId::GenCollect),
            "gen_map" => Some(BuiltinId::GenMap),
            "gen_filter" => Some(BuiltinId::GenFilter),
            "chain" => Some(BuiltinId::Chain),
            "gen_zip" => Some(BuiltinId::GenZip),
            "gen_enumerate" => Some(BuiltinId::GenEnumerate),
            "Ok" => Some(BuiltinId::Ok),
            "Err" => Some(BuiltinId::Err_),
            "is_ok" => Some(BuiltinId::IsOk),
            "is_err" => Some(BuiltinId::IsErr),
            "unwrap" => Some(BuiltinId::Unwrap),
            "set_from" => Some(BuiltinId::SetFrom),
            "set_add" => Some(BuiltinId::SetAdd),
            "set_remove" => Some(BuiltinId::SetRemove),
            "set_contains" => Some(BuiltinId::SetContains),
            "set_union" => Some(BuiltinId::SetUnion),
            "set_intersection" => Some(BuiltinId::SetIntersection),
            "set_difference" => Some(BuiltinId::SetDifference),
            // Phase 15: Data Quality & Connectors
            "fill_null" => Some(BuiltinId::FillNull),
            "drop_null" => Some(BuiltinId::DropNull),
            "dedup" => Some(BuiltinId::Dedup),
            "clamp" => Some(BuiltinId::Clamp),
            "data_profile" => Some(BuiltinId::DataProfile),
            "row_count" => Some(BuiltinId::RowCount),
            "null_rate" => Some(BuiltinId::NullRate),
            "is_unique" => Some(BuiltinId::IsUnique),
            "is_email" => Some(BuiltinId::IsEmail),
            "is_url" => Some(BuiltinId::IsUrl),
            "is_phone" => Some(BuiltinId::IsPhone),
            "is_between" => Some(BuiltinId::IsBetween),
            "levenshtein" => Some(BuiltinId::Levenshtein),
            "soundex" => Some(BuiltinId::Soundex),
            "read_mysql" => Some(BuiltinId::ReadMysql),
            "redis_connect" => Some(BuiltinId::RedisConnect),
            "redis_get" => Some(BuiltinId::RedisGet),
            "redis_set" => Some(BuiltinId::RedisSet),
            "redis_del" => Some(BuiltinId::RedisDel),
            "graphql_query" => Some(BuiltinId::GraphqlQuery),
            "register_s3" => Some(BuiltinId::RegisterS3),
            // Phase 20: Python FFI
            "py_import" => Some(BuiltinId::PyImport),
            "py_call" => Some(BuiltinId::PyCall),
            "py_eval" => Some(BuiltinId::PyEval),
            "py_getattr" => Some(BuiltinId::PyGetAttr),
            "py_setattr" => Some(BuiltinId::PySetAttr),
            "py_to_tl" => Some(BuiltinId::PyToTl),
            // Phase 21: Schema Evolution
            "schema_register" => Some(BuiltinId::SchemaRegister),
            "schema_get" => Some(BuiltinId::SchemaGet),
            "schema_latest" => Some(BuiltinId::SchemaLatest),
            "schema_history" => Some(BuiltinId::SchemaHistory),
            "schema_check" => Some(BuiltinId::SchemaCheck),
            "schema_diff" => Some(BuiltinId::SchemaDiff),
            "schema_apply_migration" => Some(BuiltinId::SchemaApplyMigration),
            "schema_versions" => Some(BuiltinId::SchemaVersions),
            "schema_fields" => Some(BuiltinId::SchemaFields),
            // Phase 22: Advanced Types
            "decimal" => Some(BuiltinId::Decimal),
            // Phase 23: Security
            "secret_get" => Some(BuiltinId::SecretGet),
            "secret_set" => Some(BuiltinId::SecretSet),
            "secret_delete" => Some(BuiltinId::SecretDelete),
            "secret_list" => Some(BuiltinId::SecretList),
            "check_permission" => Some(BuiltinId::CheckPermission),
            "mask_email" => Some(BuiltinId::MaskEmail),
            "mask_phone" => Some(BuiltinId::MaskPhone),
            "mask_cc" => Some(BuiltinId::MaskCreditCard),
            "redact" => Some(BuiltinId::Redact),
            "hash" => Some(BuiltinId::Hash),
            // Phase 24: Async
            "async_read_file" => Some(BuiltinId::AsyncReadFile),
            "async_write_file" => Some(BuiltinId::AsyncWriteFile),
            "async_http_get" => Some(BuiltinId::AsyncHttpGet),
            "async_http_post" => Some(BuiltinId::AsyncHttpPost),
            "async_sleep" => Some(BuiltinId::AsyncSleep),
            "select" => Some(BuiltinId::Select),
            "async_map" => Some(BuiltinId::AsyncMap),
            "async_filter" => Some(BuiltinId::AsyncFilter),
            "race_all" => Some(BuiltinId::RaceAll),
            // Phase 27: Data Error Hierarchy
            "is_error" => Some(BuiltinId::IsError),
            "error_type" => Some(BuiltinId::ErrorType),
            // Phase 32: GPU Tensor Support
            "gpu_available" => Some(BuiltinId::GpuAvailable),
            "to_gpu" => Some(BuiltinId::ToGpu),
            "to_cpu" => Some(BuiltinId::ToCpu),
            "gpu_matmul" => Some(BuiltinId::GpuMatmul),
            "gpu_batch_predict" => Some(BuiltinId::GpuBatchPredict),
            // Phase 33: SQLite
            "read_sqlite" => Some(BuiltinId::ReadSqlite),
            "write_sqlite" => Some(BuiltinId::WriteSqlite),
            // Phase 34: AI Agent Framework
            "embed" => Some(BuiltinId::Embed),
            "http_request" => Some(BuiltinId::HttpRequest),
            "run_agent" => Some(BuiltinId::RunAgent),
            "random" => Some(BuiltinId::Random),
            "random_int" => Some(BuiltinId::RandomInt),
            "sample" => Some(BuiltinId::Sample),
            "exp" => Some(BuiltinId::Exp),
            "is_nan" => Some(BuiltinId::IsNan),
            "is_infinite" => Some(BuiltinId::IsInfinite),
            "sign" => Some(BuiltinId::Sign),
            "assert_table_eq" => Some(BuiltinId::AssertTableEq),
            "today" => Some(BuiltinId::Today),
            "date_add" => Some(BuiltinId::DateAdd),
            "date_diff" => Some(BuiltinId::DateDiff),
            "date_trunc" => Some(BuiltinId::DateTrunc),
            "date_extract" | "extract" => Some(BuiltinId::DateExtract),
            "stream_agent" => Some(BuiltinId::StreamAgent),
            "postgres_query" => Some(BuiltinId::PostgresQuery),
            "fold" => Some(BuiltinId::Fold),
            "tl_config_resolve" => Some(BuiltinId::TlConfigResolve),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            BuiltinId::Print => "print",
            BuiltinId::Println => "println",
            BuiltinId::Len => "len",
            BuiltinId::Str => "str",
            BuiltinId::Int => "int",
            BuiltinId::Float => "float",
            BuiltinId::Abs => "abs",
            BuiltinId::Min => "min",
            BuiltinId::Max => "max",
            BuiltinId::Range => "range",
            BuiltinId::Push => "push",
            BuiltinId::TypeOf => "type_of",
            BuiltinId::Map => "map",
            BuiltinId::Filter => "filter",
            BuiltinId::Reduce => "reduce",
            BuiltinId::Sum => "sum",
            BuiltinId::Any => "any",
            BuiltinId::All => "all",
            BuiltinId::ReadCsv => "read_csv",
            BuiltinId::ReadParquet => "read_parquet",
            BuiltinId::WriteCsv => "write_csv",
            BuiltinId::WriteParquet => "write_parquet",
            BuiltinId::Collect => "collect",
            BuiltinId::Show => "show",
            BuiltinId::Describe => "describe",
            BuiltinId::Head => "head",
            BuiltinId::Postgres => "postgres",
            BuiltinId::Tensor => "tensor",
            BuiltinId::TensorZeros => "tensor_zeros",
            BuiltinId::TensorOnes => "tensor_ones",
            BuiltinId::TensorShape => "tensor_shape",
            BuiltinId::TensorReshape => "tensor_reshape",
            BuiltinId::TensorTranspose => "tensor_transpose",
            BuiltinId::TensorSum => "tensor_sum",
            BuiltinId::TensorMean => "tensor_mean",
            BuiltinId::TensorDot => "tensor_dot",
            BuiltinId::Predict => "predict",
            BuiltinId::Similarity => "similarity",
            BuiltinId::AiComplete => "ai_complete",
            BuiltinId::AiChat => "ai_chat",
            BuiltinId::ModelSave => "model_save",
            BuiltinId::ModelLoad => "model_load",
            BuiltinId::ModelRegister => "model_register",
            BuiltinId::ModelList => "model_list",
            BuiltinId::ModelGet => "model_get",
            BuiltinId::AlertSlack => "alert_slack",
            BuiltinId::AlertWebhook => "alert_webhook",
            BuiltinId::Emit => "emit",
            BuiltinId::Lineage => "lineage",
            BuiltinId::RunPipeline => "run_pipeline",
            BuiltinId::Sqrt => "sqrt",
            BuiltinId::Pow => "pow",
            BuiltinId::Floor => "floor",
            BuiltinId::Ceil => "ceil",
            BuiltinId::Round => "round",
            BuiltinId::Sin => "sin",
            BuiltinId::Cos => "cos",
            BuiltinId::Tan => "tan",
            BuiltinId::Log => "log",
            BuiltinId::Log2 => "log2",
            BuiltinId::Log10 => "log10",
            BuiltinId::Join => "join",
            BuiltinId::HttpGet => "http_get",
            BuiltinId::HttpPost => "http_post",
            BuiltinId::Assert => "assert",
            BuiltinId::AssertEq => "assert_eq",
            BuiltinId::JsonParse => "json_parse",
            BuiltinId::JsonStringify => "json_stringify",
            BuiltinId::MapFrom => "map_from",
            BuiltinId::ReadFile => "read_file",
            BuiltinId::WriteFile => "write_file",
            BuiltinId::AppendFile => "append_file",
            BuiltinId::FileExists => "file_exists",
            BuiltinId::ListDir => "list_dir",
            BuiltinId::EnvGet => "env_get",
            BuiltinId::EnvSet => "env_set",
            BuiltinId::RegexMatch => "regex_match",
            BuiltinId::RegexFind => "regex_find",
            BuiltinId::RegexReplace => "regex_replace",
            BuiltinId::Now => "now",
            BuiltinId::DateFormat => "date_format",
            BuiltinId::DateParse => "date_parse",
            BuiltinId::Zip => "zip",
            BuiltinId::Enumerate => "enumerate",
            BuiltinId::Bool => "bool",
            BuiltinId::Spawn => "spawn",
            BuiltinId::Sleep => "sleep",
            BuiltinId::Channel => "channel",
            BuiltinId::Send => "send",
            BuiltinId::Recv => "recv",
            BuiltinId::TryRecv => "try_recv",
            BuiltinId::AwaitAll => "await_all",
            BuiltinId::Pmap => "pmap",
            BuiltinId::Timeout => "timeout",
            BuiltinId::Next => "next",
            BuiltinId::IsGenerator => "is_generator",
            BuiltinId::Iter => "iter",
            BuiltinId::Take => "take",
            BuiltinId::Skip_ => "skip",
            BuiltinId::GenCollect => "gen_collect",
            BuiltinId::GenMap => "gen_map",
            BuiltinId::GenFilter => "gen_filter",
            BuiltinId::Chain => "chain",
            BuiltinId::GenZip => "gen_zip",
            BuiltinId::GenEnumerate => "gen_enumerate",
            BuiltinId::Ok => "Ok",
            BuiltinId::Err_ => "Err",
            BuiltinId::IsOk => "is_ok",
            BuiltinId::IsErr => "is_err",
            BuiltinId::Unwrap => "unwrap",
            BuiltinId::SetFrom => "set_from",
            BuiltinId::SetAdd => "set_add",
            BuiltinId::SetRemove => "set_remove",
            BuiltinId::SetContains => "set_contains",
            BuiltinId::SetUnion => "set_union",
            BuiltinId::SetIntersection => "set_intersection",
            BuiltinId::SetDifference => "set_difference",
            // Phase 15: Data Quality & Connectors
            BuiltinId::FillNull => "fill_null",
            BuiltinId::DropNull => "drop_null",
            BuiltinId::Dedup => "dedup",
            BuiltinId::Clamp => "clamp",
            BuiltinId::DataProfile => "data_profile",
            BuiltinId::RowCount => "row_count",
            BuiltinId::NullRate => "null_rate",
            BuiltinId::IsUnique => "is_unique",
            BuiltinId::IsEmail => "is_email",
            BuiltinId::IsUrl => "is_url",
            BuiltinId::IsPhone => "is_phone",
            BuiltinId::IsBetween => "is_between",
            BuiltinId::Levenshtein => "levenshtein",
            BuiltinId::Soundex => "soundex",
            BuiltinId::ReadMysql => "read_mysql",
            BuiltinId::RedisConnect => "redis_connect",
            BuiltinId::RedisGet => "redis_get",
            BuiltinId::RedisSet => "redis_set",
            BuiltinId::RedisDel => "redis_del",
            BuiltinId::GraphqlQuery => "graphql_query",
            BuiltinId::RegisterS3 => "register_s3",
            // Phase 20: Python FFI
            BuiltinId::PyImport => "py_import",
            BuiltinId::PyCall => "py_call",
            BuiltinId::PyEval => "py_eval",
            BuiltinId::PyGetAttr => "py_getattr",
            BuiltinId::PySetAttr => "py_setattr",
            BuiltinId::PyToTl => "py_to_tl",
            // Phase 21: Schema Evolution
            BuiltinId::SchemaRegister => "schema_register",
            BuiltinId::SchemaGet => "schema_get",
            BuiltinId::SchemaLatest => "schema_latest",
            BuiltinId::SchemaHistory => "schema_history",
            BuiltinId::SchemaCheck => "schema_check",
            BuiltinId::SchemaDiff => "schema_diff",
            BuiltinId::SchemaApplyMigration => "schema_apply_migration",
            BuiltinId::SchemaVersions => "schema_versions",
            BuiltinId::SchemaFields => "schema_fields",
            // Phase 22
            BuiltinId::Decimal => "decimal",
            // Phase 23
            BuiltinId::SecretGet => "secret_get",
            BuiltinId::SecretSet => "secret_set",
            BuiltinId::SecretDelete => "secret_delete",
            BuiltinId::SecretList => "secret_list",
            BuiltinId::CheckPermission => "check_permission",
            BuiltinId::MaskEmail => "mask_email",
            BuiltinId::MaskPhone => "mask_phone",
            BuiltinId::MaskCreditCard => "mask_cc",
            BuiltinId::Redact => "redact",
            BuiltinId::Hash => "hash",
            // Phase 24
            BuiltinId::AsyncReadFile => "async_read_file",
            BuiltinId::AsyncWriteFile => "async_write_file",
            BuiltinId::AsyncHttpGet => "async_http_get",
            BuiltinId::AsyncHttpPost => "async_http_post",
            BuiltinId::AsyncSleep => "async_sleep",
            BuiltinId::Select => "select",
            BuiltinId::AsyncMap => "async_map",
            BuiltinId::AsyncFilter => "async_filter",
            BuiltinId::RaceAll => "race_all",
            // Phase 27
            BuiltinId::IsError => "is_error",
            BuiltinId::ErrorType => "error_type",
            // Phase 32: GPU
            BuiltinId::GpuAvailable => "gpu_available",
            BuiltinId::ToGpu => "to_gpu",
            BuiltinId::ToCpu => "to_cpu",
            BuiltinId::GpuMatmul => "gpu_matmul",
            BuiltinId::GpuBatchPredict => "gpu_batch_predict",
            // Phase 33: SQLite
            BuiltinId::ReadSqlite => "read_sqlite",
            BuiltinId::WriteSqlite => "write_sqlite",
            // Phase 34: AI Agent Framework
            BuiltinId::Embed => "embed",
            BuiltinId::HttpRequest => "http_request",
            BuiltinId::RunAgent => "run_agent",
            // Phase E5/E6
            BuiltinId::Random => "random",
            BuiltinId::RandomInt => "random_int",
            BuiltinId::Sample => "sample",
            BuiltinId::Exp => "exp",
            BuiltinId::IsNan => "is_nan",
            BuiltinId::IsInfinite => "is_infinite",
            BuiltinId::Sign => "sign",
            BuiltinId::AssertTableEq => "assert_table_eq",
            BuiltinId::Today => "today",
            BuiltinId::DateAdd => "date_add",
            BuiltinId::DateDiff => "date_diff",
            BuiltinId::DateTrunc => "date_trunc",
            BuiltinId::DateExtract => "date_extract",
            BuiltinId::StreamAgent => "stream_agent",
            BuiltinId::PostgresQuery => "postgres_query",
            BuiltinId::Fold => "fold",
            BuiltinId::TlConfigResolve => "tl_config_resolve",
        }
    }
}

/// Table pipe operation descriptor, stored as a constant.
#[derive(Debug, Clone)]
pub struct TableOpDescriptor {
    pub op_name: String,
    pub args: Vec<AstExpr>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::compile_with_source;
    use tl_parser::parse;

    #[test]
    fn test_disassemble_simple() {
        let source = "let x = 42";
        let program = parse(source).unwrap();
        let proto = compile_with_source(&program, source).unwrap();
        let output = proto.disassemble();
        assert!(
            output.contains("LoadConst"),
            "expected LoadConst in:\n{output}"
        );
        assert!(output.contains("42"), "expected 42 in:\n{output}");
    }

    #[test]
    fn test_disassemble_line_numbers() {
        let source = "let x = 1\nlet y = 2\nlet z = x";
        let program = parse(source).unwrap();
        let proto = compile_with_source(&program, source).unwrap();
        let output = proto.disassemble();
        // Should have different line numbers for different statements
        assert!(output.contains("0001"), "expected line 0001 in:\n{output}");
        assert!(output.contains("0002"), "expected line 0002 in:\n{output}");
    }

    #[test]
    fn test_disassemble_function() {
        let source = "fn add(a, b) { a + b }";
        let program = parse(source).unwrap();
        let proto = compile_with_source(&program, source).unwrap();
        let output = proto.disassemble();
        assert!(output.contains("Closure"), "expected Closure in:\n{output}");
    }

    #[test]
    fn test_disassemble_constants_inline() {
        let source = "let s = \"hello world\"";
        let program = parse(source).unwrap();
        let proto = compile_with_source(&program, source).unwrap();
        let output = proto.disassemble();
        assert!(
            output.contains("hello world"),
            "expected 'hello world' in:\n{output}"
        );
    }

    #[test]
    fn test_builtin_id_try_from_valid() {
        for v in 0..=201u16 {
            assert!(
                BuiltinId::try_from(v).is_ok(),
                "BuiltinId::try_from({v}) should succeed"
            );
        }
        assert_eq!(BuiltinId::try_from(0u16).unwrap(), BuiltinId::Print);
        assert_eq!(BuiltinId::try_from(198u16).unwrap(), BuiltinId::StreamAgent);
        assert_eq!(BuiltinId::try_from(201u16).unwrap(), BuiltinId::TlConfigResolve);
    }

    #[test]
    fn test_builtin_id_try_from_invalid() {
        assert_eq!(BuiltinId::try_from(202u16), Err(202u16));
        assert_eq!(BuiltinId::try_from(65535u16), Err(65535u16));
    }
}
