// ThinkingLanguage — Bytecode Instruction Set
// Register-based: [opcode:8][A:8][B:8][C:8] or [opcode:8][A:8][Bx:16]

/// Bytecode operations for the TL virtual machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Op {
    // ── Constants & moves ──
    /// Load constant pool[Bx] into register A
    LoadConst = 0,
    /// Load None into register A
    LoadNone = 1,
    /// Load true into register A
    LoadTrue = 2,
    /// Load false into register A
    LoadFalse = 3,
    /// Copy register B into register A
    Move = 4,

    // ── Variables ──
    /// Load local slot B into register A
    GetLocal = 5,
    /// Store register A into local slot B
    SetLocal = 6,
    /// Load global named by constant Bx into register A
    GetGlobal = 7,
    /// Store register A into global named by constant Bx
    SetGlobal = 8,
    /// Load upvalue index B into register A
    GetUpvalue = 9,
    /// Store register A into upvalue index B
    SetUpvalue = 10,

    // ── Arithmetic ──
    /// A = B + C
    Add = 11,
    /// A = B - C
    Sub = 12,
    /// A = B * C
    Mul = 13,
    /// A = B / C
    Div = 14,
    /// A = B % C
    Mod = 15,
    /// A = B ** C
    Pow = 16,
    /// A = -B
    Neg = 17,

    // ── Comparison ──
    /// A = (B == C)
    Eq = 18,
    /// A = (B != C)
    Neq = 19,
    /// A = (B < C)
    Lt = 20,
    /// A = (B > C)
    Gt = 21,
    /// A = (B <= C)
    Lte = 22,
    /// A = (B >= C)
    Gte = 23,

    // ── Logical ──
    /// A = B and C
    And = 24,
    /// A = B or C
    Or = 25,
    /// A = not B
    Not = 26,

    // ── String ──
    /// A = concat(B, C)
    Concat = 27,

    // ── Control flow ──
    /// Jump by signed offset Bx (as i16)
    Jump = 28,
    /// If register A is falsy, jump by signed offset Bx
    JumpIfFalse = 29,
    /// If register A is truthy, jump by signed offset Bx
    JumpIfTrue = 30,

    // ── Functions ──
    /// Call: A = function register, B = first arg register, C = arg count
    /// Result goes into register A
    Call = 31,
    /// Return register A
    Return = 32,
    /// Create closure from prototype at constant Bx, store in A
    /// Followed by upvalue descriptors
    Closure = 33,

    // ── Data structures ──
    /// Create list: A = dest, B = start register, C = count
    NewList = 34,
    /// A = B[C]
    GetIndex = 35,
    /// B[C] = A
    SetIndex = 36,
    /// Create map: A = dest, B = start register (alternating key/value), C = pair count
    NewMap = 37,

    // ── Table operations ──
    /// TablePipe: A = table reg, B = op constant index, C = args start
    /// The VM handles this specially for DataFusion table ops
    TablePipe = 38,

    // ── Builtins ──
    /// CallBuiltin: A = dest, B = builtin id, C = first arg reg
    /// Next instruction word: arg count in A field
    CallBuiltin = 39,

    // ── Iteration ──
    /// ForIter: A = iterator reg, B = value dest, jump offset in next Bx if done
    ForIter = 40,
    /// ForPrep: A = dest for iterator, B = list register
    ForPrep = 41,

    // ── Pattern matching ──
    /// TestMatch: A = subject reg, B = pattern reg, C = dest bool reg
    TestMatch = 42,

    // ── Null coalesce ──
    /// NullCoalesce: if A is None, A = B
    NullCoalesce = 43,

    // ── Member access ──
    /// GetMember: A = dest, B = object reg, C = field name constant
    GetMember = 44,

    // ── String interpolation ──
    /// Interpolate: A = dest, B = template constant, C = values start reg
    /// Next instruction word: value count in A field
    Interpolate = 45,

    /// Train: A = dest for model, B = algorithm constant, C = config constant
    Train = 46,

    /// PipelineExec: A = dest for result, B = pipeline blocks constant, C = config constant
    PipelineExec = 47,
    /// StreamExec: A = dest, B = stream def constant, C = source register
    StreamExec = 48,
    /// ConnectorDecl: A = dest, B = connector type constant, C = config constant
    ConnectorDecl = 49,

    // ── Phase 5: Language completeness ──
    /// NewStruct: A = dest, B = type name constant, C = field count
    /// Followed by field name/value register pairs
    NewStruct = 50,
    /// SetMember: A = object reg, B = field name constant, C = value reg
    SetMember = 51,
    /// NewEnum: A = dest, B = type+variant name constant, C = args start reg
    /// Next instruction: arg count in A field
    NewEnum = 52,
    /// MatchEnum: A = subject reg, B = variant name constant, C = dest bool reg
    MatchEnum = 53,
    /// MethodCall: A = dest/func reg, B = object reg, C = method name constant
    /// Next instruction: args_start in A, arg_count in B
    MethodCall = 54,
    /// Throw: A = value register to throw
    Throw = 55,
    /// TryBegin: A = catch handler offset (as Bx signed)
    TryBegin = 56,
    /// TryEnd: pops the try handler
    TryEnd = 57,
    /// Import: A = dest, Bx = path constant
    Import = 58,

    // ── Phase 7: Concurrency ──
    /// Await: A = dest, B = task register (passthrough if not a task)
    Await = 59,

    // ── Phase 8: Iterators & Generators ──
    /// Yield: A = value register to yield (suspends generator)
    Yield = 60,

    // ── Phase 10: Type System ──
    /// TryPropagate: A = dest, B = source register
    /// If source is Err(...) → early return from current function
    /// If source is Ok(v) → A = v (unwrap)
    /// If source is None → early return None
    /// Otherwise → passthrough
    TryPropagate = 61,

    // ── Phase 17: Pattern Matching ──
    /// ExtractField: A = dest, B = source reg, C = field index
    /// Extracts field[C] from an enum instance or list into dest.
    /// If C has high bit set (C | 0x80), extracts rest (sublist from index C & 0x7F).
    ExtractField = 62,
    /// ExtractNamedField: A = dest, B = source reg, C = field name constant index
    /// Extracts a named field from a struct into dest.
    ExtractNamedField = 63,

    // ── Phase 28: Ownership & Move Semantics ──
    /// LoadMoved: A = Moved tombstone
    LoadMoved = 64,
    /// MakeRef: A = Ref(B) — wrap value in read-only reference
    MakeRef = 65,
    /// ParallelFor: A = list reg, B = body prototype constant, C = unused
    ParallelFor = 66,

    // ── Phase 34: AI Agent Framework ──
    /// AgentExec: A = dest, B = name constant, C = config constant
    AgentExec = 67,
}

impl Op {
    /// Return a human-readable name for this opcode.
    pub fn name(&self) -> &'static str {
        match self {
            Op::LoadConst => "LoadConst",
            Op::LoadNone => "LoadNone",
            Op::LoadTrue => "LoadTrue",
            Op::LoadFalse => "LoadFalse",
            Op::Move => "Move",
            Op::GetLocal => "GetLocal",
            Op::SetLocal => "SetLocal",
            Op::GetGlobal => "GetGlobal",
            Op::SetGlobal => "SetGlobal",
            Op::GetUpvalue => "GetUpvalue",
            Op::SetUpvalue => "SetUpvalue",
            Op::Add => "Add",
            Op::Sub => "Sub",
            Op::Mul => "Mul",
            Op::Div => "Div",
            Op::Mod => "Mod",
            Op::Pow => "Pow",
            Op::Neg => "Neg",
            Op::Eq => "Eq",
            Op::Neq => "Neq",
            Op::Lt => "Lt",
            Op::Gt => "Gt",
            Op::Lte => "Lte",
            Op::Gte => "Gte",
            Op::And => "And",
            Op::Or => "Or",
            Op::Not => "Not",
            Op::Concat => "Concat",
            Op::Jump => "Jump",
            Op::JumpIfFalse => "JumpIfFalse",
            Op::JumpIfTrue => "JumpIfTrue",
            Op::Call => "Call",
            Op::Return => "Return",
            Op::Closure => "Closure",
            Op::NewList => "NewList",
            Op::GetIndex => "GetIndex",
            Op::SetIndex => "SetIndex",
            Op::NewMap => "NewMap",
            Op::TablePipe => "TablePipe",
            Op::CallBuiltin => "CallBuiltin",
            Op::ForIter => "ForIter",
            Op::ForPrep => "ForPrep",
            Op::TestMatch => "TestMatch",
            Op::NullCoalesce => "NullCoalesce",
            Op::GetMember => "GetMember",
            Op::Interpolate => "Interpolate",
            Op::Train => "Train",
            Op::PipelineExec => "PipelineExec",
            Op::StreamExec => "StreamExec",
            Op::ConnectorDecl => "ConnectorDecl",
            Op::NewStruct => "NewStruct",
            Op::SetMember => "SetMember",
            Op::NewEnum => "NewEnum",
            Op::MatchEnum => "MatchEnum",
            Op::MethodCall => "MethodCall",
            Op::Throw => "Throw",
            Op::TryBegin => "TryBegin",
            Op::TryEnd => "TryEnd",
            Op::Import => "Import",
            Op::Await => "Await",
            Op::Yield => "Yield",
            Op::TryPropagate => "TryPropagate",
            Op::ExtractField => "ExtractField",
            Op::ExtractNamedField => "ExtractNamedField",
            Op::LoadMoved => "LoadMoved",
            Op::MakeRef => "MakeRef",
            Op::ParallelFor => "ParallelFor",
            Op::AgentExec => "AgentExec",
        }
    }
}

/// Encode an ABC-format instruction: [op:8][A:8][B:8][C:8]
pub fn encode_abc(op: Op, a: u8, b: u8, c: u8) -> u32 {
    ((op as u32) << 24) | ((a as u32) << 16) | ((b as u32) << 8) | (c as u32)
}

/// Encode an ABx-format instruction: [op:8][A:8][Bx:16]
pub fn encode_abx(op: Op, a: u8, bx: u16) -> u32 {
    ((op as u32) << 24) | ((a as u32) << 16) | (bx as u32)
}

impl TryFrom<u8> for Op {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Op::LoadConst),
            1 => Ok(Op::LoadNone),
            2 => Ok(Op::LoadTrue),
            3 => Ok(Op::LoadFalse),
            4 => Ok(Op::Move),
            5 => Ok(Op::GetLocal),
            6 => Ok(Op::SetLocal),
            7 => Ok(Op::GetGlobal),
            8 => Ok(Op::SetGlobal),
            9 => Ok(Op::GetUpvalue),
            10 => Ok(Op::SetUpvalue),
            11 => Ok(Op::Add),
            12 => Ok(Op::Sub),
            13 => Ok(Op::Mul),
            14 => Ok(Op::Div),
            15 => Ok(Op::Mod),
            16 => Ok(Op::Pow),
            17 => Ok(Op::Neg),
            18 => Ok(Op::Eq),
            19 => Ok(Op::Neq),
            20 => Ok(Op::Lt),
            21 => Ok(Op::Gt),
            22 => Ok(Op::Lte),
            23 => Ok(Op::Gte),
            24 => Ok(Op::And),
            25 => Ok(Op::Or),
            26 => Ok(Op::Not),
            27 => Ok(Op::Concat),
            28 => Ok(Op::Jump),
            29 => Ok(Op::JumpIfFalse),
            30 => Ok(Op::JumpIfTrue),
            31 => Ok(Op::Call),
            32 => Ok(Op::Return),
            33 => Ok(Op::Closure),
            34 => Ok(Op::NewList),
            35 => Ok(Op::GetIndex),
            36 => Ok(Op::SetIndex),
            37 => Ok(Op::NewMap),
            38 => Ok(Op::TablePipe),
            39 => Ok(Op::CallBuiltin),
            40 => Ok(Op::ForIter),
            41 => Ok(Op::ForPrep),
            42 => Ok(Op::TestMatch),
            43 => Ok(Op::NullCoalesce),
            44 => Ok(Op::GetMember),
            45 => Ok(Op::Interpolate),
            46 => Ok(Op::Train),
            47 => Ok(Op::PipelineExec),
            48 => Ok(Op::StreamExec),
            49 => Ok(Op::ConnectorDecl),
            50 => Ok(Op::NewStruct),
            51 => Ok(Op::SetMember),
            52 => Ok(Op::NewEnum),
            53 => Ok(Op::MatchEnum),
            54 => Ok(Op::MethodCall),
            55 => Ok(Op::Throw),
            56 => Ok(Op::TryBegin),
            57 => Ok(Op::TryEnd),
            58 => Ok(Op::Import),
            59 => Ok(Op::Await),
            60 => Ok(Op::Yield),
            61 => Ok(Op::TryPropagate),
            62 => Ok(Op::ExtractField),
            63 => Ok(Op::ExtractNamedField),
            64 => Ok(Op::LoadMoved),
            65 => Ok(Op::MakeRef),
            66 => Ok(Op::ParallelFor),
            67 => Ok(Op::AgentExec),
            _ => Err(value),
        }
    }
}

/// Decode opcode from instruction
pub fn decode_op(inst: u32) -> Op {
    Op::try_from((inst >> 24) as u8).expect("valid opcode in instruction")
}

/// Decode A field
pub fn decode_a(inst: u32) -> u8 {
    ((inst >> 16) & 0xFF) as u8
}

/// Decode B field
pub fn decode_b(inst: u32) -> u8 {
    ((inst >> 8) & 0xFF) as u8
}

/// Decode C field
pub fn decode_c(inst: u32) -> u8 {
    (inst & 0xFF) as u8
}

/// Decode Bx field (16-bit unsigned)
pub fn decode_bx(inst: u32) -> u16 {
    (inst & 0xFFFF) as u16
}

/// Decode Bx as signed offset (for jumps)
pub fn decode_sbx(inst: u32) -> i16 {
    (inst & 0xFFFF) as i16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_abc_round_trip() {
        let inst = encode_abc(Op::Add, 3, 1, 2);
        assert_eq!(decode_op(inst), Op::Add);
        assert_eq!(decode_a(inst), 3);
        assert_eq!(decode_b(inst), 1);
        assert_eq!(decode_c(inst), 2);
    }

    #[test]
    fn test_abx_round_trip() {
        let inst = encode_abx(Op::LoadConst, 5, 1000);
        assert_eq!(decode_op(inst), Op::LoadConst);
        assert_eq!(decode_a(inst), 5);
        assert_eq!(decode_bx(inst), 1000);
    }

    #[test]
    fn test_signed_offset() {
        let inst = encode_abx(Op::Jump, 0, (-10_i16) as u16);
        assert_eq!(decode_op(inst), Op::Jump);
        assert_eq!(decode_sbx(inst), -10);
    }

    #[test]
    fn test_all_ops_encode() {
        // Verify encoding/decoding for boundary values
        let inst = encode_abc(Op::Return, 255, 255, 255);
        assert_eq!(decode_op(inst), Op::Return);
        assert_eq!(decode_a(inst), 255);
        assert_eq!(decode_b(inst), 255);
        assert_eq!(decode_c(inst), 255);

        let inst = encode_abx(Op::LoadConst, 0, 0xFFFF);
        assert_eq!(decode_bx(inst), 0xFFFF);
    }

    #[test]
    fn test_op_try_from_valid() {
        for v in 0..=67u8 {
            assert!(Op::try_from(v).is_ok(), "Op::try_from({v}) should succeed");
        }
        // Round-trip: value matches discriminant
        assert_eq!(Op::try_from(0).unwrap(), Op::LoadConst);
        assert_eq!(Op::try_from(67).unwrap(), Op::AgentExec);
    }

    #[test]
    fn test_op_try_from_invalid() {
        assert_eq!(Op::try_from(68), Err(68));
        assert_eq!(Op::try_from(255), Err(255));
    }
}
