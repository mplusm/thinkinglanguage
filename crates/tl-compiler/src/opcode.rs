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
}

/// Encode an ABC-format instruction: [op:8][A:8][B:8][C:8]
pub fn encode_abc(op: Op, a: u8, b: u8, c: u8) -> u32 {
    ((op as u32) << 24) | ((a as u32) << 16) | ((b as u32) << 8) | (c as u32)
}

/// Encode an ABx-format instruction: [op:8][A:8][Bx:16]
pub fn encode_abx(op: Op, a: u8, bx: u16) -> u32 {
    ((op as u32) << 24) | ((a as u32) << 16) | (bx as u32)
}

/// Decode opcode from instruction
pub fn decode_op(inst: u32) -> Op {
    // Safety: we control all encoded values
    unsafe { std::mem::transmute((inst >> 24) as u8) }
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
}
