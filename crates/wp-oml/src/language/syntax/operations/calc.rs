use crate::language::DirectAccessor;
use crate::language::prelude::*;
use derive_getters::Getters;

#[derive(Clone, Debug, PartialEq)]
pub enum CalcNumber {
    Digit(i64),
    Float(f64),
}

impl Display for CalcNumber {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CalcNumber::Digit(v) => write!(f, "{}", v),
            CalcNumber::Float(v) => {
                if v.fract() == 0.0 {
                    write!(f, "{:.1}", v)
                } else {
                    write!(f, "{}", v)
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum CalcOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl Display for CalcOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let symbol = match self {
            CalcOp::Add => "+",
            CalcOp::Sub => "-",
            CalcOp::Mul => "*",
            CalcOp::Div => "/",
            CalcOp::Mod => "%",
        };
        write!(f, "{}", symbol)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum CalcFun {
    Abs,
    Round,
    Floor,
    Ceil,
}

impl Display for CalcFun {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            CalcFun::Abs => "abs",
            CalcFun::Round => "round",
            CalcFun::Floor => "floor",
            CalcFun::Ceil => "ceil",
        };
        write!(f, "{}", name)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum CalcExpr {
    Const(CalcNumber),
    Accessor(DirectAccessor),
    UnaryNeg(Box<CalcExpr>),
    Binary {
        op: CalcOp,
        lhs: Box<CalcExpr>,
        rhs: Box<CalcExpr>,
    },
    Func {
        fun: CalcFun,
        arg: Box<CalcExpr>,
    },
}

impl Display for CalcExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CalcExpr::Const(v) => write!(f, "{}", v),
            CalcExpr::Accessor(acc) => write!(f, "{}", acc),
            CalcExpr::UnaryNeg(expr) => write!(f, "-{}", expr),
            CalcExpr::Binary { op, lhs, rhs } => write!(f, "({} {} {})", lhs, op, rhs),
            CalcExpr::Func { fun, arg } => write!(f, "{}({})", fun, arg),
        }
    }
}

#[derive(Clone, Debug, Getters, PartialEq)]
pub struct CalcOperation {
    expr: CalcExpr,
}

impl CalcOperation {
    pub fn new(expr: CalcExpr) -> Self {
        Self { expr }
    }

    pub fn expr_mut(&mut self) -> &mut CalcExpr {
        &mut self.expr
    }
}

impl Display for CalcOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "calc({})", self.expr)
    }
}
