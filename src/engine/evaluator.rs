use crate::error::FlowError;
use crate::sql::ast::{BinOp, ColumnRef, Expr, Literal};
use crate::types::{Row, Value};

/// Evaluate a FlowQL `Expr` against a single `Row`, returning a `Value`.
pub struct Evaluator;

impl Evaluator {
    pub fn eval(expr: &Expr, row: &Row) -> Result<Value, FlowError> {
        match expr {
            Expr::Literal(lit) => Ok(Self::eval_literal(lit)),

            Expr::Column(col_ref) => {
                let col_name = Self::resolve_column(col_ref);
                Ok(row.get_or_null(&col_name))
            }

            Expr::BinOp { op, left, right } => {
                let l = Self::eval(left, row)?;
                let r = Self::eval(right, row)?;
                Self::eval_binop(op, l, r)
            }

            Expr::Not(inner) => {
                let v = Self::eval(inner, row)?;
                Ok(Value::Bool(!v.is_truthy()))
            }

            Expr::Fuzzy { column, pattern } => {
                let col_name = Self::resolve_column(column);
                let val = row.get_or_null(&col_name);
                if let Value::Text(text) = &val {
                    let score = trigram_similarity(text, pattern);
                    Ok(Value::Bool(score >= 0.3)) // threshold: 30% trigram overlap
                } else {
                    Ok(Value::Bool(false))
                }
            }
        }
    }

    /// Evaluate a WHERE predicate — returns `true` if the row passes the filter.
    pub fn matches_filter(filter: &Expr, row: &Row) -> Result<bool, FlowError> {
        let result = Self::eval(filter, row)?;
        Ok(result.is_truthy())
    }

    fn eval_literal(lit: &Literal) -> Value {
        match lit {
            Literal::Int(n)    => Value::Int(*n),
            Literal::Float(f)  => Value::Float(*f),
            Literal::Text(s)   => Value::Text(s.clone()),
            Literal::Bool(b)   => Value::Bool(*b),
            Literal::Null      => Value::Null,
            Literal::Vector(v) => Value::Vector(v.clone()),
        }
    }

    fn resolve_column(col_ref: &ColumnRef) -> String {
        // When a table qualifier is present (e.g. "a.name" in graph queries),
        // use "table.column" so aliased rows (e.g. graph-merged rows) are found.
        match &col_ref.table {
            Some(t) => format!("{}.{}", t, col_ref.column),
            None    => col_ref.column.clone(),
        }
    }

    fn eval_binop(op: &BinOp, left: Value, right: Value) -> Result<Value, FlowError> {
        match op {
            // ── Logical ──────────────────────────────────────────────────
            BinOp::And => Ok(Value::Bool(left.is_truthy() && right.is_truthy())),
            BinOp::Or  => Ok(Value::Bool(left.is_truthy() || right.is_truthy())),

            // ── Comparison ───────────────────────────────────────────────
            BinOp::Eq => Ok(Value::Bool(Self::values_equal(&left, &right))),
            BinOp::Ne => Ok(Value::Bool(!Self::values_equal(&left, &right))),
            BinOp::Lt => Ok(Value::Bool(
                left.partial_cmp_val(&right)
                    .map(|o| o == std::cmp::Ordering::Less)
                    .unwrap_or(false),
            )),
            BinOp::Le => Ok(Value::Bool(
                left.partial_cmp_val(&right)
                    .map(|o| o != std::cmp::Ordering::Greater)
                    .unwrap_or(false),
            )),
            BinOp::Gt => Ok(Value::Bool(
                left.partial_cmp_val(&right)
                    .map(|o| o == std::cmp::Ordering::Greater)
                    .unwrap_or(false),
            )),
            BinOp::Ge => Ok(Value::Bool(
                left.partial_cmp_val(&right)
                    .map(|o| o != std::cmp::Ordering::Less)
                    .unwrap_or(false),
            )),

            // ── Arithmetic ───────────────────────────────────────────────
            BinOp::Add => Self::arith_add(left, right),
            BinOp::Sub => Self::arith_sub(left, right),
            BinOp::Mul => Self::arith_mul(left, right),
            BinOp::Div => Self::arith_div(left, right),
        }
    }

    fn values_equal(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Null, Value::Null) => true,
            (Value::Null, _) | (_, Value::Null) => false,
            (Value::Int(x), Value::Int(y))     => x == y,
            (Value::Float(x), Value::Float(y)) => x == y,
            (Value::Int(x), Value::Float(y))   => (*x as f64) == *y,
            (Value::Float(x), Value::Int(y))   => *x == (*y as f64),
            (Value::Text(x), Value::Text(y))   => x == y,
            (Value::Bool(x), Value::Bool(y))   => x == y,
            _ => false,
        }
    }

    fn arith_add(l: Value, r: Value) -> Result<Value, FlowError> {
        match (l, r) {
            (Value::Int(a), Value::Int(b))     => Ok(Value::Int(a + b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
            (Value::Int(a), Value::Float(b))   => Ok(Value::Float(a as f64 + b)),
            (Value::Float(a), Value::Int(b))   => Ok(Value::Float(a + b as f64)),
            (Value::Text(a), Value::Text(b))   => Ok(Value::Text(a + &b)),
            (l, r) => Err(FlowError::type_err(format!(
                "cannot add `{}` and `{}`", l.type_name(), r.type_name()
            ))),
        }
    }

    fn arith_sub(l: Value, r: Value) -> Result<Value, FlowError> {
        match (l, r) {
            (Value::Int(a), Value::Int(b))     => Ok(Value::Int(a - b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
            (Value::Int(a), Value::Float(b))   => Ok(Value::Float(a as f64 - b)),
            (Value::Float(a), Value::Int(b))   => Ok(Value::Float(a - b as f64)),
            (l, r) => Err(FlowError::type_err(format!(
                "cannot subtract `{}` from `{}`", r.type_name(), l.type_name()
            ))),
        }
    }

    fn arith_mul(l: Value, r: Value) -> Result<Value, FlowError> {
        match (l, r) {
            (Value::Int(a), Value::Int(b))     => Ok(Value::Int(a * b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
            (Value::Int(a), Value::Float(b))   => Ok(Value::Float(a as f64 * b)),
            (Value::Float(a), Value::Int(b))   => Ok(Value::Float(a * b as f64)),
            (l, r) => Err(FlowError::type_err(format!(
                "cannot multiply `{}` and `{}`", l.type_name(), r.type_name()
            ))),
        }
    }

    fn arith_div(l: Value, r: Value) -> Result<Value, FlowError> {
        match (l, r) {
            (Value::Int(a), Value::Int(b)) => {
                if b == 0 { return Err(FlowError::type_err("division by zero")); }
                Ok(Value::Int(a / b))
            }
            (Value::Float(a), Value::Float(b)) => {
                if b == 0.0 { return Err(FlowError::type_err("division by zero")); }
                Ok(Value::Float(a / b))
            }
            (Value::Int(a), Value::Float(b)) => {
                if b == 0.0 { return Err(FlowError::type_err("division by zero")); }
                Ok(Value::Float(a as f64 / b))
            }
            (Value::Float(a), Value::Int(b)) => {
                if b == 0 { return Err(FlowError::type_err("division by zero")); }
                Ok(Value::Float(a / b as f64))
            }
            (l, r) => Err(FlowError::type_err(format!(
                "cannot divide `{}` by `{}`", l.type_name(), r.type_name()
            ))),
        }
    }
}

// ── Trigram fuzzy similarity ───────────────────────────────────────────────

/// Compute Dice coefficient of trigrams between two strings.
/// Returns 0.0 (no overlap) to 1.0 (identical).
pub fn trigram_similarity(a: &str, b: &str) -> f64 {
    let ta: std::collections::HashSet<[char; 3]> = trigrams(a).into_iter().collect();
    let tb: std::collections::HashSet<[char; 3]> = trigrams(b).into_iter().collect();
    if ta.is_empty() || tb.is_empty() {
        // Fall back to simple substring check for very short strings
        return if a.to_lowercase().contains(&b.to_lowercase()) || b.to_lowercase().contains(&a.to_lowercase()) {
            1.0
        } else {
            0.0
        };
    }
    let intersection = ta.intersection(&tb).count();
    2.0 * intersection as f64 / (ta.len() + tb.len()) as f64
}

fn trigrams(s: &str) -> Vec<[char; 3]> {
    let padded: Vec<char> = format!("  {}  ", s.to_lowercase()).chars().collect();
    padded.windows(3).map(|w| [w[0], w[1], w[2]]).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{ColumnRef, Expr, Literal};
    use std::collections::HashMap;

    fn make_row(fields: Vec<(&str, Value)>) -> Row {
        let mut map = HashMap::new();
        for (k, v) in fields {
            map.insert(k.to_string(), v);
        }
        Row::new(map)
    }

    #[test]
    fn test_eq_filter() {
        let row = make_row(vec![("age", Value::Int(25))]);
        let expr = Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Column(ColumnRef::simple("age"))),
            right: Box::new(Expr::Literal(Literal::Int(25))),
        };
        assert!(Evaluator::matches_filter(&expr, &row).unwrap());
    }

    #[test]
    fn test_and_filter() {
        let row = make_row(vec![
            ("age", Value::Int(25)),
            ("active", Value::Bool(true)),
        ]);
        let expr = Expr::BinOp {
            op: BinOp::And,
            left: Box::new(Expr::BinOp {
                op: BinOp::Gt,
                left: Box::new(Expr::Column(ColumnRef::simple("age"))),
                right: Box::new(Expr::Literal(Literal::Int(18))),
            }),
            right: Box::new(Expr::BinOp {
                op: BinOp::Eq,
                left: Box::new(Expr::Column(ColumnRef::simple("active"))),
                right: Box::new(Expr::Literal(Literal::Bool(true))),
            }),
        };
        assert!(Evaluator::matches_filter(&expr, &row).unwrap());
    }

    #[test]
    fn test_trigram_similarity() {
        let score = trigram_similarity("Alice", "Alice");
        assert!(score > 0.9);
        let score2 = trigram_similarity("Alice", "Alise");
        assert!(score2 > 0.5);
        let score3 = trigram_similarity("Alice", "xyz123");
        assert!(score3 < 0.3);
    }

    #[test]
    fn test_division_by_zero() {
        let row = make_row(vec![]);
        let expr = Expr::BinOp {
            op: BinOp::Div,
            left: Box::new(Expr::Literal(Literal::Int(10))),
            right: Box::new(Expr::Literal(Literal::Int(0))),
        };
        assert!(Evaluator::eval(&expr, &row).is_err());
    }
}
