pub struct EvaluationError {}

pub trait Expression {
    fn eval(&self) -> Result<Value, EvaluationError>;
}

pub struct LeafExpression<T> {
    data: T,
}

impl<T> LeafExpression<T> {
    pub fn new(value: T) -> LeafExpression<T> {
        LeafExpression { data: value }
    }
}

impl Expression for LeafExpression<u32> {
    fn eval(&self) -> Result<Value, EvaluationError> {
        Ok(Value::Integer(self.data))
    }
}

pub enum Operation {
    Plus,
    Minus,
    Multiply,
    Divide,
}

pub struct OperationExpression {
    pub operation: Operation,
    pub left: Box<dyn Expression>,
    pub right: Box<dyn Expression>,
}

impl Expression for OperationExpression {
    fn eval(&self) -> Result<Value, EvaluationError> {
        let l = self.left.eval()?;
        let r = self.right.eval()?;
        match self.operation {
            Operation::Plus => l.apply_plus(r),
            Operation::Minus => l.apply_minus(r),
            Operation::Multiply => todo!(),
            Operation::Divide => todo!(),
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum Value {
    Integer(u32),
    String(String),
}

impl Value {
    fn apply_plus(&self, other: Value) -> Result<Value, EvaluationError> {
        match (self, other) {
            (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l + r)),
            (Value::String(l), Value::Integer(r)) => {
                let mut concat = l.clone();
                concat.push_str(&r.to_string());
                Ok(Value::String(concat))
            }
            (l, r) => Err(EvaluationError {}),
        }
    }
    fn apply_minus(&self, other: Value) -> Result<Value, EvaluationError> {
        match (self, other) {
            (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l - r)),
            (l, r) => Err(EvaluationError {}),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_expr() {
        let expr: LeafExpression<u32> = LeafExpression::new(123);
        let v = expr.eval();
        assert!(v.is_ok());
    }

    #[test]
    fn test_operation() {
        let expr = OperationExpression {
            operation: Operation::Minus,
            left: Box::new(LeafExpression::new(1)),
            right: Box::new(LeafExpression::new(1)),
        };
        match expr.eval() {
            Ok(val) => match val {
                Value::Integer(v) => assert_eq!(v, 0),
                _ => panic!(),
            },
            Err(_) => panic!(),
        }
    }

    #[test]
    fn test_nested_operation() {
        let expr = OperationExpression {
            operation: Operation::Minus,
            left: Box::new(OperationExpression {
                operation: Operation::Plus,
                left: Box::new(LeafExpression::new(5)),
                right: Box::new(LeafExpression::new(15)),
            }),
            right: Box::new(LeafExpression::new(1)),
        };
        match expr.eval() {
            Ok(val) => match val {
                Value::Integer(v) => assert_eq!(v, 19),
                _ => panic!(),
            },
            Err(_) => panic!(),
        }
    }
}
