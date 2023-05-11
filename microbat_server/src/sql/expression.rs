use microbat_protocol::data_representation::{Data, DataError};

#[derive(Debug)]
pub struct EvaluationError {
    msg: String,
}

impl From<DataError> for EvaluationError {
    fn from(value: DataError) -> Self {
        EvaluationError { msg: value.msg } 
    }
}

pub trait Expression {
    fn eval(&self) -> Result<Data, EvaluationError>;
    fn visualize(&self) -> String;
}

#[derive(Debug)]
pub struct LeafExpression<T> {
    data: T,
}

impl<T> LeafExpression<T> {
    pub fn new(value: T) -> LeafExpression<T> {
        LeafExpression { data: value }
    }
}

impl Expression for LeafExpression<i32> {
    fn eval(&self) -> Result<Data, EvaluationError> {
        Ok(Data::Integer(self.data))
    }

    fn visualize(&self) -> String {
        self.data.to_string()
    }
}

#[derive(Debug)]
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
    fn eval(&self) -> Result<Data, EvaluationError> {
        let l = self.left.eval()?;
        let r = self.right.eval()?;
        match self.operation {
            Operation::Plus => Ok(l.apply_plus(r)?),
            Operation::Minus => Ok(l.apply_minus(r)?),
            Operation::Multiply => todo!(),
            Operation::Divide => todo!(),
        }
    }

    fn visualize(&self) -> String {
        let l = self.left.visualize();
        let r = self.right.visualize();
        let mut s = String::new();
        s.push_str("( ");
        s.push_str(&l);
        s.push_str(format!("{:?}", self.operation).as_str());
        s.push_str(&r);
        s.push_str(" )");
        s
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_expr() {
        let expr: LeafExpression<i32> = LeafExpression::new(123);
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
                Data::Integer(v) => assert_eq!(v, 0),
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
                Data::Integer(v) => assert_eq!(v, 19),
                _ => panic!(),
            },
            Err(_) => panic!(),
        }
    }
}
