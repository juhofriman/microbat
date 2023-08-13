use microbat_protocol::data::{
    data_values::{DataError, MData, MDataType},
    table_model::{Column, TableSchema},
};

#[derive(Debug)]
pub struct EvaluationError {
    pub msg: String,
}

impl From<DataError> for EvaluationError {
    fn from(value: DataError) -> Self {
        EvaluationError { msg: value.msg }
    }
}

pub trait Expression {
    fn schema_column(&self, schema: &TableSchema, index: usize) -> Result<Column, EvaluationError>;
    fn eval(&self, schema: &TableSchema, row: &Vec<MData>) -> Result<MData, EvaluationError>;
}

pub struct AsExpression {
    name: String,
    expression: Box<dyn Expression>,
}

impl AsExpression {
    pub fn new(name: String, expression: Box<dyn Expression>) -> Self {
        Self { name, expression }
    }
}

impl Expression for AsExpression {
    fn schema_column(&self, schema: &TableSchema, index: usize) -> Result<Column, EvaluationError> {
        let sub = self.expression.schema_column(schema, index)?;
        Ok(Column::new(self.name.clone(), sub.data_type.clone()))
    }

    fn eval(&self, schema: &TableSchema, row: &Vec<MData>) -> Result<MData, EvaluationError> {
        self.expression.eval(schema, row)
    }
}

#[derive(Debug)]
pub struct ReferenceExpression {
    name: String,
}

impl ReferenceExpression {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl Expression for ReferenceExpression {
    fn eval(&self, schema: &TableSchema, row: &Vec<MData>) -> Result<MData, EvaluationError> {
        match schema
            .columns
            .iter()
            .position(|r| r.name.to_uppercase() == self.name)
        {
            Some(index) => Ok(row.get(index).unwrap().clone()),
            None => Err(EvaluationError {
                msg: format!("No such column {}", self.name),
            }),
        }
    }

    fn schema_column(
        &self,
        schema: &TableSchema,
        _index: usize,
    ) -> Result<Column, EvaluationError> {
        match schema
            .columns
            .iter()
            .find(|c| c.name.to_uppercase() == self.name)
        {
            Some(column) => Ok(Column::new(self.name.clone(), column.data_type.clone())),
            None => Err(EvaluationError {
                msg: format!("No such column {}", self.name),
            }),
        }
    }
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
    fn eval(&self, _schema: &TableSchema, _row: &Vec<MData>) -> Result<MData, EvaluationError> {
        Ok(MData::Integer(self.data))
    }

    fn schema_column(
        &self,
        _schema: &TableSchema,
        index: usize,
    ) -> Result<Column, EvaluationError> {
        Ok(Column::new(format!("column_{}", index), MDataType::Integer))
    }
}

pub struct NegateExpression {
    pub expression: Box<dyn Expression>,
}

impl Expression for NegateExpression {
    fn eval(&self, schema: &TableSchema, row: &Vec<MData>) -> Result<MData, EvaluationError> {
        let val = self.expression.eval(schema, row)?;
        match val {
            MData::Null => todo!(),
            MData::Integer(v) => Ok(MData::Integer(-v)),
            MData::Varchar(_) => todo!(),
        }
    }

    fn schema_column(&self, schema: &TableSchema, index: usize) -> Result<Column, EvaluationError> {
        self.expression.schema_column(schema, index)
    }
}

#[derive(Debug)]
pub enum Operation {
    Plus,
    Minus,
}

pub struct OperationExpression {
    pub operation: Operation,
    pub left: Box<dyn Expression>,
    pub right: Box<dyn Expression>,
}

impl Expression for OperationExpression {
    fn eval(&self, schema: &TableSchema, row: &Vec<MData>) -> Result<MData, EvaluationError> {
        let l = self.left.eval(schema, row)?;
        let r = self.right.eval(schema, row)?;
        match self.operation {
            Operation::Plus => Ok(l.apply_plus(r)?),
            Operation::Minus => Ok(l.apply_minus(r)?),
        }
    }

    fn schema_column(
        &self,
        _schema: &TableSchema,
        index: usize,
    ) -> Result<Column, EvaluationError> {
        // TODO: this is absolutely not correct
        Ok(Column::new(format!("column_{}", index), MDataType::Integer))
    }
}
