use quackdb_internal::value::ValueHandle;

#[derive(Debug)]
pub struct Value {
    pub handle: ValueHandle,
}

impl From<ValueHandle> for Value {
    fn from(value: ValueHandle) -> Self {
        Self { handle: value }
    }
}

impl Value {}
