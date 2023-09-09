use quackdb_internal::vector::ValidityHandle;

#[derive(Debug)]
pub struct Validity {
    pub handle: ValidityHandle,
}

impl From<ValidityHandle> for Validity {
    fn from(value: ValidityHandle) -> Self {
        Self { handle: value }
    }
}

impl Validity {
    pub fn row_is_valid(&self, _row: u64) -> bool {
        todo!()
    }
    pub fn set_row_validity(&self, _row: u64, _valid: bool) {
        todo!()
    }
}
