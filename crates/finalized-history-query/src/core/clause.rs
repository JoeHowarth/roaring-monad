#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Clause<T> {
    Any,
    One(T),
    Or(Vec<T>),
}

impl<T> Clause<T> {
    pub fn or_terms(&self) -> usize {
        match self {
            Self::Any => 0,
            Self::One(_) => 1,
            Self::Or(values) => values.len(),
        }
    }
}
