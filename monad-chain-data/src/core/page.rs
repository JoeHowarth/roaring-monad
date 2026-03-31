#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryOrder {
    #[default]
    Ascending,
    Descending,
}
