use crate::core::clause::Clause;

#[derive(Debug, Clone)]
pub(crate) struct StreamSelector {
    pub stream_kind: &'static str,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexedClause<K> {
    pub kind: K,
    pub selectors: Vec<StreamSelector>,
}

pub(crate) fn indexed_clause<K>(
    kind: K,
    stream_kind: &'static str,
    values: Vec<Vec<u8>>,
) -> Option<IndexedClause<K>> {
    (!values.is_empty()).then(|| IndexedClause {
        kind,
        selectors: values
            .into_iter()
            .map(|value| StreamSelector { stream_kind, value })
            .collect(),
    })
}

pub(crate) fn single_selector_clause<K>(
    kind: K,
    stream_kind: &'static str,
    value: Vec<u8>,
) -> IndexedClause<K> {
    IndexedClause {
        kind,
        selectors: vec![StreamSelector { stream_kind, value }],
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedClause<K> {
    pub kind: K,
    pub stream_ids: Vec<String>,
    pub estimated_count: u64,
}

pub(crate) fn clause_values<T>(clause: &Clause<T>) -> Vec<Vec<u8>>
where
    T: Copy + Into<Vec<u8>>,
{
    match clause {
        Clause::Any => Vec::new(),
        Clause::One(value) => vec![(*value).into()],
        Clause::Or(values) => values.iter().copied().map(Into::into).collect(),
    }
}
