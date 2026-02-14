use crate::{IntoSqlOwned, RowMessage};

/// create a RowMessage from list of values
pub trait IntoRowMessage<'a> {
    /// create a RowMessage from list of values which implements IntoSQL
    fn into_row(self) -> RowMessage<'a>;
}

impl<'a, A> IntoRowMessage<'a> for A
where
    A: IntoSqlOwned<'a>,
{
    fn into_row(self) -> RowMessage<'a> {
        let mut row = RowMessage::new();
        row.push(self.into_sql());
        row
    }
}

impl<'a, A, B> IntoRowMessage<'a> for (A, B)
where
    A: IntoSqlOwned<'a>,
    B: IntoSqlOwned<'a>,
{
    fn into_row(self) -> RowMessage<'a> {
        let mut row = RowMessage::new();
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row
    }
}

impl<'a, A, B, C> IntoRowMessage<'a> for (A, B, C)
where
    A: IntoSqlOwned<'a>,
    B: IntoSqlOwned<'a>,
    C: IntoSqlOwned<'a>,
{
    fn into_row(self) -> RowMessage<'a> {
        let mut row = RowMessage::new();
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row
    }
}

impl<'a, A, B, C, D> IntoRowMessage<'a> for (A, B, C, D)
where
    A: IntoSqlOwned<'a>,
    B: IntoSqlOwned<'a>,
    C: IntoSqlOwned<'a>,
    D: IntoSqlOwned<'a>,
{
    fn into_row(self) -> RowMessage<'a> {
        let mut row = RowMessage::new();
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row
    }
}

impl<'a, A, B, C, D, E> IntoRowMessage<'a> for (A, B, C, D, E)
where
    A: IntoSqlOwned<'a>,
    B: IntoSqlOwned<'a>,
    C: IntoSqlOwned<'a>,
    D: IntoSqlOwned<'a>,
    E: IntoSqlOwned<'a>,
{
    fn into_row(self) -> RowMessage<'a> {
        let mut row = RowMessage::new();
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row.push(self.4.into_sql());
        row
    }
}

impl<'a, A, B, C, D, E, F> IntoRowMessage<'a> for (A, B, C, D, E, F)
where
    A: IntoSqlOwned<'a>,
    B: IntoSqlOwned<'a>,
    C: IntoSqlOwned<'a>,
    D: IntoSqlOwned<'a>,
    E: IntoSqlOwned<'a>,
    F: IntoSqlOwned<'a>,
{
    fn into_row(self) -> RowMessage<'a> {
        let mut row = RowMessage::new();
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row.push(self.4.into_sql());
        row.push(self.5.into_sql());
        row
    }
}

impl<'a, A, B, C, D, E, F, G> IntoRowMessage<'a> for (A, B, C, D, E, F, G)
where
    A: IntoSqlOwned<'a>,
    B: IntoSqlOwned<'a>,
    C: IntoSqlOwned<'a>,
    D: IntoSqlOwned<'a>,
    E: IntoSqlOwned<'a>,
    F: IntoSqlOwned<'a>,
    G: IntoSqlOwned<'a>,
{
    fn into_row(self) -> RowMessage<'a> {
        let mut row = RowMessage::new();
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row.push(self.4.into_sql());
        row.push(self.5.into_sql());
        row.push(self.6.into_sql());
        row
    }
}

impl<'a, A, B, C, D, E, F, G, H> IntoRowMessage<'a> for (A, B, C, D, E, F, G, H)
where
    A: IntoSqlOwned<'a>,
    B: IntoSqlOwned<'a>,
    C: IntoSqlOwned<'a>,
    D: IntoSqlOwned<'a>,
    E: IntoSqlOwned<'a>,
    F: IntoSqlOwned<'a>,
    G: IntoSqlOwned<'a>,
    H: IntoSqlOwned<'a>,
{
    fn into_row(self) -> RowMessage<'a> {
        let mut row = RowMessage::new();
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row.push(self.4.into_sql());
        row.push(self.5.into_sql());
        row.push(self.6.into_sql());
        row.push(self.7.into_sql());
        row
    }
}

impl<'a, A, B, C, D, E, F, G, H, I> IntoRowMessage<'a> for (A, B, C, D, E, F, G, H, I)
where
    A: IntoSqlOwned<'a>,
    B: IntoSqlOwned<'a>,
    C: IntoSqlOwned<'a>,
    D: IntoSqlOwned<'a>,
    E: IntoSqlOwned<'a>,
    F: IntoSqlOwned<'a>,
    G: IntoSqlOwned<'a>,
    H: IntoSqlOwned<'a>,
    I: IntoSqlOwned<'a>,
{
    fn into_row(self) -> RowMessage<'a> {
        let mut row = RowMessage::new();
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row.push(self.4.into_sql());
        row.push(self.5.into_sql());
        row.push(self.6.into_sql());
        row.push(self.7.into_sql());
        row.push(self.8.into_sql());
        row
    }
}

impl<'a, A, B, C, D, E, F, G, H, I, J> IntoRowMessage<'a> for (A, B, C, D, E, F, G, H, I, J)
where
    A: IntoSqlOwned<'a>,
    B: IntoSqlOwned<'a>,
    C: IntoSqlOwned<'a>,
    D: IntoSqlOwned<'a>,
    E: IntoSqlOwned<'a>,
    F: IntoSqlOwned<'a>,
    G: IntoSqlOwned<'a>,
    H: IntoSqlOwned<'a>,
    I: IntoSqlOwned<'a>,
    J: IntoSqlOwned<'a>,
{
    fn into_row(self) -> RowMessage<'a> {
        let mut row = RowMessage::new();
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row.push(self.4.into_sql());
        row.push(self.5.into_sql());
        row.push(self.6.into_sql());
        row.push(self.7.into_sql());
        row.push(self.8.into_sql());
        row.push(self.9.into_sql());
        row
    }
}
