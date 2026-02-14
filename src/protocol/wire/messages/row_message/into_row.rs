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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_element_row() {
        let row = 42i32.into_row();
        assert_eq!(1, row.len());
    }

    #[test]
    fn two_element_row() {
        let row = (1i32, "hello").into_row();
        assert_eq!(2, row.len());
    }

    #[test]
    fn three_element_row() {
        let row = (1i32, 2i64, true).into_row();
        assert_eq!(3, row.len());
    }

    #[test]
    fn four_element_row() {
        let row = (1i32, 2i32, 3i32, 4i32).into_row();
        assert_eq!(4, row.len());
    }

    #[test]
    fn five_element_row() {
        let row = (1i32, 2i32, 3i32, 4i32, 5i32).into_row();
        assert_eq!(5, row.len());
    }

    #[test]
    fn six_element_row() {
        let row = (1i32, 2i32, 3i32, 4i32, 5i32, 6i32).into_row();
        assert_eq!(6, row.len());
    }

    #[test]
    fn seven_element_row() {
        let row = (1i32, 2i32, 3i32, 4i32, 5i32, 6i32, 7i32).into_row();
        assert_eq!(7, row.len());
    }

    #[test]
    fn eight_element_row() {
        let row = (1i32, 2i32, 3i32, 4i32, 5i32, 6i32, 7i32, 8i32).into_row();
        assert_eq!(8, row.len());
    }

    #[test]
    fn nine_element_row() {
        let row = (1i32, 2i32, 3i32, 4i32, 5i32, 6i32, 7i32, 8i32, 9i32).into_row();
        assert_eq!(9, row.len());
    }

    #[test]
    fn ten_element_row() {
        let row = (1i32, 2i32, 3i32, 4i32, 5i32, 6i32, 7i32, 8i32, 9i32, 10i32).into_row();
        assert_eq!(10, row.len());
    }

    #[test]
    fn mixed_types_row() {
        let row = (42i32, "hello", true, 3.14f64).into_row();
        assert_eq!(4, row.len());
    }
}
