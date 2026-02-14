use std::fmt::{Display, Write};

pub(crate) trait IteratorJoin {
    fn join(self, sep: &str) -> String;
}

impl<T, I> IteratorJoin for T
where
    T: Iterator<Item = I>,
    I: Display,
{
    fn join(mut self, sep: &str) -> String {
        let (lower_bound, _) = self.size_hint();
        let mut out = String::with_capacity(sep.len() * lower_bound);

        if let Some(first_item) = self.next() {
            write!(out, "{}", first_item).unwrap();
        }

        for item in self {
            out.push_str(sep);
            write!(out, "{}", item).unwrap();
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_empty() {
        let v: Vec<String> = vec![];
        assert_eq!("", v.into_iter().join(", "));
    }

    #[test]
    fn join_single() {
        assert_eq!("hello", vec!["hello"].into_iter().join(", "));
    }

    #[test]
    fn join_multiple() {
        assert_eq!("a, b, c", vec!["a", "b", "c"].into_iter().join(", "));
    }

    #[test]
    fn join_numbers() {
        assert_eq!("1-2-3", vec![1, 2, 3].into_iter().join("-"));
    }
}
