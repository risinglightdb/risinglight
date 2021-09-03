use super::*;

macro_rules! try_match {
    ($e:expr, $pat:pat => $ok:expr, $desc:literal) => {
        match &$e {
            $pat => $ok,
            _ => return Err(ParseError::NotFound($desc)),
        }
    };
}

mod create;
