// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

pub mod time;

pub mod pretty {
    use std::borrow::Cow;
    use std::collections::BTreeMap;

    use pretty_xmlish::Pretty;

    pub fn named_record<'a>(
        name: impl Into<Cow<'a, str>>,
        fields: BTreeMap<&'a str, Pretty<'a>>,
        children: Vec<Pretty<'a>>,
    ) -> Pretty<'a> {
        Pretty::simple_record(name, fields, children)
    }
}
