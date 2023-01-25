// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

pub mod time;

pub mod pretty {
    use std::{borrow::Cow, collections::BTreeMap};

    use pretty_xmlish::{Pretty, XmlNode};

    pub fn named_record<'a>(
        name: impl Into<Cow<'a, str>>,
        fields: BTreeMap<&'a str, Pretty<'a>>,
        children: Vec<Pretty<'a>>,
    ) -> Pretty<'a> {
        let fields = fields.into_iter().map(|(k, v)| (k.into(), v)).collect();
        Pretty::Record(XmlNode::new(name.into(), fields, children))
    }
}
