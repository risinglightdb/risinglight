use bytes::BufMut;

use super::BlockBuilder;
use crate::array::UTF8Array;

/// Encodes fixed-width char into a block.
///
/// Every record is composed of a item width and the actual data. For example,
///
/// ```plain
/// | length (1B, u8) | data (10B) |
/// ```
pub struct PlainCharBlockBuilder {
    data: Vec<u8>,
    char_width: usize,
    target_size: usize,
}

impl PlainCharBlockBuilder {
    pub fn new(target_size: usize, char_width: u64) -> Self {
        let data = Vec::with_capacity(target_size);
        assert!(char_width < 256);
        Self {
            data,
            char_width: char_width as usize,
            target_size,
        }
    }
}

impl BlockBuilder<UTF8Array> for PlainCharBlockBuilder {
    fn append(&mut self, item: Option<&str>) {
        let item = item
            .expect("nullable item found in non-nullable block builder")
            .as_bytes();
        if item.len() > self.char_width {
            panic!(
                "item length {} > char width {}",
                item.len(),
                self.char_width
            );
        }
        self.data.put_u8(item.len() as u8);
        self.data.extend(item);
        self.data.extend(
            [0].iter()
                .cycle()
                .take(self.char_width - item.len())
                .cloned(),
        );
    }

    fn estimated_size(&self) -> usize {
        self.data.len()
    }

    fn should_finish(&self, _next_item: &Option<&str>) -> bool {
        !self.data.is_empty()
            && self.estimated_size() + 1 /* length of an item */ + self.char_width
                > self.target_size
    }

    fn finish(self) -> Vec<u8> {
        self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_char() {
        let mut builder = PlainCharBlockBuilder::new(128, 40);
        builder.append(Some("233"));
        builder.append(Some("2333"));
        builder.append(Some("23333"));
        assert_eq!(builder.estimated_size(), 123);
        assert!(builder.should_finish(&Some("2333333")));
        builder.finish();
    }
}
