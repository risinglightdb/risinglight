use byteorder::LittleEndian;
use int_enum::IntEnum;
use positioned_io_preview::{ReadBytesAtExt, WriteBytesAtExt};
use std::mem::size_of;
use std::vec::Vec;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntEnum)]
pub enum LogType {
    NONE = 0,
    START = 1,
    COMMIT = 2,
}

// TODO: Add deserialize method
pub trait Log {
    fn serialize(&self) -> Vec<u8>;
}

pub struct StartTxnLog {
    log_type: LogType,
    txn_id: u64,
}

impl Log for StartTxnLog {
    fn serialize(&self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::new();
        let mut buf = [0u8; size_of::<u8>() + size_of::<u64>()];
        buf.as_mut()
            .write_u8_at(0, self.log_type.int_value())
            .unwrap();
        buf.as_mut()
            .write_u64_at::<LittleEndian>(1, self.txn_id)
            .unwrap();
        buffer.extend_from_slice(&buf);
        buffer
    }
}

impl StartTxnLog {
    pub fn new(txn_id: u64) -> StartTxnLog {
        StartTxnLog {
            log_type: LogType::START,
            txn_id: txn_id,
        }
    }
}

#[cfg(test)]
mod log_tests {
    use super::*;
    #[test]
    fn logtest() {
        let start_txn_log = StartTxnLog::new(3);
        let buffer = start_txn_log.serialize();
        let expected_buf: [u8; 9] = [1, 3, 0, 0, 0, 0, 0, 0, 0];
        for (pos, val) in buffer.iter().enumerate() {
            assert_eq!(*val, expected_buf[pos]);
        }
    }
}
