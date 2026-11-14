use std::cell::RefCell;
use std::ops::Range;

use dbflux_byte_source::{ByteSource, SourceError};

/// Records every range a reader asks for, in order.
pub(crate) struct CountingSource<S> {
    inner: S,
    requests: RefCell<Vec<Range<u64>>>,
}

impl<S> CountingSource<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self {
            inner,
            requests: RefCell::new(Vec::new()),
        }
    }

    pub(crate) fn requests(&self) -> Vec<Range<u64>> {
        self.requests.borrow().clone()
    }

    pub(crate) fn clear_requests(&self) {
        self.requests.borrow_mut().clear();
    }

    pub(crate) fn bytes_read(&self) -> u64 {
        self.requests
            .borrow()
            .iter()
            .map(|range| range.end - range.start)
            .sum()
    }
}

impl<S: ByteSource> ByteSource for CountingSource<S> {
    fn byte_length(&self) -> Result<u64, SourceError> {
        self.inner.byte_length()
    }

    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        self.requests.borrow_mut().push(range.clone());

        self.inner.read_range(range)
    }
}
