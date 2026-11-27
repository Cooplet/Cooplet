use std::{
    fmt::{self, Write as _},
    io::{self, Write as _},
    sync::atomic::{AtomicU64, Ordering},
};

use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};

pub fn install() {
    let _ = tracing::subscriber::set_global_default(Stderr {
        next_span: AtomicU64::new(1),
    });
}

struct Stderr {
    next_span: AtomicU64,
}

impl Subscriber for Stderr {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= &tracing::Level::WARN
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(self.next_span.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let metadata = event.metadata();
        let mut fields = LogFields(format!("{} {}", metadata.level(), metadata.target()));
        event.record(&mut fields);
        let _ = writeln!(io::stderr().lock(), "{}", fields.0);
    }
}

struct LogFields(String);

impl Visit for LogFields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        let _ = write!(self.0, " {}={value:?}", field.name());
    }
}
