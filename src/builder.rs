use log::trace;
use native_tls::Certificate;
use random_word::Lang;
use support::QueryHolder;
use tokio_util::sync::CancellationToken;

use crate::upsert::UpsertQuickStream;

pub mod support;

#[derive(Clone)]
pub struct QuickStreamBuilder {
    cancellation_token: Option<CancellationToken>,
    max_con_count: Option<usize>,
    buffer_size: Option<usize>,
    single_digits: Option<usize>,
    tens: Option<usize>,
    hundreds: Option<usize>,
    db_config: Option<tokio_postgres::Config>,
    tls: Option<Certificate>,
    queries: Option<QueryHolder>,
    max_records_per_cycle_batch: Option<usize>, //a batch = introduced_lag_cycles
    introduced_lag_cycles: Option<usize>,
    introduced_lag_in_millies: Option<u64>,
    connection_creation_threshold: Option<f64>,
    name: Option<String>,
    print_connection_configuration: bool
}

impl Default for QuickStreamBuilder {
    fn default() -> Self {
        Self {
            cancellation_token: None,
            max_con_count: None,
            buffer_size: None,
            single_digits: None,
            tens: None,
            hundreds: None,
            db_config: None,
            tls: None,
            queries: None,
            max_records_per_cycle_batch: None,
            introduced_lag_cycles: None,
            introduced_lag_in_millies: None,
            connection_creation_threshold: None,
            name: Some(format!("{}_{}", random_word::gen(Lang::En), random_word::gen(Lang::En))),
            print_connection_configuration: false
        }
    }
}

impl QuickStreamBuilder {
    pub fn cancellation_tocken(&mut self, cancellation_token: CancellationToken) -> &mut Self {
        self.cancellation_token = Some(cancellation_token);
        self
    }

    pub fn max_connection_count(&mut self, max_con_count: usize) -> &mut Self {
        self.max_con_count = Some(max_con_count);
        self
    }

    pub fn buffer_size(&mut self, buffer_size: usize) -> &mut Self {
        self.buffer_size = Some(buffer_size);
        self
    }

    pub fn single_digits(&mut self, single_digits: usize) -> &mut Self {
        self.single_digits = Some(single_digits);
        self
    }

    pub fn tens(&mut self, tens: usize) -> &mut Self {
        self.tens = Some(tens);
        self
    }

    pub fn hundreds(&mut self, hundreds: usize) -> &mut Self {
        self.hundreds = Some(hundreds);
        self
    }

    pub fn db_config(&mut self, db_config: tokio_postgres::Config) -> &mut Self {
        self.db_config = Some(db_config);
        self
    }

    pub fn tls(&mut self, tls: Certificate) -> &mut Self {
        self.tls = Some(tls);
        self
    }

    pub fn queries(&mut self, queries: QueryHolder) -> &mut Self {
        self.queries = Some(queries);
        self
    }

    pub fn max_records_per_cycle_batch(&mut self, max_records_per_cycle_batch: usize) -> &mut Self {
        self.max_records_per_cycle_batch = Some(max_records_per_cycle_batch);
        self
    }

    pub fn introduced_lag_cycles(&mut self, introduced_lag_cycles: usize) -> &mut Self {
        self.introduced_lag_cycles = Some(introduced_lag_cycles);
        self
    }

    pub fn introduced_lag_in_millies(&mut self, introduced_lag_in_millies: u64) -> &mut Self {
        self.introduced_lag_in_millies = Some(introduced_lag_in_millies);
        self
    }

    pub fn connection_creation_threshold(&mut self, connection_creation_threshold: f64) -> &mut Self {
        self.connection_creation_threshold = Some(connection_creation_threshold);
        self
    }

    pub fn name(&mut self, name: String) -> &mut Self {
        self.name = Some(name);
        self
    }

    /**
     This will print the current connections configuration end of every lag cycle.
     * ***Default behaviour is to print the current connections confguration iff it changed***
     */
    pub fn print_connection_configuration(&mut self) -> &mut Self {
        self.print_connection_configuration = true;
        self
    }

    pub fn build_update(self) -> UpsertQuickStream {
        trace!("building UpsertQuickStream from builder");
        UpsertQuickStream {
            cancellation_token: self.cancellation_token.expect("cancellation_token is None"),
            max_con_count: self.max_con_count.expect("max_con_count is None"),
            buffer_size: self.buffer_size.expect("buffer_size is None"),
            single_digits: self.single_digits.expect("single_digits is None"),
            tens: self.tens.expect("tens is None"),
            hundreds: self.hundreds.expect("hundreds is None"),
            db_config: self.db_config.expect("db_config is None"),
            tls: self.tls,
            queries: self.queries.expect("queries is None"),
            max_records_per_cycle_batch: self.max_records_per_cycle_batch.expect("max_records_per_cycle_batch is None"),
            introduced_lag_cycles: self.introduced_lag_cycles.expect("introduced_lag_cycles is None"),
            introduced_lag_in_millies: self.introduced_lag_in_millies.expect("introduced_lag_in_millies is None"),
            connection_creation_threshold: self.connection_creation_threshold.expect("connection_creation_threshold is None"),
            name: self.name.expect("not a possible scenario"),
            print_con_config: self.print_connection_configuration
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio_postgres::Config;
    use tokio_util::sync::CancellationToken;

    use super::{support::QueryHolder, QuickStreamBuilder};

#[test]
    fn test_builder() {
        let cancellation_token = CancellationToken::new();

        let db_config = Config::new();
        let queries = QueryHolder::default();

        let mut builder = QuickStreamBuilder::default();
        builder
            .cancellation_tocken(cancellation_token.clone())
            .max_connection_count(2)
            .buffer_size(10)
            .single_digits(1)
            .tens(2)
            .hundreds(1)
            .db_config(db_config.clone())
            .queries(queries.clone())
            .max_records_per_cycle_batch(10)
            .introduced_lag_cycles(2)
            .introduced_lag_in_millies(10)
            .connection_creation_threshold(15.0)
            .print_connection_configuration();

        let upsert_processor = builder.build_update();
        
        assert_eq!(upsert_processor.max_con_count, 2);
        assert_eq!(upsert_processor.buffer_size, 10);
        assert_eq!(upsert_processor.single_digits, 1);
        assert_eq!(upsert_processor.tens, 2);
        assert_eq!(upsert_processor.hundreds, 1);
        // Compare the string representation of db_config as Config doesn't implement PartialEq
        assert_eq!(format!("{:?}", upsert_processor.db_config), format!("{:?}", db_config));
        // Compare the string representation of queries as it may not implement PartialEq
        assert_eq!(format!("{:?}", upsert_processor.queries), format!("{:?}", queries));
        assert_eq!(upsert_processor.max_records_per_cycle_batch, 10);
        assert_eq!(upsert_processor.introduced_lag_cycles, 2);
        assert_eq!(upsert_processor.introduced_lag_in_millies, 10);
        assert_eq!(upsert_processor.connection_creation_threshold, 15.0);
        assert_eq!(upsert_processor.print_con_config, true);
        assert_eq!(upsert_processor.cancellation_token.is_cancelled(), cancellation_token.is_cancelled());

        cancellation_token.cancel();
        assert_eq!(upsert_processor.cancellation_token.is_cancelled(), cancellation_token.is_cancelled());
    }

    #[test]
    #[should_panic(expected = "cancellation_token is None")]
    fn test_missing_cancellation_token() {
        let mut builder = QuickStreamBuilder::default();
        builder
            .max_connection_count(2)
            .buffer_size(10)
            .single_digits(1)
            .tens(2)
            .hundreds(1)
            .db_config(Config::new())
            .queries(QueryHolder::default())
            .max_records_per_cycle_batch(10)
            .introduced_lag_cycles(2)
            .introduced_lag_in_millies(10)
            .connection_creation_threshold(15.0)
            .name("test".to_string())
            .print_connection_configuration();

        let _ = builder.build_update();
    }

    #[test]
    #[should_panic(expected = "max_con_count is None")]
    fn test_missing_max_con_count() {
        let cancellation_token = CancellationToken::new();
        let mut builder = QuickStreamBuilder::default();
        builder
            .cancellation_tocken(cancellation_token)
            .buffer_size(10)
            .single_digits(1)
            .tens(2)
            .hundreds(1)
            .db_config(Config::new())
            .queries(QueryHolder::default())
            .max_records_per_cycle_batch(10)
            .introduced_lag_cycles(2)
            .introduced_lag_in_millies(10)
            .connection_creation_threshold(15.0)
            .name("test".to_string())
            .print_connection_configuration();

        let _ = builder.build_update();
    }

    #[test]
    #[should_panic(expected = "buffer_size is None")]
    fn test_missing_buffer_size() {
        let cancellation_token = CancellationToken::new();
        let mut builder = QuickStreamBuilder::default();
        builder
            .cancellation_tocken(cancellation_token)
            .max_connection_count(2)
            .single_digits(1)
            .tens(2)
            .hundreds(1)
            .db_config(Config::new())
            .queries(QueryHolder::default())
            .max_records_per_cycle_batch(10)
            .introduced_lag_cycles(2)
            .introduced_lag_in_millies(10)
            .connection_creation_threshold(15.0)
            .name("test".to_string())
            .print_connection_configuration();

        let _ = builder.build_update();
    }

    #[test]
    #[should_panic(expected = "single_digits is None")]
    fn test_missing_single_digits() {
        let cancellation_token = CancellationToken::new();
        let mut builder = QuickStreamBuilder::default();
        builder
            .cancellation_tocken(cancellation_token)
            .max_connection_count(2)
            .buffer_size(10)
            .tens(2)
            .hundreds(1)
            .db_config(Config::new())
            .queries(QueryHolder::default())
            .max_records_per_cycle_batch(10)
            .introduced_lag_cycles(2)
            .introduced_lag_in_millies(10)
            .connection_creation_threshold(15.0)
            .name("test".to_string())
            .print_connection_configuration();

        let _ = builder.build_update();
    }

    #[test]
    #[should_panic(expected = "tens is None")]
    fn test_missing_tens() {
        let cancellation_token = CancellationToken::new();
        let mut builder = QuickStreamBuilder::default();
        builder
            .cancellation_tocken(cancellation_token)
            .max_connection_count(2)
            .buffer_size(10)
            .single_digits(1)
            .hundreds(1)
            .db_config(Config::new())
            .queries(QueryHolder::default())
            .max_records_per_cycle_batch(10)
            .introduced_lag_cycles(2)
            .introduced_lag_in_millies(10)
            .connection_creation_threshold(15.0)
            .name("test".to_string())
            .print_connection_configuration();

        let _ = builder.build_update();
    }

    #[test]
    #[should_panic(expected = "hundreds is None")]
    fn test_missing_hundreds() {
        let cancellation_token = CancellationToken::new();
        let mut builder = QuickStreamBuilder::default();
        builder
            .cancellation_tocken(cancellation_token)
            .max_connection_count(2)
            .buffer_size(10)
            .single_digits(1)
            .tens(2)
            .db_config(Config::new())
            .queries(QueryHolder::default())
            .max_records_per_cycle_batch(10)
            .introduced_lag_cycles(2)
            .introduced_lag_in_millies(10)
            .connection_creation_threshold(15.0)
            .name("test".to_string())
            .print_connection_configuration();

        let _ = builder.build_update();
    }

    #[test]
    #[should_panic(expected = "db_config is None")]
    fn test_missing_db_config() {
        let cancellation_token = CancellationToken::new();
        let mut builder = QuickStreamBuilder::default();
        builder
            .cancellation_tocken(cancellation_token)
            .max_connection_count(2)
            .buffer_size(10)
            .single_digits(1)
            .tens(2)
            .hundreds(1)
            .queries(QueryHolder::default())
            .max_records_per_cycle_batch(10)
            .introduced_lag_cycles(2)
            .introduced_lag_in_millies(10)
            .connection_creation_threshold(15.0)
            .name("test".to_string())
            .print_connection_configuration();

        let _ = builder.build_update();
    }

    #[test]
    #[should_panic(expected = "queries is None")]
    fn test_missing_queries() {
        let cancellation_token = CancellationToken::new();
        let mut builder = QuickStreamBuilder::default();
        builder
            .cancellation_tocken(cancellation_token)
            .max_connection_count(2)
            .buffer_size(10)
            .single_digits(1)
            .tens(2)
            .hundreds(1)
            .db_config(Config::new())
            .max_records_per_cycle_batch(10)
            .introduced_lag_cycles(2)
            .introduced_lag_in_millies(10)
            .connection_creation_threshold(15.0)
            .name("test".to_string())
            .print_connection_configuration();

        let _ = builder.build_update();
    }

    #[test]
    #[should_panic(expected = "max_records_per_cycle_batch is None")]
    fn test_missing_max_records_per_cycle_batch() {
        let cancellation_token = CancellationToken::new();
        let mut builder = QuickStreamBuilder::default();
        builder
            .cancellation_tocken(cancellation_token)
            .max_connection_count(2)
            .buffer_size(10)
            .single_digits(1)
            .tens(2)
            .hundreds(1)
            .db_config(Config::new())
            .queries(QueryHolder::default())
            .introduced_lag_cycles(2)
            .introduced_lag_in_millies(10)
            .connection_creation_threshold(15.0)
            .name("test".to_string())
            .print_connection_configuration();

        let _ = builder.build_update();
    }

    #[test]
    #[should_panic(expected = "introduced_lag_cycles is None")]
    fn test_missing_introduced_lag_cycles() {
        let cancellation_token = CancellationToken::new();
        let mut builder = QuickStreamBuilder::default();
        builder
            .cancellation_tocken(cancellation_token)
            .max_connection_count(2)
            .buffer_size(10)
            .single_digits(1)
            .tens(2)
            .hundreds(1)
            .db_config(Config::new())
            .queries(QueryHolder::default())
            .max_records_per_cycle_batch(10)
            .introduced_lag_in_millies(10)
            .connection_creation_threshold(15.0)
            .name("test".to_string())
            .print_connection_configuration();

        let _ = builder.build_update();
    }

    #[test]
    #[should_panic(expected = "introduced_lag_in_millies is None")]
    fn test_missing_introduced_lag_in_millies() {
        let cancellation_token = CancellationToken::new();
        let mut builder = QuickStreamBuilder::default();
        builder
            .cancellation_tocken(cancellation_token)
            .max_connection_count(2)
            .buffer_size(10)
            .single_digits(1)
            .tens(2)
            .hundreds(1)
            .db_config(Config::new())
            .queries(QueryHolder::default())
            .max_records_per_cycle_batch(10)
            .introduced_lag_cycles(2)
            .connection_creation_threshold(15.0)
            .name("test".to_string())
            .print_connection_configuration();

        let _ = builder.build_update();
    }

    #[test]
    #[should_panic(expected = "connection_creation_threshold is None")]
    fn test_missing_connection_creation_threshold() {
        let cancellation_token = CancellationToken::new();
        let mut builder = QuickStreamBuilder::default();
        builder
            .cancellation_tocken(cancellation_token)
            .max_connection_count(2)
            .buffer_size(10)
            .single_digits(1)
            .tens(2)
            .hundreds(1)
            .db_config(Config::new())
            .queries(QueryHolder::default())
            .max_records_per_cycle_batch(10)
            .introduced_lag_cycles(2)
            .introduced_lag_in_millies(10)
            .name("test".to_string())
            .print_connection_configuration();

        let _ = builder.build_update();
    }
}