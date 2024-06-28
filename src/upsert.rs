use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use chrono::NaiveDateTime;
use futures::future::BoxFuture;
use log::{error, info, trace, warn};
use native_tls::{Certificate, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use tokio::{sync::mpsc::{self, Receiver, Sender}, task::JoinHandle};
use tokio_postgres::{Client, Error, NoTls, Statement};
use tokio_util::sync::CancellationToken;

use crate::{builder::support::QueryHolder, introduce_lag, remove_duplicates, split_vec};

#[async_trait]
pub trait Upsert<T>: Send + Sync
where
    T: Clone + Send + Sync,
{
    fn upsert(
        client: &Client,
        data: Vec<T>,
        statement: &Statement,
        thread_id: i64,
    ) -> BoxFuture<'static, Result<u64, Error>>;

    fn modified_date(&self) -> NaiveDateTime;
    fn pkey(&self) -> i64;
}

#[derive(Debug)]
struct UpsertData<T> where T: Upsert<T> + Clone + Send {
    pub tx: Sender<Vec<T>>,
    pub join_handler: JoinHandle<u8>,
    pub id: i64,
    pub type_: usize
}

impl<T> UpsertData<T> where T: Upsert<T> + Clone + Send {
    pub fn new(tx: Sender<Vec<T>>, join_handler: JoinHandle<u8>, id: i64, type_: usize) -> Self {
        Self {
            tx,
            join_handler,
            id,
            type_
        }
    }
}



#[derive(Default, Clone)]
#[allow(dead_code)] //for cancellation token, TODO remove when used
pub struct UpsertQuickStream {
    pub(crate) cancellation_token: CancellationToken,
    pub(crate) max_con_count: usize,
    pub(crate) buffer_size: usize,
    pub(crate) single_digits: usize,
    pub(crate) tens: usize,
    pub(crate) hundreds: usize,
    pub(crate) db_config: tokio_postgres::Config,
    pub(crate) tls: Option<Certificate>,
    pub(crate) queries: QueryHolder,
    pub(crate) max_records_per_cycle_batch: usize, //a batch = introduced_lag_cycles
    pub(crate) introduced_lag_cycles: usize,
    pub(crate) introduced_lag_in_millies: u64,
    pub(crate) connection_creation_threshold: f64,
    pub(crate) name: String,
    pub(crate) print_con_config: bool
}

#[allow(dead_code)]
impl UpsertQuickStream {
    pub async fn run<T>(&self, mut rx: Receiver<Vec<T>>) where T: Upsert<T> + Clone + Send + 'static {

        info!("{}: upsert quick stream is starting", self.name);
        info!("{}: testing database connections", self.name);
        let _client = self.get_db_client().await;
        drop(_client);
        info!("{}: database sucsessfully connected", self.name);
        let mut tx_count = 0;

        trace!("{}: initiating senders", self.name);
        let mut senders = self.init_senders::<T>(&mut tx_count);
        trace!("{}: inititating senders complete", self.name);
        
        info!("{}: main channel receiver starting", self.name);
        while let Some(mut data) = rx.recv().await {
            if data.len() >= self.max_records_per_cycle_batch {
                trace!("{}: data count: {} exceeds max records per cycle batch: {}. proceesing for ingestion", self.name, data.len(), self.max_records_per_cycle_batch);

                trace!("{}: removing duplicates", self.name);
                remove_duplicates(&mut data);
                trace!("{}: removing duplicates complete", self.name);

                trace!("{}: splitting vectors for batch ingestion", self.name);
                let vec_data = split_vec(data);
                trace!("{}: splitting vectors complete. batch count: {}", self.name, vec_data.len());

                trace!("{}: data ingestion starting for batches", self.name);
                self.push_to_handle(&mut senders, vec_data.to_owned(), &mut tx_count).await;
                trace!("{}: data pushed for ingestion", self.name);
            } else {
                trace!(target: format!("").as_str() ,"{}: data count: {} does not exceeds max records per cycle batch: {}", self.name, data.len(), self.max_records_per_cycle_batch);

                trace!("{}: starting lag cycles", self.name);
                let mut introduced_lag_cycles = 0;
                'inner: loop {
                    match rx.try_recv() {
                        Ok(mut more_data) => {
                            trace!("{}: more data received. amount : {}. appending to data", self.name, more_data.len());
                            data.append(&mut more_data);
                            trace!("{}: append success", self.name);

                            trace!("{}: removing duplicates", self.name);
                            remove_duplicates(&mut data);
                            trace!("{}: removing duplicates success", self.name);
                            if data.len() >= self.max_records_per_cycle_batch {
                                trace!("{}: data count: {} exceeds max records per cycle batch: {}. breaking the lag cycle and proceesing for ingestion", self.name, data.len(), self.max_records_per_cycle_batch);
                                break 'inner;
                            }
                        },
                        Err(_) => {
                            trace!("{}: no data received. data count: {}", self.name, data.len());
                            introduced_lag_cycles += 1;

                            trace!("{}: lag cycles: {}", self.name, introduced_lag_cycles);
                            // greater than or equal is used allowing 0 lag cycles
                            if introduced_lag_cycles >= self.introduced_lag_cycles {
                                trace!("{}: lag cycles: {} exceeds or reached max introduced lag cycles. data count : {}. proceeding for ingestion.", self.name, self.introduced_lag_cycles, data.len());
                                break 'inner;
                            } else {
                                trace!("{}: introducing lag", self.name);
                                introduce_lag(self.introduced_lag_in_millies).await;
                                trace!("{}: introduced lag successfull", self.name);
                            }
                        },
                    }
                };

                trace!("{}: splitting vectors for batch ingestion", self.name);
                let vec_data = split_vec(data);
                trace!("{}: splitting vectors complete. batch count: {}", self.name, vec_data.len());

                trace!("{}: data ingestion starting for batches", self.name);
                self.push_to_handle(&mut senders, vec_data, &mut tx_count).await;
                trace!("{}: data pushed for ingestion", self.name);
            }

            self.rebalance_senders(&mut senders, &mut tx_count);
        }
    }

    async fn get_db_client(&self) -> Client {
        trace!("{}: creating database client", self.name);
        let config = self.db_config.to_owned();

        match &self.tls {
            Some(tls) => {
                trace!("{}: tls is enabled", self.name);
                trace!("{}: creating tls connector", self.name);
                let connector = TlsConnector::builder()
                    .add_root_certificate(tls.clone())
                    .build()
                    .unwrap();

                let tls = MakeTlsConnector::new(connector);

                trace!("{}: creating tls connector success", self.name);

                trace!("{}: establishing database connection with tls", self.name);
                let (client, connection) = match config
                    .connect(tls)
                    .await {
                    Ok(cnc) => cnc,
                    Err(error) => panic!("error occured during database client establishment with tls, error : {}", error)
                };
                trace!("{}: establishing database connection with tls success", self.name);
        
                trace!("{}: creating thread to hold the database connection with tls", self.name);
                tokio::spawn(async move {
                    if let Err(error) = connection.await {
                        eprintln!("connection failed with error : {}", error)
                    }
                });
        
                trace!("{}: creating database client with tls success, returning client", self.name);
                client                
            },
            None => {
                trace!("{}: tls is dissabled", self.name);

                trace!("{}: establishing database connection", self.name);
                let (client, connection) = match config
                    .connect(NoTls)
                    .await {
                    Ok(cnc) => cnc,
                    Err(error) => panic!("error occured during database client establishment, error : {}", error)
                };
                trace!("{}: establishing database connection success", self.name);
        
                trace!("{}: creating thread to hold the database connection", self.name);
                tokio::spawn(async move {
                    if let Err(error) = connection.await {
                        eprintln!("connection failed with error : {}", error)
                    }
                });
                trace!("{}: creating thread to hold the database connection success", self.name);
        
                trace!("{}: creating database client success, returning client", self.name);
                client
            },
        }
    }

    async fn process_n<T>(&self, query: String, mut rx: Receiver<Vec<T>>, thread_id: i64, n: usize) -> Result<(), Error>  where T: Upsert<T> + Clone + Send + 'static {
        info!("{}:{}:{}: starting data ingestor", self.name, n, thread_id);

        info!("{}:{}:{}: creating database client", self.name, n, thread_id);
        let client = self.get_db_client().await;
        info!("{}:{}:{}: creating database client success", self.name, n, thread_id);

        info!("{}:{}:{}: preparing query and creating statement", self.name, n, thread_id);
        let statement = client.prepare(query.as_str()).await.unwrap();
        info!("{}:{}:{}: query prepared and created statement successfully", self.name, n, thread_id);

        info!("{}:{}:{}: data ingestor channel receiver starting", self.name, n, thread_id);
        while let Some(data) = rx.recv().await {
            trace!("{}:{}:{}: data received pushing for ingestion. pkeys: {:?}", self.name, n, thread_id, data.iter().map(|f| f.pkey()).collect::<Vec<i64>>());
            T::upsert(&client, data, &statement, thread_id).await?;
        }

        info!("{}:{}:{} shutting down data ingestor", self.name, n, thread_id);
        Ok(())
    }

    /**
     * n is redunt here as n is the same as type_ ***need to remove n***
     */
    fn init_sender<T>(&self, n: usize, count: usize, tx_count: &mut i64, type_: usize) -> Vec<UpsertData<T>> where T: Upsert<T> + Clone + Send + 'static {
        trace!("{}: initiating sender, creating {} upsert senders", self.name, count);
        let mut senders = vec![];
    
        for _ in 0..count {
            let (tx_t, rx_t) = mpsc::channel::<Vec<T>>(self.buffer_size);
    
            let thread_id = tx_count.clone();
            let query = self.queries.get(&n);
            let n_clone = n.clone();
            let self_clone = self.to_owned();
            let handler = tokio::spawn(async move {
                let _ = self_clone.process_n(query, rx_t, thread_id, n_clone).await;
                1u8
            });
    
            let tx_struct = UpsertData::new(tx_t, handler, tx_count.clone(), type_);
    
            *tx_count += 1;
    
            senders.push(tx_struct);
        }
    
        senders
    }

    fn init_senders<T>(&self, tx_count: &mut i64) -> HashMap<usize, Vec<UpsertData<T>>> where T: Upsert<T> + Clone + Send + 'static {
        trace!("{}: creating sender map of capacity 11", self.name);
        let mut sender_map = HashMap::with_capacity(11);
        
        trace!("{}: creating data senders from 1-10 and 100", self.name);
        let senders_1 = self.init_sender::<T>(1, self.single_digits, tx_count, 1);
        let senders_2 = self.init_sender::<T>(2, self.single_digits, tx_count, 2);
        let senders_3 = self.init_sender::<T>(3, self.single_digits, tx_count, 3);
        let senders_4 = self.init_sender::<T>(4, self.single_digits, tx_count, 4);
        let senders_5 = self.init_sender::<T>(5, self.single_digits, tx_count, 5);
        let senders_6 = self.init_sender::<T>(6, self.single_digits, tx_count, 6);
        let senders_7 = self.init_sender::<T>(7, self.single_digits, tx_count, 7);
        let senders_8 = self.init_sender::<T>(8, self.single_digits, tx_count, 8);
        let senders_9 = self.init_sender::<T>(9, self.single_digits, tx_count, 9);
        let senders_10 = self.init_sender::<T>(10, self.tens, tx_count, 10);
        trace!("{}: creating data senders from 1-10 success", self.name);

        let senders_100 = self.init_sender::<T>(1, self.hundreds, tx_count, 100);
        trace!("{}: creating data senders for 100 success", self.name);

        sender_map.insert(1, senders_1);
        sender_map.insert(2, senders_2);
        sender_map.insert(3, senders_3);
        sender_map.insert(4, senders_4);
        sender_map.insert(5, senders_5);
        sender_map.insert(6, senders_6);
        sender_map.insert(7, senders_7);
        sender_map.insert(8, senders_8);
        sender_map.insert(9, senders_9);
        sender_map.insert(10, senders_10);

        sender_map.insert(100, senders_100);

        self.print_sender_status(&sender_map, &tx_count);

        sender_map
    }

    async fn push_to_handle<T>(&self, senders: &mut HashMap<usize, Vec<UpsertData<T>>>, vec_data: Vec<Vec<T>>, tx_count: &mut i64) where T: Upsert<T> + Clone + Send + 'static {
        for data in vec_data {
            let k = data.len();
            self.handle_n(data,
                 senders.get_mut(&k)
                    .expect("Unreachable logic reached. Check quick_stream::split_vec<T>(data: Vec<T>) function"), 
                 tx_count, k).await;
        }
    }

    async fn handle_n<T>(&self, data: Vec<T>, senders: &mut Vec<UpsertData<T>>, tx_count: &mut i64, type_: usize) where T: Upsert<T> + Clone + Send + 'static {
        trace!("{}: handeling data started", self.name);
        trace!("{}: sorting senders by capacity to get the channel with highest capacity", self.name);
        senders.sort_by(|x, y| y.tx.capacity().cmp(&x.tx.capacity()));

        let sender_0 = match senders.first() {
            Some(sender) => sender,
            None => {
                error!("{}: no senders found, this is an impossible scenario", self.name);
                panic!("no senders found, impossible scenario")
            },
        };

        let capacity = sender_0.tx.capacity() as f64 / self.buffer_size as f64 * 100f64;

        if capacity <= self.connection_creation_threshold {
            warn!("{}: capacity of {}:{} {}% is below connection creation threshold {}%", self.name, sender_0.type_, sender_0.id, capacity, self.connection_creation_threshold);

            if *tx_count < self.max_con_count as i64 {
                info!("{}: creating a sender of type {} since current connections {} is below allowed max connections count {}", self.name, type_, *tx_count, self.max_con_count);
                let (tx_t, rx_t) = mpsc::channel::<Vec<T>>(self.buffer_size);

                let thread_id = tx_count.clone();
                let n = data.len();
                let query = self.queries.get(&n);
                let self_clone = Arc::new(self.to_owned());
                let handler = tokio::spawn(async move {
                    let _ = self_clone.process_n(query, rx_t, thread_id, n).await;
                    0u8
                });

                match tx_t.send(data).await {
                    Ok(_) => {
                        let tx_struct = UpsertData::new(tx_t, handler, tx_count.clone(), type_);
                        info!("{}: creating sender {}:{} successful", self.name, tx_struct.type_, tx_struct.id);
                        *tx_count += 1;
                        senders.push(tx_struct);

                        if *tx_count == self.max_con_count as i64 {
                            eprintln!("warn max connection count reached")
                        } else {
                            println!("connection created, current total connections : {}", tx_count)
                        }
                    },
                    Err(error) => {
                        error!("{}: creating sender failed with error: {}", self.name, error);
                        panic!("{}: failed to send data through the newly created channel {}", self.name, error)
                    },
                };
            } else {
                error!("{}: unable to create connection as max connection count has already reached", self.name);
                warn!("{}: PROCESSOR WILL HAVE TO WAIT UNTIL CAPACITY IS AVAIALABLE TO PROCEED", self.name);
                match sender_0.tx.send(data).await {
                    Ok(_) => info!("{}: data successfully pushed after capacity was available", self.name),
                    Err(error) => {
                        panic!("{}: failed to send data through the channel of sender {}:{} : {}", self.name, sender_0.type_, sender_0.id, error)
                    },
                }
            }
        } else {
            info!("{}: capacity of sender {}:{} is at {}%", self.name, sender_0.type_, sender_0.id, capacity);
            match sender_0.tx.send(data).await {
                Ok(_) => {
                    trace!("{}: pushing to data ingestor success using sender {}:{}", self.name, sender_0.type_, sender_0.id);
                },
                Err(error) => {
                    panic!("{}: failed to send data through the channel of sender {}:{} : {}", self.name, sender_0.type_, sender_0.id, error)
                },
            };
        }
    }

    fn re_balance_sender<T>(&self, senders: &mut Vec<UpsertData<T>>, init_limit: usize, tx_count: &mut i64, type_: usize) -> bool where T: Upsert<T> + Clone + Send + 'static {

        trace!("{}: rebalancing senders of type {}", self.name, type_);

        let start_senders = senders.len();
        senders.retain(|upsert_data| !upsert_data.tx.is_closed() || upsert_data.join_handler.is_finished());

        let removed_senders = start_senders - senders.len();

        if removed_senders > 0 {
            info!("{}: removed {} senders of type {}", self.name, removed_senders, type_);
            *tx_count -= removed_senders as i64;
        }

        if senders.len() > init_limit {
            let full_capacity_count = senders.iter().filter(|sender| sender.tx.capacity() == self.buffer_size).collect::<Vec<&UpsertData<T>>>().len();
    
            if full_capacity_count > 0 {
                let mut amount_to_pop = full_capacity_count - (full_capacity_count / 2usize);
                if senders.len() - amount_to_pop < init_limit {
                    amount_to_pop = senders.len() - init_limit;
                }
                senders.sort_by(|x, y| x.tx.capacity().cmp(&y.tx.capacity()));
                for _ in 0..amount_to_pop {
                    senders.pop();
                    *tx_count -= 1;
                }
            }
        }

        trace!("{}: rebalancing senders of type {} complete", self.name, type_);
        senders.len() != start_senders
    }

    fn rebalance_senders<T>(&self, senders: &mut HashMap<usize, Vec<UpsertData<T>>>, tx_count: &mut i64) where T: Upsert<T> + Clone + Send + 'static {
        trace!("{}: rebalancing database connections", self.name);
        let mut rebalanced = false;
        senders.iter_mut().for_each(|(sender_type, sender)| {
            if *sender_type < 10 {
                if self.re_balance_sender(sender, self.single_digits, tx_count, *sender_type) {
                    rebalanced = true
                }
            } else if *sender_type == 10 {
                if self.re_balance_sender(sender, self.tens, tx_count, *sender_type) {
                    rebalanced = true
                }
            } else if *sender_type == 100 {
                if self.re_balance_sender(sender, self.hundreds, tx_count, *sender_type) {
                    rebalanced = true
                }
            } else {
                error!("{}: Impossible Scenario, Check quick_stream::upsert::init_senders<T>(&self, tx_count: &mut i64) function", self.name);
                panic!("Unreachable logic reached. Check quick_stream::upsert::init_senders<T>(&self, tx_count: &mut i64) function")
            }
        });

        if rebalanced || self.print_con_config {
            self.print_sender_status(&senders, &tx_count)
        }
    }

    fn print_sender_status<T>(&self, senders: &HashMap<usize, Vec<UpsertData<T>>>, tx_count: &i64) where T: Upsert<T> + Clone + Send + 'static {
        let total_senders_percentage = (*tx_count * 100) as f64 / self.max_con_count as f64;
        info!(" {}: Current Senders (Database Connections) configuration
                SENDER          AMOUNT
            senders     1   :     {}
            senders     2   :     {}
            senders     3   :     {}
            senders     4   :     {}
            senders     5   :     {}
            senders     6   :     {}
            senders     7   :     {}
            senders     8   :     {}
            senders     9   :     {}
            senders    10   :     {}
            senders   100   :     {}
            ____________________________
            total senders   :     {}
            total senders % :     {}
            ============================
        ", 
        self.name, 
        senders.get(&1).unwrap().len(), 
        senders.get(&2).unwrap().len(), 
        senders.get(&3).unwrap().len(), 
        senders.get(&4).unwrap().len(), 
        senders.get(&5).unwrap().len(), 
        senders.get(&6).unwrap().len(), 
        senders.get(&7).unwrap().len(), 
        senders.get(&8).unwrap().len(), 
        senders.get(&9).unwrap().len(), 
        senders.get(&10).unwrap().len(), 
        senders.get(&100).unwrap().len(),
        *tx_count,
        total_senders_percentage)
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use chrono::{DateTime, NaiveDateTime, Utc};
    use futures::future::BoxFuture;
    use tokio_postgres::{Client, Error, Statement};

    use crate::{builder, introduce_lag, remove_duplicates, split_vec, split_vec_by_given};

    use super::Upsert;

    #[derive(Clone, PartialEq, Eq, Debug)]
    struct MockData {
        id: i64,
        modified_date: NaiveDateTime,
    }

    #[async_trait]
    impl Upsert<MockData> for MockData {
        fn upsert(
            _client: &Client,
            data: Vec<MockData>,
            _statement: &Statement,
            _thread_id: i64,
        ) -> BoxFuture<'static, Result<u64, Error>> {
            println!("data received, amount : {}", data.len());
            Box::pin(async { Ok(1) })
        }
        
        fn pkey(&self) -> i64 {
            self.id
        }

        fn modified_date(&self) -> NaiveDateTime {
            self.modified_date
        }
    }

    #[tokio::test]
    async fn test_remove_duplicates() {
        let mut data = vec![
            MockData { id: 1, modified_date: DateTime::from_timestamp(1627847280, 0).unwrap().naive_utc() },
            MockData { id: 1, modified_date: DateTime::from_timestamp(1627847281, 0).unwrap().naive_utc() },
            MockData { id: 2, modified_date: DateTime::from_timestamp(1627847282, 0).unwrap().naive_utc() },
        ];

        remove_duplicates(&mut data);
        assert_eq!(data.len(), 2);
        assert_eq!(data[0].id, 2);
        assert_eq!(data[1].id, 1);
    }

    #[test]
    fn test_split_vec() {
        let data = (0..110).map(|i| MockData { id: i, modified_date: DateTime::from_timestamp(1627847280, 0).unwrap().naive_utc() }).collect::<Vec<_>>();

        let result = split_vec(data);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].len(), 100);
        assert_eq!(result[1].len(), 10);
    }

    #[tokio::test]
    async fn test_introduce_lag() {
        let start = std::time::Instant::now();
        introduce_lag(100).await;
        let duration = start.elapsed();
        assert!(duration.as_millis() >= 100);
    }

    #[tokio::test]
    async fn test_init_sender() {
        let builder = builder::tests::test_builder();
        let processor = builder.build_update();

        let n = 7;
        let count = 10;
        let mut tx_count = 90;
        let original_tx_count = tx_count.to_owned();
        let type_ = 7;
        let sender = processor.init_sender::<MockData>(n, count, &mut tx_count, type_);

        assert_eq!(tx_count, 100);
        assert_eq!(sender.len(), 10);
        assert_eq!(sender.get(0).unwrap().type_, type_);
        assert_eq!(sender.get(2).unwrap().id, original_tx_count + 2);
    }

    #[tokio::test]
    async fn test_init_senders() {
        let builder = builder::tests::test_builder();
        let processor = builder.build_update();

        let mut tx_count = 0;
        let senders = processor.init_senders::<MockData>(&mut tx_count);

        assert_eq!(senders.len(), 11);
        assert_eq!(senders.get(&10).unwrap().len(), 12);
        assert_eq!(senders.get(&100).unwrap().len(), 1);

        assert_eq!(senders.get(&3).unwrap().len(), 2);
        assert_eq!(senders.get(&5).unwrap().get(0).unwrap().type_, 5);
        assert_eq!(senders.get(&6).unwrap().get(0).unwrap().id, 10);
    
        assert_eq!(tx_count, 31); // 2*9 (single digits) + 12 (tens) + 1 (hundreds) = 31
    }

    #[test]
    fn test_split_vec_by_given() {
        let data: Vec<MockData> = (0..250).map(|i| MockData { id: i, modified_date: Utc::now().naive_utc() }).collect();
        let result = split_vec_by_given(data.clone(), 2, 5, 0);

        assert_eq!(result.len(), 7);
        assert_eq!(result[0].len(), 100);
        assert_eq!(result[1].len(), 100);
        assert_eq!(result[2].len(), 10);
        assert_eq!(result[3].len(), 10);
        assert_eq!(result[4].len(), 10);
        assert_eq!(result[5].len(), 10);
        assert_eq!(result[6].len(), 10);

        // Ensure the elements are as expected
        assert_eq!(result[0][0].id, 0);
        assert_eq!(result[0][99].id, 99);
        assert_eq!(result[1][0].id, 100);
        assert_eq!(result[1][99].id, 199);
        assert_eq!(result[6][0].id, 240);
        assert_eq!(result[6][9].id, 249);
    }

    #[test]
    fn test_split_vec_2() {
        let data: Vec<MockData> = (0..250).map(|i| MockData { id: i, modified_date: Utc::now().naive_utc() }).collect();
        let result = split_vec(data.clone());

        assert_eq!(result.len(), 7);
        assert_eq!(result[0].len(), 100);
        assert_eq!(result[1].len(), 100);
        assert_eq!(result[2].len(), 10);
        assert_eq!(result[3].len(), 10);
        assert_eq!(result[4].len(), 10);
        assert_eq!(result[5].len(), 10);
        assert_eq!(result[6].len(), 10);

        // Ensure the elements are as expected
        assert_eq!(result[0][0].id, 0);
        assert_eq!(result[0][99].id, 99);
        assert_eq!(result[1][0].id, 100);
        assert_eq!(result[1][99].id, 199);
        assert_eq!(result[6][0].id, 240);
        assert_eq!(result[6][9].id, 249);
    }

    #[test]
    fn test_split_vec_edge_cases() {
        let empty_data: Vec<MockData> = vec![];
        let result = split_vec(empty_data);
        assert!(result.is_empty());

        let single_data: Vec<MockData> = vec![MockData { id: 1, modified_date: Utc::now().naive_utc() }];
        let result = split_vec(single_data.clone());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], single_data);
    }
}