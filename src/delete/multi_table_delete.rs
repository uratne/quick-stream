use std::sync::Arc;

use async_trait::async_trait;
use log::{error, info, trace, warn};
use native_tls::{Certificate, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use support::DataHolder;
use tokio::{sync::mpsc::{self, Receiver, Sender}, task::JoinHandle};
use tokio_postgres::{Client, Error, NoTls};
use tokio_util::sync::CancellationToken;

use crate::{builder::support::MultiTableDeleteQueryHolder, introduce_lag};

use super::Delete;

mod support;

#[async_trait]
pub trait MultiTableDelete<T>: Send + Sync + Delete<T>
where
    T: Clone + Send + Sync,
{
    fn table(&self) -> String;
    fn tables() -> Vec<String>;
}

#[derive(Debug)]
struct DeleteData<T> where T: MultiTableDelete<T> + Clone + Send {
    pub tx: Sender<Vec<T>>,
    pub join_handler: JoinHandle<u8>,
    pub id: i64
}

impl<T> DeleteData<T> where T: MultiTableDelete<T> + Clone + Send {
    pub fn new(tx: Sender<Vec<T>>, join_handler: JoinHandle<u8>, id: i64) -> Self {
        Self {
            tx,
            join_handler,
            id
        }
    }
}

#[derive(Default, Clone)]
pub struct MultiTableDeleteQuickStream {
    pub(crate) cancellation_token: CancellationToken,
    pub(crate) max_con_count: usize,
    pub(crate) buffer_size: usize,
    pub(crate) init_con_count: usize,
    pub(crate) db_config: tokio_postgres::Config,
    pub(crate) tls: Option<Certificate>,
    pub(crate) queries: MultiTableDeleteQueryHolder,
    pub(crate) max_records_per_cycle_batch: usize, //a batch = introduced_lag_cycles
    pub(crate) introduced_lag_cycles: usize,
    pub(crate) introduced_lag_in_millies: u64,
    pub(crate) connection_creation_threshold: f64,
    pub(crate) name: String,
    pub(crate) print_con_config: bool
}

impl MultiTableDeleteQuickStream {
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
    
    async fn process_n<T>(&self, multi_table_single_queries: MultiTableDeleteQueryHolder, mut rx: Receiver<Vec<T>>, thread_id: i64) -> Result<(), Error>  where T: MultiTableDelete<T> + Clone + Send + 'static {
        info!("{}:{}: starting data deletor", self.name, thread_id);

        info!("{}:{}: creating database client", self.name, thread_id);
        let client = self.get_db_client().await;
        info!("{}:{}: creating database client success", self.name, thread_id);

        info!("{}:{}: preparing queries and creating statement map", self.name, thread_id);
        let statement_map = multi_table_single_queries.prepare(&client).await;
        info!("{}:{}: queries prepared and created statement map successfully", self.name, thread_id);

        info!("{}:{}: data deletor channel receiver starting", self.name, thread_id);
        'inner: loop {
            tokio::select! {
                Some(data) = rx.recv() => {
                    //Make sure to send same type of data to a single sender so we can get the type
                    let table = data.first().expect("Unreachable logic reached. Check quick_stream::delete::process_n<T>,  function").table();
                    trace!("{}:{}: data received pushing for ingestion to table: {}. pkeys: {:?}", self.name, thread_id, table, data.iter().map(|f| f.pkey()).collect::<Vec<i64>>());
                    let count = T::delete(&client, data, &statement_map.get(&table).unwrap(), thread_id).await?;
                    trace!("{}:{}: data ingestion to table: {} successfull. count: {}", self.name, thread_id, table, count);
                }
                _ = self.cancellation_token.cancelled() => {
                    info!("{}:{}: cancellation token received. shutting down data delettor", self.name, thread_id);
                    break 'inner
                }
            }
        }

        info!("{}:{}: closing the channel", self.name, thread_id);
        drop(rx);
        

        info!("{}:{} shutting down data ingestor", self.name, thread_id);
        Ok(())
    }

    fn init_senders<T>(&self, count: usize, tx_count: &mut i64) -> Vec<DeleteData<T>> where T: MultiTableDelete<T> + Clone + Send + 'static {
        trace!("{}: initiating sender, creating {} upsert senders", self.name, count);
        let mut senders = vec![];
    
        for _ in 0..count {
            let (tx_t, rx_t) = mpsc::channel::<Vec<T>>(self.buffer_size);
    
            let thread_id = tx_count.clone();
            let queries_clone = self.queries.clone();
            let self_clone = self.to_owned();
            let handler = tokio::spawn(async move {
                let _ = self_clone.process_n(queries_clone, rx_t, thread_id).await;
                1u8
            });
    
            let tx_struct = DeleteData::new(tx_t, handler, tx_count.clone());
    
            *tx_count += 1;
    
            senders.push(tx_struct);
        }
    
        senders
    }

    async fn handle_n<T>(&self, data: Vec<T>, senders: &mut Vec<DeleteData<T>>, tx_count: &mut i64) where T: MultiTableDelete<T> + Clone + Send + 'static {
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
            warn!("{}: capacity of {} {}% is below connection creation threshold {}%", self.name, sender_0.id, capacity, self.connection_creation_threshold);

            if *tx_count < self.max_con_count as i64 {
                info!("{}: creating a sender since current connections {} is below allowed max connections count {}", self.name, *tx_count, self.max_con_count);
                let (tx_t, rx_t) = mpsc::channel::<Vec<T>>(self.buffer_size);

                let thread_id = tx_count.clone();
                let queries_clone = self.queries.clone();
                let self_clone = Arc::new(self.to_owned());
                let handler = tokio::spawn(async move {
                    let _ = self_clone.process_n(queries_clone, rx_t, thread_id).await;
                    0u8
                });

                match tx_t.send(data).await {
                    Ok(_) => {
                        let tx_struct = DeleteData::new(tx_t, handler, tx_count.clone());
                        info!("{}: creating sender {} successful", self.name, tx_struct.id);
                        *tx_count += 1;
                        senders.push(tx_struct);

                        if *tx_count == self.max_con_count as i64 {
                            warn!("{}: max connection count reached", self.name)
                        } else {
                            info!("{}: connection created, current total connections : {}", self.name, tx_count)
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
                        panic!("{}: failed to send data through the channel of sender {} : {}", self.name, sender_0.id, error)
                    },
                }
            }
        } else {
            info!("{}: capacity of sender {} is at {}%", self.name, sender_0.id, capacity);
            match sender_0.tx.send(data).await {
                Ok(_) => {
                    trace!("{}: pushing to data ingestor success using sender {}", self.name, sender_0.id);
                },
                Err(error) => {
                    panic!("{}: failed to send data through the channel of sender {} : {}", self.name, sender_0.id, error)
                },
            };
        }
    }

    fn rebalance_senders<T>(&self, senders:&mut Vec<DeleteData<T>>, tx_count: &mut i64) where T: MultiTableDelete<T> + Clone + Send + 'static {

        trace!("{}: rebalancing senders", self.name);

        let start_senders = senders.len();

        senders.retain(|delete_data| !delete_data.tx.is_closed() || delete_data.join_handler.is_finished());

        let removed_senders = start_senders - senders.len();

        if removed_senders > 0 {
            info!("{}: removed {} senders", self.name, removed_senders);
            *tx_count -= removed_senders as i64;
        }


        if senders.len() > self.init_con_count {
            let full_capacity_count = senders.iter().filter(|sender| sender.tx.capacity() == self.buffer_size).collect::<Vec<&DeleteData<T>>>().len();

            if full_capacity_count > 0 {
                let mut amount_to_pop = full_capacity_count - (full_capacity_count / 2usize);
                if senders.len() - amount_to_pop < self.init_con_count {
                    amount_to_pop = senders.len() - self.init_con_count;
                }
                senders.sort_by(|x, y| x.tx.capacity().cmp(&y.tx.capacity()));
                for _ in 0..amount_to_pop {
                    senders.pop();
                    *tx_count -= 1;
                }
            }
        }
    
        trace!("{}: rebalancing senders complete", self.name);
        if senders.len() != start_senders || self.print_con_config {
            self.print_sender_status(&senders, &tx_count)
        }
    }

    async fn push_to_handle<T>(&self, mut senders: &mut Vec<DeleteData<T>>, vec_data: Vec<T>, tx_count: &mut i64) where T: MultiTableDelete<T> + Clone + Send + 'static {
        self.handle_n(vec_data, &mut senders, tx_count).await;
    }

    fn print_sender_status<T>(&self, senders: &Vec<DeleteData<T>>, tx_count: & i64) where T: MultiTableDelete<T> + Clone + Send + 'static {
        let total_senders_percentage = (*tx_count * 100) as f64 / self.max_con_count as f64;
        info!("{}: Current Senders (Database Connections) configuration 
                SENDER          AMOUNT
            delete senders  :     {}
            ____________________________
            total senders   :     {}
            total senders % :     {}
            ============================
        ", self.name, senders.len(), *tx_count, total_senders_percentage);
    }

    async fn send_processed<T>(&self, data: Vec<T>, table: String, senders: &mut Vec<DeleteData<T>>, tx_count: &mut i64 ) where T: MultiTableDelete<T> + Clone + Send + 'static {
        trace!("{}: data count: {} exceeds max records per cycle batch: {}. proceeding for ingestion to table: {}", self.name, data.len(), self.max_records_per_cycle_batch, table);


        trace!("{}: data ingestion starting for batches of table: {}", self.name, table);
        self.push_to_handle(senders, data, tx_count).await;
        trace!("{}: data pushed for ingestion for table: {}", self.name, table);
    }

    async fn process_received<T>(&self, data: Vec<T>,mut senders: &mut Vec<DeleteData<T>>, mut tx_count: &mut i64, rx: &mut Receiver<Vec<T>>) where T: MultiTableDelete<T> + Clone + Send + 'static {

        let mut data_holder = DataHolder::<T>::default();
        trace!("{}: data received. Adding data to a data holder", self.name);
        let data_ready_to_process = data_holder.add_all(data, self.max_records_per_cycle_batch);
        if data_ready_to_process.len() > 0 {
            trace!("{}: ready to process data available. proceding for ingestion one table at a time", self.name);
            for (table, data) in data_ready_to_process {
                trace!("{}: data count: {} exceeds max records per cycle batch: {}. proceeding for ingestion to table: {}", self.name, data.len(), self.max_records_per_cycle_batch, table);
                self.send_processed(data, table, &mut senders, &mut tx_count).await;
            }
        } else if data_holder.len() > 0 {

            trace!("{}: starting lag cycles", self.name);
            let mut introduced_lag_cycles = 0;
            'inner: loop {
                match rx.try_recv() {
                    Ok(more_data) => {
                        trace!("{}: more data received. Adding data to a data holder", self.name);
                        let data_ready_to_process = data_holder.add_all(more_data, self.max_records_per_cycle_batch);

                        trace!("{}: ready to process data available. proceding for ingestion one table at a time", self.name);
                        for (table, data) in data_ready_to_process {
                            trace!("{}: data count: {} exceeds max records per cycle batch: {}. breaking the lag cycle and proceesing for ingestion", self.name, data.len(), self.max_records_per_cycle_batch);
                            self.send_processed(data, table, &mut senders, &mut tx_count).await;
                        }

                        if data_holder.len() == 0 {
                            trace!("{}: no more data to process. breaking the lag cycle", self.name);
                            break 'inner;
                        }
                    },
                    Err(_) => {
                        trace!("{}: no data received. data count: {}", self.name, data_holder.len());
                        introduced_lag_cycles += 1;

                        trace!("{}: lag cycles: {}", self.name, introduced_lag_cycles);
                        // greater than is used allowing 0 lag cycles
                        if introduced_lag_cycles > self.introduced_lag_cycles {
                            trace!("{}: lag cycles: {} exceeds or reached max introduced lag cycles. data count : {}. proceeding for ingestion.", self.name, self.introduced_lag_cycles, data_holder.len());
                            break 'inner;
                        } else {
                            trace!("{}: introducing lag", self.name);
                            introduce_lag(self.introduced_lag_in_millies).await;
                            trace!("{}: introduced lag successfull", self.name);
                        }
                    },
                }
            };

            trace!("{}: lag cycles complete. Getting all data from data holder to process", self.name);
            let all_data = data_holder.get_all();
        
            for (table, data) in all_data {
                trace!("{}: data count: {} exceeds max records per cycle batch: {}. proceeding for ingestion to table: {}", self.name, data.len(), self.max_records_per_cycle_batch, table);
                self.send_processed(data, table, &mut senders, &mut tx_count).await;
            }
            
        }

        self.rebalance_senders(&mut senders, &mut tx_count);
    }

    pub async fn run<T>(&self, mut rx: Receiver<Vec<T>>) where T: MultiTableDelete<T> + Clone + Send + 'static {

        info!("{}: upsert quick stream is starting", self.name);
        info!("{}: testing database connections", self.name);
        let _client = self.get_db_client().await;
        drop(_client);
        info!("{}: database sucsessfully connected", self.name);
        let mut tx_count = 0;

        trace!("{}: initiating senders", self.name);
        let mut senders = self.init_senders::<T>( self.init_con_count, &mut tx_count);
        trace!("{}: inititating senders complete", self.name);

        #[cfg(all(unix, feature = "unix-signals"))]
        let cancellation_token = self.cancellation_token.clone();

        #[cfg(all(unix, feature = "unix-signals"))]
        let unix_shutdown_service = tokio::spawn(async move {
            shutdown_service::shutdown_unix(cancellation_token).await;
            3u8
        });

        #[cfg(all(windows, feature = "windows-signals"))]
        let cancellation_token = self.cancellation_token.clone();

        #[cfg(all(windows, feature = "windows-signals"))]
        match ctrlc::try_set_handler(move || {
            cancellation_token.cancel();
        }) {
            Ok(_) => trace!("{}: ctrlc handler set", self.name),
            Err(_) => error!("{}: ctrlc handler failed to set", self.name)
        };
        
        info!("{}: main channel receiver starting", self.name);
        'outer: loop {
            tokio::select! {
                Some(data) = rx.recv() => {
                    self.process_received(data, &mut senders, &mut tx_count, &mut rx).await;
                }
                _ = self.cancellation_token.cancelled() => {
                    info!("{}: cancellation token received. shutting down upsert quick stream", self.name);
                    break 'outer;
                }
            }
        }

        info!("{}: shutting down senders", self.name);
        for sender in senders {
            match sender.join_handler.await {
                Ok(_) => trace!("{}: sender {} shutdown", self.name, sender.id),
                Err(error) => error!("{}: sender {} shutdown failed with error: {}", self.name, sender.id, error),
            };
        }
        info!("{}: senders shutdown complete", self.name);

        #[cfg(all(unix, feature = "unix-signals"))]
        match unix_shutdown_service.await {
            Ok(_) => info!("{}: upsert quick stream shutdown service complete", self.name),
            Err(error) => error!("{}: upsert quick stream shutdown service failed with error: {}", self.name, error)
        }

        info!("{}: upsert quick stream shutdown complete", self.name);
    }


}