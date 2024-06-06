use std::{collections::{HashMap, HashSet}, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::NaiveDateTime;
use futures::future::BoxFuture;
use native_tls::{Certificate, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use tokio::{sync::mpsc::{self, Receiver, Sender}, task::JoinHandle};
use tokio_postgres::{Client, Error, NoTls, Statement};
use tokio_util::sync::CancellationToken;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

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
}

impl<T> UpsertData<T> where T: Upsert<T> + Clone + Send {
    pub fn new(tx: Sender<Vec<T>>, join_handler: JoinHandle<u8>, id: i64) -> Self {
        Self {
            tx,
            join_handler,
            id,
        }
    }
}



#[derive(Default, Clone)]
#[allow(dead_code)] //for cancellation token, TODO remove when used
pub struct UpsertQuickStream {
    cancellation_tocken: CancellationToken,
    max_con_count: usize,
    buffer_size: usize,
    single_digits: usize,
    tens: usize,
    hundreds: usize,
    db_config: tokio_postgres::Config,
    tls: Option<Certificate>,
    queries: HashMap<usize, String>,
    max_records_per_cycle_batch: usize, //a batch = introduced_lag_cycles
    introduced_lag_cycles: usize,
    introduced_lag_in_millies: u64,
    connection_creation_threshold: f64,
    built: bool
}

fn remove_duplicates<T>(data: &mut Vec<T>) where T: Upsert<T> + Clone + Send + 'static {
    let mut hash_set = HashSet::new();
    data.sort_by(|x, y| y.modified_date().cmp(&x.modified_date()));
    data.retain(|ftp_current| hash_set.insert(ftp_current.pkey().clone()))
}

fn split_vec_by_given<T>(mut data: Vec<T>, hundreds: usize, tens: usize, single_digit: usize) -> Vec<Vec<T>> where T: Upsert<T> + Clone + Send +'static {
    let mut results = vec![];

    for _hundred in 0..hundreds {
        let (ftp_current_1, ftp_current_2) = data.split_at(100);
        results.push(ftp_current_1.to_vec());
        data = ftp_current_2.to_vec();
    }

    for _ten in 0..tens {
        let (ftp_current_1, ftp_current_2) = data.split_at(10);
        results.push(ftp_current_1.to_vec());
        data = ftp_current_2.to_vec();
    }

    if single_digit != data.len() {
        panic!("Unreachable logic reached")
    } else {
        if !data.is_empty() {
            results.push(data);
        }
        return results;
    }
}

fn split_vec<T>(ftp_currents: Vec<T>) -> Vec<Vec<T>> where T: Upsert<T> + Clone + Send + 'static {
    let hundreds = ftp_currents.len() / 100;
    let hundreds_remainder = ftp_currents.len() % 100;

    let tens = hundreds_remainder / 10;
    let tens_remainder = hundreds_remainder % 10;


    split_vec_by_given(ftp_currents, hundreds, tens, tens_remainder)
}

async fn introduce_lag(lag: u64) {
    println!("debug introduced lag : {}ms", lag);
    tokio::time::sleep(Duration::from_millis(lag)).await;
    println!("debug introduced lag over");
}

#[allow(dead_code)]
impl UpsertQuickStream {
    pub async fn run<T>(&self, mut rx: Receiver<Vec<T>>) where T: Upsert<T> + Clone + Send + 'static {

        if !self.built {
            panic!("upsert quick stream is not built");
        }

        let _client = self.get_db_client().await;
        let mut tx_count = 0;

        let mut senders_1 = self.init_senders_handlers::<T>(1, self.single_digits, &mut tx_count);
        let mut senders_2 = self.init_senders_handlers::<T>(2, self.single_digits, &mut tx_count);
        let mut senders_3 = self.init_senders_handlers::<T>(3, self.single_digits, &mut tx_count);
        let mut senders_4 = self.init_senders_handlers::<T>(4, self.single_digits, &mut tx_count);
        let mut senders_5 = self.init_senders_handlers::<T>(5, self.single_digits, &mut tx_count);
        let mut senders_6 = self.init_senders_handlers::<T>(6, self.single_digits, &mut tx_count);
        let mut senders_7 = self.init_senders_handlers::<T>(7, self.single_digits, &mut tx_count);
        let mut senders_8 = self.init_senders_handlers::<T>(8, self.single_digits, &mut tx_count);
        let mut senders_9 = self.init_senders_handlers::<T>(9, self.single_digits, &mut tx_count);

        let mut senders_10 = self.init_senders_handlers::<T>(10, self.tens, &mut tx_count);

        let mut senders_100 = self.init_senders_handlers::<T>(1, self.hundreds, &mut tx_count);

        
        if let Some(mut data) = rx.recv().await {
            if data.len() >= self.max_records_per_cycle_batch {
                remove_duplicates(&mut data);
                let vec_data = split_vec(data);

                for data in vec_data {
                    match data.len() {
                        0 => continue,
                        1 => self.handle_n(data, &mut senders_1, &mut tx_count).await,
                        2 => self.handle_n(data, &mut senders_2, &mut tx_count).await,
                        3 => self.handle_n(data, &mut senders_3, &mut tx_count).await,
                        4 => self.handle_n(data, &mut senders_4, &mut tx_count).await,
                        5 => self.handle_n(data, &mut senders_5, &mut tx_count).await,
                        6 => self.handle_n(data, &mut senders_6, &mut tx_count).await,
                        7 => self.handle_n(data, &mut senders_7, &mut tx_count).await,
                        8 => self.handle_n(data, &mut senders_8, &mut tx_count).await,
                        9 => self.handle_n(data, &mut senders_9, &mut tx_count).await,
                        10 => self.handle_n(data, &mut senders_10, &mut tx_count).await,
                        100 => self.handle_n(data, &mut senders_100, &mut tx_count).await,
                        _ => {
                            eprint!("error");
                            panic!("Unreachable logic reached")
                        }
                    }
                }
            } else {
                let mut introduced_lag_cycles = 0;
                'inner: loop {
                    match rx.try_recv() {
                        Ok(mut more_data) => {
                            data.append(&mut more_data);

                            if data.len() >= self.max_records_per_cycle_batch {
                                break 'inner;
                            }
                        },
                        Err(_) => {
                            introduced_lag_cycles += 1;

                            // greater than or equal is used allowing 0 lag cycles
                            if introduced_lag_cycles >= self.introduced_lag_cycles {
                                break 'inner;
                            } else {
                                introduce_lag(self.introduced_lag_in_millies).await
                            }
                        },
                    }
                };

                remove_duplicates(&mut data);
                let vec_data = split_vec(data);
                for data in vec_data {
                    match data.len() {
                        0 => continue,
                        1 => self.handle_n(data, &mut senders_1, &mut tx_count).await,
                        2 => self.handle_n(data, &mut senders_2, &mut tx_count).await,
                        3 => self.handle_n(data, &mut senders_3, &mut tx_count).await,
                        4 => self.handle_n(data, &mut senders_4, &mut tx_count).await,
                        5 => self.handle_n(data, &mut senders_5, &mut tx_count).await,
                        6 => self.handle_n(data, &mut senders_6, &mut tx_count).await,
                        7 => self.handle_n(data, &mut senders_7, &mut tx_count).await,
                        8 => self.handle_n(data, &mut senders_8, &mut tx_count).await,
                        9 => self.handle_n(data, &mut senders_9, &mut tx_count).await,
                        10 => self.handle_n(data, &mut senders_10, &mut tx_count).await,
                        100 => self.handle_n(data, &mut senders_100, &mut tx_count).await,
                        _ => {
                            eprint!("error");
                            panic!("Unreachable logic reached")
                        }
                    }
                }
            }

            println!("re-balancing connections");
            self.re_balance(&mut senders_1, self.single_digits, &mut tx_count);
        }
    }

    async fn get_db_client(&self) -> Client {
        let config = self.db_config.to_owned();

        match &self.tls {
            Some(tls) => {
                let connector = TlsConnector::builder()
                    .add_root_certificate(tls.clone())
                    .build()
                    .unwrap();

                let tls = MakeTlsConnector::new(connector);

                let (client, connection) = match config
                    .connect(tls)
                    .await {
                    Ok(cnc) => cnc,
                    Err(error) => panic!("error occured during database client establishment, error : {}", error)
                };
        
                tokio::spawn(async move {
                    if let Err(error) = connection.await {
                        eprintln!("connection failed with error : {}", error)
                    }
                });
        
                client                
            },
            None => {
                let (client, connection) = match config
                    .connect(NoTls)
                    .await {
                    Ok(cnc) => cnc,
                    Err(error) => panic!("error occured during database client establishment, error : {}", error)
                };
        
                tokio::spawn(async move {
                    if let Err(error) = connection.await {
                        eprintln!("connection failed with error : {}", error)
                    }
                });
        
                client
            },
        }
    }

    async fn process_n<T>(&self, query: String, mut rx: Receiver<Vec<T>>, thread_id: i64, n: usize) -> Result<(), Error>  where T: Upsert<T> + Clone + Send + 'static {
        let client = self.get_db_client().await;

        let statement = client.prepare(query.as_str()).await.unwrap();

        println!("thread n : {}", n);

        if let Some(data) = rx.recv().await {
            T::upsert(&client, data, &statement, thread_id).await?;
        }

        Ok(())
    }

    fn init_senders_handlers<T>(&self, n: usize, count: usize, tx_count: &mut i64) -> Vec<UpsertData<T>> where T: Upsert<T> + Clone + Send + 'static {
        let mut senders = vec![];
    
        for _ in 0..count {
            let (tx_t, rx_t) = mpsc::channel::<Vec<T>>(self.buffer_size);
    
            let thread_id = tx_count.clone();
            let query = self.queries.get(&n).unwrap().to_owned();
            let n_clone = n.clone();
            let self_clone = self.to_owned();
            let handler = tokio::spawn(async move {
                let _ = self_clone.process_n(query, rx_t, thread_id, n_clone).await;
                1u8
            });
    
            let tx_struct = UpsertData::new(tx_t, handler, tx_count.clone());
    
            *tx_count += 1;
    
            senders.push(tx_struct);
        }
    
        senders
    }

    async fn handle_n<T>(&self, data: Vec<T>, senders: &mut Vec<UpsertData<T>>, tx_count: &mut i64) where T: Upsert<T> + Clone + Send + 'static {
        senders.sort_by(|x, y| y.tx.capacity().cmp(&x.tx.capacity()));

        let sender_0 = match senders.first() {
            Some(sender) => sender,
            None => panic!("no senders found, impossible scenario"),
        };

        let capacity = sender_0.tx.capacity() as f64 / self.buffer_size as f64 * 100f64;

        if capacity <= self.connection_creation_threshold {
            eprintln!("warn the capacity : {}, thread id : {}", capacity, sender_0.id);
            println!("attempting to create a connection");

            if *tx_count < self.max_con_count as i64 {
                let (tx_t, rx_t) = mpsc::channel::<Vec<T>>(self.buffer_size);

                let thread_id = tx_count.clone();
                let n = data.len();
                let query = self.queries.get(&n).unwrap().to_owned();
                let self_clone = Arc::new(self.to_owned());
                let handler = tokio::spawn(async move {
                    let _ = self_clone.process_n(query, rx_t, thread_id, n).await;
                    0u8
                });

                match tx_t.send(data).await {
                    Ok(_) => {
                        let tx_struct = UpsertData::new(tx_t, handler, tx_count.clone());
                        *tx_count += 1;
                        senders.push(tx_struct);

                        if *tx_count == self.max_con_count as i64 {
                            eprintln!("warn max connection count reached")
                        } else {
                            println!("connection created, current total connections : {}", tx_count)
                        }
                    },
                    Err(error) => panic!("error : {}", error),
                };
            } else {
                eprintln!("unable to create a connection, max connection count reached, PROCESSOR AT CAPACITY, will have to wait till capacity is available !!!");
                match sender_0.tx.send(data).await {
                    Ok(_) => println!("data sent after capacity was available"),
                    Err(error) => panic!("error : {}", error),
                }
            }
        } else {
            println!("info the capacity : {}, thread id : {}", capacity, sender_0.id);

            match sender_0.tx.send(data).await {
                Ok(_) => {
                    println!("trace success")
                },
                Err(error) => {
                    eprintln!("error : {}", error);
                    panic!("error : {}", error)
                },
            };
        }
    }

    fn re_balance<T>(&self, senders: &mut Vec<UpsertData<T>>, init_limit: usize, tx_count: &mut i64) where T: Upsert<T> + Clone + Send + 'static {

        let start_senders = senders.len();
        senders.retain(|upsert_data| !upsert_data.tx.is_closed() || upsert_data.join_handler.is_finished());

        let removed_senders = start_senders - senders.len();

        if removed_senders > 0 {
            println!("removed {} senders", removed_senders);
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
    }
}





#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
