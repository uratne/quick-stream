use std::{collections::HashSet, time::Duration};

use delete::Delete;
use log::debug;
use upsert::Upsert;

pub mod builder;
pub mod upsert;
pub mod delete;

fn remove_upsert_duplicates<T>(data: &mut Vec<T>) where T: Upsert<T> + Clone + Send + 'static {
    let mut hash_set = HashSet::new();
    data.sort_by(|x, y| y.modified_date().cmp(&x.modified_date()));
    data.retain(|data| hash_set.insert(data.pkey().clone()))
}

fn remove_delete_duplicates<T>(data: &mut Vec<T>) where T: Delete<T> + Clone + Send + 'static {
    let mut hash_set = HashSet::new();
    data.sort_by(|x, y| y.modified_date().cmp(&x.modified_date()));
    data.retain(|data| hash_set.insert(data.pkey().clone()))
}

fn split_vec_by_given<T>(mut data: Vec<T>, hundreds: usize, tens: usize, single_digit: usize) -> Vec<Vec<T>> where T: Upsert<T> + Clone + Send +'static {
    let mut results = vec![];

    for _hundred in 0..hundreds {
        let (data_1, data_2) = data.split_at(100);
        results.push(data_1.to_vec());
        data = data_2.to_vec();
    }

    for _ten in 0..tens {
        let (data_1, data_2) = data.split_at(10);
        results.push(data_1.to_vec());
        data = data_2.to_vec();
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

fn split_vec<T>(data: Vec<T>) -> Vec<Vec<T>> where T: Upsert<T> + Clone + Send + 'static {
    let hundreds = data.len() / 100;
    let hundreds_remainder = data.len() % 100;

    let tens = hundreds_remainder / 10;
    let tens_remainder = hundreds_remainder % 10;


    split_vec_by_given(data, hundreds, tens, tens_remainder)
}

async fn introduce_lag(lag: u64) {
    debug!("introducing lag: {}ms", lag);
    tokio::time::sleep(Duration::from_millis(lag)).await;
    debug!("introduced lag complete");
}



#[cfg(test)]
mod tests {
    use crate::builder::{support::QueryHolderBuilder, QuickStreamBuilder};
    use crate::Upsert;
    use std::time::Duration;
    use chrono::DateTime;
    use tokio::sync::mpsc;
    use chrono::NaiveDateTime;
    use async_trait::async_trait;
    use tokio::time;
    use tokio_postgres::{Client, Statement, Error};
    use futures::future::BoxFuture;
    use tokio_util::sync::CancellationToken;

    #[derive(Clone, Debug)]
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

        fn modified_date(&self) -> NaiveDateTime {
            self.modified_date
        }

        fn pkey(&self) -> i64 {
            self.id
        }
    }

    #[ignore = "only works with a database connection"]
    #[tokio::test]
    async fn test_upsert_quick_stream() {
        let query = "INSERT INTO trax_production.ftp_current (ftpc_tripplannumber, ftpc_tripplanversion, ftpc_scheduleeventseq, ftpc_scheduleeventcode, ftpc_scheduleeventtype, ftpc_scheduleeventcity, ftpc_scheduleeventstate, ftpc_schedulerailcarrier, ftpc_scheduletrainid, ftpc_scheduledatetime, ftpc_scheduletimemillis, ftpc_estimatedetadatetime, ftpc_estimatedetatimemillis, ftpc_eventtimezone, ftpc_actualeventdatetime, ftpc_actualtimemillis, ftpc_scheduledaynumber, ftpc_schedulecutofftime, ftpc_schedulecutoffday, ftpc_operationmon, ftpc_operationtue, ftpc_operationwed, ftpc_operationthu, ftpc_operationfri, ftpc_operationsat, ftpc_operationsun, ftpc_comments, ftpc_actualeventcode, ftpc_actualtrainid, ftpc_optn_prfmnce_ind, ftpc_optn_prfmnce_minutes, ftpc_ovrl_prfmnce_ind, ftpc_ovrl_prfmnce_minutes, ftpc_consignee_id, ftpc_shipper_id, ftpc_close_ind, ftpc_clm_load_status, ftpc_clm_destination, id, modified_date, ev_date_time, trax_created_date_time, trax_updated_date_time, created_date, row_active, record_synced_datetime) VALUES(51183682, 1, 28, 'ARV', NULL, 'INTFREGAT', 'MO', 'KCS', NULL, '2024-04-17 23:30:00.000', 1713391200000, '2024-04-17 19:31:00.000', 1713376860000, 'EST', NULL, NULL, 8, NULL, NULL, 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', NULL, NULL, NULL, NULL, NULL, 'ONTIME', 239, 29024027, 931429, '0', NULL, NULL, 330288083, '2024-04-10 21:39:54.000', NULL, NULL, NULL, '2024-04-10 21:39:54.000', true, '2024-04-30 04:29:50.634');".to_string();

        let mut config = tokio_postgres::Config::new();
        
        config
        .host("127.0.0.1")
        .port(5432)
        .user("production")
        .password("production")
        .dbname("analyticsdb")
        .connect_timeout(Duration::from_secs(30));

        let cancellation_token = CancellationToken::new();
        let mut query_holder_builder = QueryHolderBuilder::new();
        query_holder_builder
        .set_one(query.to_owned())
        .set_two(query.to_owned())
        .set_three(query.to_owned())
        .set_four(query.to_owned())
        .set_five(query.to_owned())
        .set_six(query.to_owned())
        .set_seven(query.to_owned())
        .set_eight(query.to_owned())
        .set_nine(query.to_owned())
        .set_ten(query.to_owned())
        .set_hundred(query.to_owned());

        let query_holder = query_holder_builder.build();

        let mut quick_stream_builder = QuickStreamBuilder::default();

        quick_stream_builder
            .cancellation_tocken(cancellation_token)
            .max_connection_count(10)
            .buffer_size(10)
            .single_digits(1)
            .tens(2)
            .hundreds(1)
            .db_config(config)
            .queries(query_holder)
            .max_records_per_cycle_batch(100)
            .introduced_lag_cycles(1)
            .introduced_lag_in_millies(10)
            .connection_creation_threshold(25.0);

        let upsert_quick_stream = quick_stream_builder.build_update();

        let (tx, rx) = mpsc::channel::<Vec<MockData>>(100);
        let handle = tokio::spawn(async move {
            upsert_quick_stream.run(rx).await;
        });

        let data = vec![
            MockData {
                id: 1,
                modified_date: DateTime::from_timestamp(1627847280, 0).unwrap().naive_local(),
            },
            MockData {
                id: 2,
                modified_date: DateTime::from_timestamp(1627847280, 0).unwrap().naive_local(),
            },
        ];

        tx.send(data.clone()).await.unwrap();
        time::sleep(Duration::from_secs(5)).await;
        tx.send(data).await.unwrap();
        handle.await.unwrap();
    }

}
