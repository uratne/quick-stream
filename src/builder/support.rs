use std::collections::HashMap;

use tokio_postgres::{Client, Statement};

#[derive(Debug, Clone, Default)]
pub struct MultiTableQueryHolder {
    query_map: HashMap<String, QueryHolder>
}

impl MultiTableQueryHolder {
    pub fn new(query_map: HashMap<String, QueryHolder>) -> MultiTableQueryHolder {
        MultiTableQueryHolder { query_map }
    }

    pub fn get(&self, n: &usize) -> MultiTableSingleQueryHolder {
        let mut query_map = HashMap::new();
        for (key, value) in self.query_map.iter() {
            query_map.insert(key.clone(), value.get(n));
        }
        MultiTableSingleQueryHolder { query_map }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MultiTableSingleQueryHolder {
    query_map: HashMap<String, String>
}

impl MultiTableSingleQueryHolder {
    pub fn get(&self, table_name: fn() -> String) -> String {
        match self.query_map.get(&table_name()) {
            Some(query) => query.clone(),
            None => panic!("Query Not Found For Table: {}", table_name()),
        }
    }

    pub async fn prepare(&self, client: &Client) -> HashMap<String, Statement> {
        let mut statement_map = HashMap::new();
        for (key, value) in self.query_map.iter() {
            let statement = client.prepare(value).await.unwrap();
            statement_map.insert(key.clone(), statement);
        }
        statement_map
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryHolder {
    one: String,
    two: String,
    three: String,
    four: String,
    five: String,
    six: String,
    seven: String,
    eight: String,
    nine: String,
    ten: String,
    hundred: String,
}

impl QueryHolder {
    /// Retrieves the query string corresponding to the given number `n`.
    ///
    /// # Arguments
    ///
    /// * `n` - A positive integer representing the query to retrieve.
    ///
    /// # Returns
    ///
    /// * `String` - The query string corresponding to the given number.
    ///
    /// # Panics
    ///
    /// This function will panic if `n` is not a valid query number (1-11) or 100.
    pub(crate) fn get(&self, n: &usize) -> String {
        match n {
            1 => self.one.to_owned(),
            2 => self.two.to_owned(),
            3 => self.three.to_owned(),
            4 => self.four.to_owned(),
            5 => self.five.to_owned(),
            6 => self.six.to_owned(),
            7 => self.seven.to_owned(),
            8 => self.eight.to_owned(),
            9 => self.nine.to_owned(),
            10 => self.ten.to_owned(),
            100 => self.hundred.to_owned(),
            _ => panic!("Invalid query number: {}", n),
        }
    }
}

pub struct QueryHolderBuilder {
    one: Option<String>,
    two: Option<String>,
    three: Option<String>,
    four: Option<String>,
    five: Option<String>,
    six: Option<String>,
    seven: Option<String>,
    eight: Option<String>,
    nine: Option<String>,
    ten: Option<String>,
    hundred: Option<String>,
}

impl QueryHolderBuilder {
    pub fn new() -> QueryHolderBuilder {
        QueryHolderBuilder {
            one: None,
            two: None,
            three: None,
            four: None,
            five: None,
            six: None,
            seven: None,
            eight: None,
            nine: None,
            ten: None,
            hundred: None,
        }
    }

    pub fn set_one(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.one = Some(value);
        self
    }

    pub fn set_two(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.two = Some(value);
        self
    }

    pub fn set_three(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.three = Some(value);
        self
    }

    pub fn set_four(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.four = Some(value);
        self
    }

    pub fn set_five(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.five = Some(value);
        self
    }

    pub fn set_six(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.six = Some(value);
        self
    }

    pub fn set_seven(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.seven = Some(value);
        self
    }

    pub fn set_eight(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.eight = Some(value);
        self
    }

    pub fn set_nine(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.nine = Some(value);
        self
    }

    pub fn set_ten(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.ten = Some(value);
        self
    }

    pub fn set_hundred(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.hundred = Some(value);
        self
    }

    pub fn build(&self) -> QueryHolder {
        if self.one.is_none()
            || self.two.is_none()
            || self.three.is_none()
            || self.four.is_none()
            || self.five.is_none()
            || self.six.is_none()
            || self.seven.is_none()
            || self.eight.is_none()
            || self.nine.is_none()
            || self.ten.is_none()
            || self.hundred.is_none()
        {
            panic!("Some Queries Are Missing")
        } else {
            QueryHolder {
                one: self.one.clone().unwrap(),
                two: self.two.clone().unwrap(),
                three: self.three.clone().unwrap(),
                four: self.four.clone().unwrap(),
                five: self.five.clone().unwrap(),
                six: self.six.clone().unwrap(),
                seven: self.seven.clone().unwrap(),
                eight: self.eight.clone().unwrap(),
                nine: self.nine.clone().unwrap(),
                ten: self.ten.clone().unwrap(),
                hundred: self.hundred.clone().unwrap(),
            }
        }
    }
}


