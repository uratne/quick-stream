use std::collections::HashMap;

use tokio_postgres::{Client, Statement};


/// A structure to hold delete queries for multiple tables.
#[derive(Debug, Clone, Default)]
pub struct MultiTableDeleteQueryHolder {
    query_map: HashMap<String, String>
}

impl MultiTableDeleteQueryHolder {
    /// Creates a new `MultiTableDeleteQueryHolder` with the given query map.
    ///
    /// # Arguments
    ///
    /// * `query_map` - A `HashMap` where keys are table names and values are delete queries.
    ///
    /// # Returns
    ///
    /// * `MultiTableDeleteQueryHolder` - A new instance of the struct.
    pub fn new(query_map: HashMap<String, String>) -> MultiTableDeleteQueryHolder {
        MultiTableDeleteQueryHolder { query_map }
    }

    /// Retrieves the delete query for a specific table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table for which to retrieve the query.
    ///
    /// # Returns
    ///
    /// * `String` - The delete query for the specified table.
    ///
    /// # Panics
    ///
    /// This function will panic if the query for the specified table is not found.
    pub fn get(&self, table_name: String) -> String {
        match self.query_map.get(&table_name) {
            Some(query) => query.clone(),
            None => panic!("Query Not Found For Table: {}", table_name),
        }
    }

    /// Prepares all delete queries and returns a map of prepared statements.
    ///
    /// # Arguments
    ///
    /// * `client` - A reference to a `tokio_postgres::Client` to prepare the statements.
    ///
    /// # Returns
    ///
    /// * `HashMap<String, Statement>` - A map where keys are table names and values are prepared statements.
    ///
    /// # Panics
    ///
    /// This function will panic if preparing any of the statements fails.
    pub async fn prepare(&self, client: &Client) -> HashMap<String, Statement> {
        let mut statement_map = HashMap::new();
        for (key, value) in self.query_map.iter() {
            let statement = client.prepare(value).await.unwrap();
            statement_map.insert(key.clone(), statement);
        }
        statement_map
    }
}


/// A structure to hold upsert queries for multiple tables.
#[derive(Debug, Clone, Default)]
pub struct MultiTableUpsertQueryHolder {
    query_map: HashMap<String, QueryHolder>
}

impl MultiTableUpsertQueryHolder {
    /// Creates a new `MultiTableUpsertQueryHolder` with the given query map.
    ///
    /// # Arguments
    ///
    /// * `query_map` - A `HashMap` where keys are table names and values are `QueryHolder` instances.
    ///
    /// # Returns
    ///
    /// * `MultiTableUpsertQueryHolder` - A new instance of the struct.
    pub fn new(query_map: HashMap<String, QueryHolder>) -> MultiTableUpsertQueryHolder {
        MultiTableUpsertQueryHolder { query_map }
    }

    /// Retrieves a `MultiTableSingleQueryHolder` for a specific query number.
    ///
    /// # Arguments
    ///
    /// * `n` - A reference to the query number to retrieve.
    ///
    /// # Returns
    ///
    /// * `MultiTableSingleQueryHolder` - A new instance of the struct with queries for the specified number.
    pub fn get(&self, n: &usize) -> MultiTableSingleQueryHolder {
        let mut query_map = HashMap::new();
        for (key, value) in self.query_map.iter() {
            query_map.insert(key.clone(), value.get(n));
        }
        MultiTableSingleQueryHolder { query_map }
    }
}

/// A structure to hold a single query for multiple tables.
#[derive(Debug, Clone, Default)]
pub struct MultiTableSingleQueryHolder {
    query_map: HashMap<String, String>
}

impl MultiTableSingleQueryHolder {
    /// Retrieves the query for a specific table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table for which to retrieve the query.
    ///
    /// # Returns
    ///
    /// * `String` - The query for the specified table.
    ///
    /// # Panics
    ///
    /// This function will panic if the query for the specified table is not found.
    pub fn get(&self, table_name: String) -> String {
        match self.query_map.get(&table_name) {
            Some(query) => query.clone(),
            None => panic!("Query Not Found For Table: {}", table_name),
        }
    }

    /// Prepares all queries and returns a map of prepared statements.
    ///
    /// # Arguments
    ///
    /// * `client` - A reference to a `tokio_postgres::Client` to prepare the statements.
    ///
    /// # Returns
    ///
    /// * `HashMap<String, Statement>` - A map where keys are table names and values are prepared statements.
    ///
    /// # Panics
    ///
    /// This function will panic if preparing any of the statements fails.
    pub async fn prepare(&self, client: &Client) -> HashMap<String, Statement> {
        let mut statement_map = HashMap::new();
        for (key, value) in self.query_map.iter() {
            let statement = client.prepare(value).await.unwrap();
            statement_map.insert(key.clone(), statement);
        }
        statement_map
    }
}


/// A structure to hold multiple upsert queries.
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
    /// This function will panic if `n` is not a valid query number (1-10) or 100.
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

    /// Sets the query string for a specific number `n`.
    ///
    /// # Arguments
    ///
    /// * `n` - A positive integer representing the query to set.
    /// * `query` - The query string to set.
    ///
    /// # Panics
    ///
    /// This function will panic if `n` is not a valid query number (1-10) or 100.
    pub fn set_n(&mut self, n: usize, query: String) {
        match n {
            1 => self.one = query,
            2 => self.two = query,
            3 => self.three = query,
            4 => self.four = query,
            5 => self.five = query,
            6 => self.six = query,
            7 => self.seven = query,
            8 => self.eight = query,
            9 => self.nine = query,
            10 => self.ten = query,
            100 => self.hundred = query,
            _ => panic!("Invalid query number: {}", n),
        }
    }
}

/// A builder for the `QueryHolder` struct.
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
    /// Creates a new `QueryHolderBuilder`.
    ///
    /// # Returns
    ///
    /// * `QueryHolderBuilder` - A new instance of the builder.
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

    /// Sets the value for the `one` query.
    ///
    /// # Arguments
    ///
    /// * `value` - The query string to set.
    ///
    /// # Returns
    ///
    /// * `&mut QueryHolderBuilder` - A mutable reference to the builder.
    pub fn set_one(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.one = Some(value);
        self
    }

    /// Sets the value for the `two` query.
    ///
    /// # Arguments
    ///
    /// * `value` - The query string to set.
    ///
    /// # Returns
    ///
    /// * `&mut QueryHolderBuilder` - A mutable reference to the builder.
    pub fn set_two(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.two = Some(value);
        self
    }

    /// Sets the value for the `three` query.
    ///
    /// # Arguments
    ///
    /// * `value` - The query string to set.
    ///
    /// # Returns
    ///
    /// * `&mut QueryHolderBuilder` - A mutable reference to the builder.
    pub fn set_three(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.three = Some(value);
        self
    }

    /// Sets the value for the `four` query.
    ///
    /// # Arguments
    ///
    /// * `value` - The query string to set.
    ///
    /// # Returns
    ///
    /// * `&mut QueryHolderBuilder` - A mutable reference to the builder.
    pub fn set_four(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.four = Some(value);
        self
    }

    /// Sets the value for the `five` query.
    ///
    /// # Arguments
    ///
    /// * `value` - The query string to set.
    ///
    /// # Returns
    ///
    /// * `&mut QueryHolderBuilder` - A mutable reference to the builder.
    pub fn set_five(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.five = Some(value);
        self
    }

    /// Sets the value for the `six` query.
    ///
    /// # Arguments
    ///
    /// * `value` - The query string to set.
    ///
    /// # Returns
    ///
    /// * `&mut QueryHolderBuilder` - A mutable reference to the builder.
    pub fn set_six(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.six = Some(value);
        self
    }

    /// Sets the value for the `seven` query.
    ///
    /// # Arguments
    ///
    /// * `value` - The query string to set.
    ///
    /// # Returns
    ///
    /// * `&mut QueryHolderBuilder` - A mutable reference to the builder.
    pub fn set_seven(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.seven = Some(value);
        self
    }

    /// Sets the value for the `eight` query.
    ///
    /// # Arguments
    ///
    /// * `value` - The query string to set.
    ///
    /// # Returns
    ///
    /// * `&mut QueryHolderBuilder` - A mutable reference to the builder.
    pub fn set_eight(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.eight = Some(value);
        self
    }

    /// Sets the value for the `nine` query.
    ///
    /// # Arguments
    ///
    /// * `value` - The query string to set.
    ///
    /// # Returns
    ///
    /// * `&mut QueryHolderBuilder` - A mutable reference to the builder.
    pub fn set_nine(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.nine = Some(value);
        self
    }

    /// Sets the value for the `ten` query.
    ///
    /// # Arguments
    ///
    /// * `value` - The query string to set.
    ///
    /// # Returns
    ///
    /// * `&mut QueryHolderBuilder` - A mutable reference to the builder.
    pub fn set_ten(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.ten = Some(value);
        self
    }

    /// Sets the value for the `hundred` query.
    ///
    /// # Arguments
    ///
    /// * `value` - The query string to set.
    ///
    /// # Returns
    ///
    /// * `&mut QueryHolderBuilder` - A mutable reference to the builder.
    pub fn set_hundred(&mut self, value: String) -> &mut QueryHolderBuilder {
        self.hundred = Some(value);
        self
    }

    /// Builds the `QueryHolder` from the set values.
    ///
    /// # Returns
    ///
    /// * `QueryHolder` - A new instance of the struct.
    ///
    /// # Panics
    ///
    /// This function will panic if any of the query values are not set.
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


