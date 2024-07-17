use std::collections::HashMap;

use crate::remove_delete_duplicates;

use super::MultiTableDelete;

#[derive(Debug, Clone)]
pub struct DataHolder<T> where T: MultiTableDelete<T> + Send + Clone {
    data: HashMap<String, Vec<T>>
}

impl<T> Default for DataHolder<T> where T: MultiTableDelete<T> + Send + Clone {
    fn default() -> Self {
        let mut data = HashMap::with_capacity(T::tables().len());
        for table_name in T::tables() {
            data.insert(table_name.to_string(), Vec::new());
        }
        DataHolder { data}
    }
}

impl<T> DataHolder<T> where T: MultiTableDelete<T> + Send + Clone + 'static {
    fn add(&mut self, datam: T) {
        let table_name = datam.table();
        self.data.get_mut(&table_name).unwrap().push(datam);
    }

    pub fn add_all(&mut self, data: Vec<T>, limit: usize) -> HashMap<String, Vec<T>>{
        for datam in data {
            self.add(datam);
        };

        self.remove_duplicates();
        self.get_over_limit(limit)
    }

    pub fn remove_duplicates(&mut self) {
        for (_, data) in self.data.iter_mut() {
            remove_delete_duplicates(data);
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn get_over_limit(&mut self, limit: usize) -> HashMap<String, Vec<T>> {

        let mut cloned_data = self.data.to_owned();
        self.data.retain(|_, x| x.len() < limit);
        cloned_data.retain(|_, y| y.len() >= limit);

        cloned_data
    }

    pub fn get_all(&self) -> HashMap<String, Vec<T>> {
        self.data.to_owned()
    }
}