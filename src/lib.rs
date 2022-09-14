//! Reference: https://github.com/lazear/dbscan which licenced under MIT Licence

mod nd;
mod oned;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DBScan<F> {
    epsilon: F,
    min: usize,
}

impl<F: PartialOrd> DBScan<F> {
    pub fn new(epsilon: F, min: usize) -> Self {
        Self { epsilon, min }
    }

    pub fn classify<T, DistFn>(&self, data: &[T], distance: DistFn) -> Vec<Class>
    where
        DistFn: Fn(&T, &T) -> F,
    {
        nd::DBScanRunner::new(self, data, distance).classify()
    }

    pub fn oned_classify<T, DistFn>(
        &self,
        data: &[T],
        distance: DistFn,
    ) -> Result<Vec<Class>, DBScanError>
    where
        DistFn: Fn(&T, &T) -> F,
        T: PartialOrd,
    {
        oned::DBScanRunner::new(self, data, distance).map(|x| x.classify())
    }
}

#[derive(Debug, Clone, Default, Copy, Hash, PartialEq, Eq)]
pub enum Class {
    /// A point with at least `min_points` neighbors within `eps` diameter
    Core(ClassId),

    /// A point within `eps` of a core point, but has less than `min_points` neighbors
    Edge(ClassId),

    /// A point with no connections
    #[default]
    Noise,
}

/// DBScan Class ID
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct ClassId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct DataIdx(usize);
impl<T> std::ops::Index<DataIdx> for [T] {
    type Output = T;

    fn index(&self, index: DataIdx) -> &Self::Output {
        let idx = index.0;
        &self[idx]
    }
}

impl<T> std::ops::Index<DataIdx> for Vec<T> {
    type Output = T;

    fn index(&self, index: DataIdx) -> &Self::Output {
        let idx = index.0;
        &self[idx]
    }
}

impl<T> std::ops::IndexMut<DataIdx> for Vec<T> {
    fn index_mut(&mut self, index: DataIdx) -> &mut Self::Output {
        let idx = index.0;
        &mut self[idx]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DBScanError {
    DataIsNotSorted,
}
