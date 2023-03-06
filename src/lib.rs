//! Reference: https://github.com/lazear/dbscan which licenced under MIT Licence

mod nd;
mod oned;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct DBScan<F> {
    pub epsilon: F,
    pub min: usize,
}

impl<F: PartialOrd> DBScan<F> {
    pub const fn new(epsilon: F, min: usize) -> Self {
        Self { epsilon, min }
    }

    pub fn classify<T, DistFn>(&self, data: &[T], distance: DistFn) -> (Vec<Class>, Option<ClassId>)
    where
        DistFn: Fn(&T, &T) -> F,
    {
        nd::DBScanRunner::new(self, data, distance).classify()
    }

    pub fn oned_classify<T, DistFn>(
        &self,
        data: &[T],
        distance: DistFn,
    ) -> Result<(Vec<Class>, Option<ClassId>), DBScanError>
    where
        DistFn: Fn(&T, &T) -> F,
        T: PartialOrd,
    {
        let runner = oned::DBScanRunner::new(self, data, distance)?;
        Ok(runner.classify())
    }

    /// # Safety
    ///
    /// This function assume data is sorted.
    pub unsafe fn oned_classify_unchecked<T, DistFn>(
        &self,
        data: &[T],
        distance: DistFn,
    ) -> (Vec<Class>, Option<ClassId>)
    where
        DistFn: Fn(&T, &T) -> F,
    {
        let runner = oned::DBScanRunner::new_unchecked(self, data, distance);
        runner.classify()
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
