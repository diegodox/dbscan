use crate::{Class, ClassId, DBScan, DBScanError, DataIdx};

pub(crate) struct DBScanRunner<'p, 'd, T, F, DistFn> {
    param: &'p DBScan<F>,
    // class: Vec<Class>,
    // is_visited: Vec<bool>,
    data: &'d [T],
    distance: DistFn,
}

impl<'p, 'd, T, F, DistFn> DBScanRunner<'p, 'd, T, F, DistFn>
where
    T: PartialOrd,
{
    pub(crate) fn new(
        param: &'p DBScan<F>,
        data: &'d [T],
        distance: DistFn,
    ) -> Result<Self, DBScanError> {
        if !data._is_sorted() {
            return Err(DBScanError::DataIsNotSorted);
        }
        Ok(Self {
            param,
            data,
            distance,
        })
    }
}

impl<F, T, DistFn> DBScanRunner<'_, '_, T, F, DistFn>
where
    DistFn: Fn(&T, &T) -> F,
    F: PartialOrd,
{
    /// Do classify
    pub(crate) fn classify(self) -> (Vec<Class>, ClassId) {
        let mut ret = vec![Class::Noise; self.data.len()];
        let mut cursor = DataIdx(0);
        let mut current_class_id = ClassId(0);
        while cursor < DataIdx(self.data.len()) {
            match self.distinct_core(cursor.0) {
                None => {
                    cursor.0 += 1;
                    continue;
                }
                Some(0) => unreachable!(),
                Some(1) => {
                    cursor.0 += 1;
                    current_class_id.0 += 1;
                }
                Some(k) => {
                    ret[cursor.0..][..k]
                        .iter_mut()
                        .for_each(|i| *i = Class::Core(current_class_id));
                    cursor.0 += k - 1;
                }
            }
        }
        current_class_id.0 -= 1;
        (ret, current_class_id)
    }

    /// Return Some(number of point in epsilon) if is core,
    /// otherwise return None.
    pub fn distinct_core(&self, i: usize) -> Option<usize> {
        let min = self.data[..=i]
            .partition_point(|x| (self.distance)(x, &self.data[i]) > self.param.epsilon);
        let max = self.data[i..]
            .partition_point(|x| (self.distance)(x, &self.data[i]) <= self.param.epsilon);
        (i + max - min >= self.param.min).then_some(max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_core() {
        let model = DBScan::new(0.5, 3);
        let inputs = vec![0.3f64, 1.0, 1.2, 1.5, 3.6, 3.7, 3.9, 10.0];

        let runner = DBScanRunner::new(&model, &inputs, |a: &f64, b: &f64| (a - b).abs()).unwrap();
        assert_eq!(runner.distinct_core(0), None);
        assert_eq!(runner.distinct_core(1), Some(3));
        assert_eq!(runner.distinct_core(2), Some(2));
        assert_eq!(runner.distinct_core(3), Some(1));

        assert_eq!(runner.distinct_core(4), Some(3));
        assert_eq!(runner.distinct_core(5), Some(2));
        assert_eq!(runner.distinct_core(6), Some(1));

        assert_eq!(runner.distinct_core(7), None);

        for i in 0..inputs.len() {
            let Some(max) = runner.distinct_core(i) else { continue; };
            assert!(
                inputs[i..][..max]
                    .iter()
                    .all(|x| (x - inputs[i]).abs() <= 0.5),
                "{i}"
            );
        }
    }

    #[test]
    fn cluster() {
        let model = DBScan::new(0.5, 3);
        let inputs = vec![0.3f64, 1.0, 1.2, 1.5, 3.6, 3.7, 3.9, 10.0];

        let output = model.oned_classify(&inputs, |a, b| (a - b).abs());

        assert_eq!(
            output,
            Ok((
                vec![
                    Class::Noise,
                    Class::Core(ClassId(0)),
                    Class::Core(ClassId(0)),
                    Class::Core(ClassId(0)),
                    Class::Core(ClassId(1)),
                    Class::Core(ClassId(1)),
                    Class::Core(ClassId(1)),
                    Class::Noise
                ],
                ClassId(1)
            ))
        );
    }
}

/// Extension trait for slice, impl is_sorted
trait SliceIsSortedEx {
    fn _is_sorted(&self) -> bool;
}

impl<U: PartialOrd> SliceIsSortedEx for &[U] {
    fn _is_sorted(&self) -> bool {
        iter_is_sorted_by(self.as_ref().iter(), |a, b| a.partial_cmp(b))
    }
}

/// Checks if the elements of this iterator are sorted using the given comparator function.
fn iter_is_sorted_by<I, F>(mut iter: I, compare: F) -> bool
where
    I: Iterator,
    F: FnMut(&I::Item, &I::Item) -> Option<std::cmp::Ordering>,
{
    #[inline]
    fn check<'a, T>(
        last: &'a mut T,
        mut compare: impl FnMut(&T, &T) -> Option<std::cmp::Ordering> + 'a,
    ) -> impl FnMut(T) -> bool + 'a {
        move |curr| {
            if let Some(std::cmp::Ordering::Greater) | None = compare(last, &curr) {
                return false;
            }
            *last = curr;
            true
        }
    }

    let mut last = match iter.next() {
        Some(e) => e,
        None => return true,
    };

    iter.all(check(&mut last, compare))
}
