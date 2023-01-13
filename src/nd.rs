use crate::{Class, ClassId, DBScan, DataIdx};

pub struct DBScanRunner<'p, 'd, T, F, DistFn> {
    param: &'p DBScan<F>,
    class: Vec<Class>,
    is_visited: Vec<bool>,
    data: &'d [T],
    distance: DistFn,
}

impl<'p, 'd, T, F, DistFn> DBScanRunner<'p, 'd, T, F, DistFn> {
    pub fn new(param: &'p DBScan<F>, data: &'d [T], distance: DistFn) -> Self {
        Self {
            param,
            class: vec![Class::Noise; data.len()],
            is_visited: vec![false; data.len()],
            data,
            distance,
        }
    }
}

impl<'d, F, T, D> DBScanRunner<'_, 'd, T, F, D> {
    fn data_iter_enumerate(&self) -> impl Iterator<Item = (DataIdx, &'d T)> {
        self.data
            .iter()
            .enumerate()
            .map(|(i, data)| (DataIdx(i), data))
    }
}

impl<F, T, DistFn> DBScanRunner<'_, '_, T, F, DistFn>
where
    DistFn: Fn(&T, &T) -> F,
    F: PartialOrd,
{
    pub fn classify(mut self) -> (Vec<Class>, Option<ClassId>) {
        if self.data.is_empty() {
            return (Vec::new(), None);
        }
        let mut next_cluster = ClassId(0);
        for (idx, sample) in self.data_iter_enumerate() {
            let idx_is_visited = self.is_visited[idx];
            if !idx_is_visited {
                self.is_visited[idx] = true;

                let near = self.range_query(sample);
                if near.len() >= self.param.min {
                    self.class[idx] = Class::Core(next_cluster);
                    self.expand_cluster(&near, next_cluster);
                    next_cluster.0 += 1;
                };
            }
        }
        next_cluster.0 -= 1;
        (self.class, Some(next_cluster))
    }

    fn range_query(&self, sample: &T) -> Vec<DataIdx> {
        self.data_iter_enumerate()
            .filter(move |(_, point)| (self.distance)(sample, point) < self.param.epsilon)
            .map(|(i, _)| i)
            .collect()
    }

    fn expand_cluster(&mut self, neighbors: &[DataIdx], next_cluster: ClassId) {
        for &idx in neighbors {
            if self.class[idx] == Class::Noise {
                self.class[idx] = Class::Edge(next_cluster);
            }

            if !self.is_visited[idx] {
                self.is_visited[idx] = true;

                let n = self.range_query(&self.data[idx]);
                if n.len() >= self.param.min {
                    self.class[idx] = Class::Core(next_cluster);
                    self.expand_cluster(&n, next_cluster);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster() {
        let model = DBScan::new(1.0, 3);
        let inputs = vec![
            vec![1.5f64, 2.2],
            vec![1.0, 1.1],
            vec![1.2, 1.4],
            vec![0.8, 1.0],
            vec![3.7, 4.0],
            vec![3.9, 3.9],
            vec![3.6, 4.1],
            vec![10.0, 10.0],
        ];

        let output = model.classify(&inputs, |a, b| {
            std::iter::zip(a, b)
                .map(|(a, b)| (a - b).powi(2))
                .sum::<f64>()
                .sqrt()
        });

        assert_eq!(
            output,
            (
                vec![
                    Class::Edge(ClassId(0)),
                    Class::Core(ClassId(0)),
                    Class::Core(ClassId(0)),
                    Class::Core(ClassId(0)),
                    Class::Core(ClassId(1)),
                    Class::Core(ClassId(1)),
                    Class::Core(ClassId(1)),
                    Class::Noise
                ],
                Some(ClassId(1))
            )
        );
    }

    #[test]
    fn cluster_edge() {
        let model = DBScan::new(0.253110, 3);
        let inputs = vec![
            vec![
                0.3311755015020835f64,
                0.20474852214361858,
                0.21050489388506638,
                0.23040992344219402,
                0.023161159027037505,
            ],
            vec![
                0.5112445458548497,
                0.1898442816540571,
                0.11674072294944157,
                0.14853288499259437,
                0.03363756454905728,
            ],
            vec![
                0.581134172697341,
                0.15084733646825743,
                0.09997992993087741,
                0.13580335513916678,
                0.03223520576435743,
            ],
            vec![
                0.17210416043100868,
                0.3403172702783598,
                0.18218098373740396,
                0.2616980943829193,
                0.04369949117030829,
            ],
        ];
        let output = model.classify(&inputs, |a, b| {
            std::iter::zip(a, b)
                .map(|(a, b)| (a - b).powi(2))
                .sum::<f64>()
                .sqrt()
        });
        assert_eq!(
            output,
            (
                vec![
                    Class::Core(ClassId(0)),
                    Class::Core(ClassId(0)),
                    Class::Edge(ClassId(0)),
                    Class::Edge(ClassId(0))
                ],
                Some(ClassId(0))
            )
        );
    }

    #[test]
    fn range_query() {
        let model = DBScan::new(1.0, 3);
        let inputs = vec![vec![1.0f64, 1.0], vec![1.1, 1.9], vec![3.0, 3.0]];
        let dist = |a: &Vec<f64>, b: &Vec<f64>| -> f64 {
            std::iter::zip(a, b)
                .map(|(a, b)| (a - b).powi(2))
                .sum::<f64>()
                .sqrt()
        };
        let runner = DBScanRunner::new(&model, &inputs, dist);

        let neighbours = runner.range_query(&vec![1.0, 1.0]);

        assert!(neighbours.len() == 2);
    }

    #[test]
    fn range_query_small_eps() {
        let model = DBScan::new(0.01, 3);
        let inputs = vec![vec![1.0f64, 1.0], vec![1.1, 1.9], vec![3.0, 3.0]];
        let dist = |a: &Vec<f64>, b: &Vec<f64>| -> f64 {
            std::iter::zip(a, b)
                .map(|(a, b)| (a - b).powi(2))
                .sum::<f64>()
                .sqrt()
        };
        let runner = DBScanRunner::new(&model, &inputs, dist);

        let neighbours = runner.range_query(&vec![1.0, 1.0]);

        assert!(neighbours.len() == 1);
    }
}
