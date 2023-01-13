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

impl<'p, 'd, T, F, DistFn> DBScanRunner<'p, 'd, T, F, DistFn> {
    pub(crate) unsafe fn new_unchecked(
        param: &'p DBScan<F>,
        data: &'d [T],
        distance: DistFn,
    ) -> Self {
        Self {
            param,
            data,
            distance,
        }
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

    #[test]
    fn test_classify_and_distinct_core_match() {
        let model = DBScan::new(0.5, 3);
        let inputs = vec![0.3f64, 1.0, 1.2, 1.5, 3.6, 3.7, 3.9, 10.0];

        let runner = DBScanRunner::new(&model, &inputs, |a: &f64, b: &f64| (a - b).abs()).unwrap();

        assert!(model
            .oned_classify(&inputs, |a: &f64, b| (a - b).abs())
            .unwrap()
            .0
            .iter()
            .enumerate()
            .all(|(i, class)| match (class, runner.distinct_core(i)) {
                (Class::Core(_), None) => false,
                (Class::Edge(_), None) => true,
                (Class::Noise, None) => true,

                (Class::Core(_), Some(_)) => true,
                (Class::Edge(_), Some(_)) => true,
                (Class::Noise, Some(_)) => false,
            }));
    }
}

#[cfg(test)]
mod tests_bug {
    use super::*;

    const CLASS0: [f64; 173] = [
        15.101792062247112,
        15.136578519322939,
        15.159033642597933,
        15.1690292669864,
        15.174026358525223,
        15.174170689999299,
        15.206163995042516,
        15.207362850977006,
        15.208950081289004,
        15.22100531351316,
        15.221138254528343,
        15.24227742493349,
        15.243325073446613,
        15.247537854284017,
        15.255163659317985,
        15.276423418521516,
        15.277234695095101,
        15.279953118371848,
        15.283018640531738,
        15.305218872448677,
        15.347025425364336,
        15.403949928453585,
        15.406600505304596,
        15.49508496837552,
        15.50246105675433,
        15.516370697059756,
        15.5756416350614,
        15.606085225113931,
        15.62675369292083,
        15.635606999188893,
        15.638545675963542,
        15.647259516987106,
        15.653074753186958,
        15.669674594291793,
        15.67053401876001,
        15.673807434148332,
        15.675316683636995,
        15.687110238524838,
        15.689824357123143,
        15.692186909129305,
        15.707830620385721,
        15.71297230070377,
        15.713400071137583,
        15.726856306795526,
        15.759095539106056,
        15.762686160476733,
        15.76742807118353,
        15.774440505319946,
        15.791748427560377,
        15.795707529996434,
        15.80175309648439,
        15.802491432306853,
        15.836753314521047,
        15.842484593451445,
        15.86317431875068,
        15.882443761287504,
        15.928368040404164,
        15.988001099061874,
        15.990244569455173,
        16.007529230399086,
        16.018057398488963,
        16.024868743363186,
        16.02745431627045,
        16.032489351526237,
        16.033613284454987,
        16.045415503023833,
        16.047216528615536,
        16.057004396625416,
        16.070407552622783,
        16.079285239464298,
        16.07965415039871,
        16.0826578524684,
        16.083551869473013,
        16.087730823996026,
        16.09060360444346,
        16.115340043457763,
        16.132347448376095,
        16.146743568806414,
        16.151199152742265,
        16.156356830839286,
        16.16444123039946,
        16.16824770745916,
        16.16939741950955,
        16.176628317463837,
        16.177412353990803,
        16.178236731069774,
        16.17885009753445,
        16.182478021502902,
        16.209106647014778,
        16.221316905057392,
        16.22371385043516,
        16.23717203303022,
        16.238328865043513,
        16.251838093934566,
        16.25624841879835,
        16.25850443116542,
        16.267569564748555,
        16.26852124749894,
        16.287293259265425,
        16.32677132478966,
        16.341724345669718,
        16.344347847118115,
        16.364696162867403,
        16.36613212591692,
        16.38220865843323,
        16.41531452313575,
        16.422582786672137,
        16.435990663201665,
        16.43896522649993,
        16.440623955713818,
        16.446269338624916,
        16.462971481980276,
        16.468375252716214,
        16.46851219868222,
        16.46957479356024,
        16.47029021858907,
        16.478633469856504,
        16.478696145008144,
        16.48201141988102,
        16.487704278059027,
        16.48955970134375,
        16.490990563092055,
        16.500341159060554,
        16.5054691495352,
        16.513648260261107,
        16.516354916382625,
        16.516397201712607,
        16.52312126720426,
        16.525507396780085,
        16.543328523694072,
        16.54710892356161,
        16.548722444120358,
        16.55090634514636,
        16.55373876925296,
        16.555286924767643,
        16.555340228285786,
        16.55973560152961,
        16.561267422384844,
        16.561417375891324,
        16.562240487381132,
        16.566501538783996,
        16.57305179688774,
        16.578845998536053,
        16.58797161791881,
        16.591369648138425,
        16.59211544575919,
        16.592425018769063,
        16.598512337513966,
        16.603604402844212,
        16.60745768283232,
        16.608810720481415,
        16.608887886342927,
        16.611552143787776,
        16.617137130402625,
        16.622925389438024,
        16.62312703891439,
        16.62828581197573,
        16.63403086643848,
        16.639832266071608,
        16.646810443000504,
        16.6528397799284,
        16.660414730502453,
        16.667106064243853,
        16.6816098033687,
        16.683643174479585,
        16.692373178186244,
        16.71384433523417,
        16.728936199964664,
        16.74542782174649,
        16.755087871475553,
        16.755551916281547,
        16.784188129011454,
        16.844871905153923,
    ];

    const CLASS1: [f64; 31] = [
        16.955376250665722,
        16.981869178760462,
        16.997744234171478,
        17.021030226589573,
        17.04279576445333,
        17.05152695351353,
        17.09629113666233,
        17.104030948114087,
        17.149090851798974,
        17.15390749272956,
        17.243906247313134,
        17.34049835364749,
        17.341086680893568,
        17.493799800942725,
        17.52212565879745,
        17.526859481005886,
        17.542735517703477,
        17.59824898447914,
        17.652772141822425,
        17.681110734289177,
        17.689586827733365,
        17.700363301706602,
        17.709597620460045,
        17.71497397449275,
        17.716055595905345,
        17.743470316347157,
        17.757728501817837,
        17.783604707905397,
        17.788094101912066,
        17.839512890444894,
        17.87599978238177,
    ];

    const CLASS2: [f64; 144] = [
        19.15457283561227,
        19.15937543295695,
        19.16434070433752,
        19.200683912435125,
        19.201994099501462,
        19.202257371378437,
        19.212264074400082,
        19.216905070790745,
        19.221475841335632,
        19.222674302107407,
        19.22522843853949,
        19.225448673039864,
        19.230662347999896,
        19.233328555130356,
        19.233572419429038,
        19.2364618255433,
        19.238521234550717,
        19.239883808591912,
        19.243930043754517,
        19.2464170271287,
        19.246564102510092,
        19.253511016946504,
        19.253632052677858,
        19.257089406493833,
        19.258537660338334,
        19.26122979511274,
        19.26220276926324,
        19.26433378695583,
        19.264531734661432,
        19.26768464437555,
        19.27225250898846,
        19.27249134959493,
        19.278936570206497,
        19.28214941484657,
        19.28812006692715,
        19.289496914398114,
        19.29145031886401,
        19.299607055723754,
        19.303996437151,
        19.309278568638547,
        19.315137201323523,
        19.315450862266516,
        19.31696317904425,
        19.317687737315282,
        19.319663031392338,
        19.32029858688111,
        19.322392879514155,
        19.325865310453082,
        19.332813893155617,
        19.334632571362818,
        19.334982843358375,
        19.342555289063966,
        19.34340732034798,
        19.34795413428037,
        19.348696423397996,
        19.35009995848486,
        19.351327198053696,
        19.35213228813882,
        19.355148756249037,
        19.355732142796114,
        19.357640732703658,
        19.358966498071823,
        19.361083261537715,
        19.361548575565394,
        19.363514677848798,
        19.36648123136729,
        19.37023534158834,
        19.371162974406616,
        19.372033632009334,
        19.374069428207804,
        19.37576175893082,
        19.375877070691786,
        19.376105864454075,
        19.388583935813585,
        19.389632000295023,
        19.39345486827733,
        19.399676981498487,
        19.403872669998236,
        19.405541973157597,
        19.40806667074503,
        19.408127523851363,
        19.41050059360714,
        19.412924484242467,
        19.415392270007942,
        19.417032020654005,
        19.41930359748767,
        19.420744891640425,
        19.425756824743075,
        19.428528081640252,
        19.431249350087455,
        19.43502479381641,
        19.43534258532418,
        19.436361000263787,
        19.437505334473826,
        19.44116957733786,
        19.44380119455309,
        19.44441038432342,
        19.444881826946585,
        19.446667525839075,
        19.448385439209233,
        19.45308254549127,
        19.45363441759946,
        19.45566331320333,
        19.457555642373336,
        19.458337541278524,
        19.4590470592957,
        19.459070823726506,
        19.459456797927487,
        19.46102275591693,
        19.463346030730463,
        19.466272415451385,
        19.46645992887352,
        19.46920242827764,
        19.471583042497514,
        19.47222536066147,
        19.479729774218868,
        19.48147157227322,
        19.48256640040927,
        19.48687669307583,
        19.487212242020178,
        19.48919109752751,
        19.490663346105066,
        19.49262511752204,
        19.49483957713892,
        19.495195820356457,
        19.505835622803716,
        19.511233786812227,
        19.51823606390826,
        19.530373353200048,
        19.530597823959397,
        19.5321956798507,
        19.535362195214475,
        19.539739864811054,
        19.542379434016766,
        19.546179894203306,
        19.552598459356886,
        19.58445080009733,
        19.612604871024814,
        19.612901948670697,
        19.61661681192527,
        19.643330314755076,
        19.65603776399803,
        19.697630766580914,
        19.70057669365633,
    ];

    #[test]
    fn print_test_bug() {
        const DBSCAN_TH: f64 = 0.1;
        const DBSCAN_TH_CLUSTER: usize = 5;
        let model = DBScan::new(DBSCAN_TH, DBSCAN_TH_CLUSTER);

        let data = [CLASS0.to_vec(), CLASS1.to_vec(), CLASS2.to_vec()].concat();
        let point = dbg!(CLASS0.len() + CLASS1.len() - 1);
        model
            .oned_classify(&data, |a: &f64, b| (*a - *b).abs())
            .unwrap()
            .0
            .iter()
            .enumerate()
            .skip(point - 10)
            .take(20)
            .for_each(|(i, c)| println!("{i}, {c:?}, {}", data[i]));
    }

    #[test]
    fn print_test_bug_distinct_core() {
        const DBSCAN_TH: f64 = 0.1;
        const DBSCAN_TH_CLUSTER: usize = 5;
        let model = DBScan::new(DBSCAN_TH, DBSCAN_TH_CLUSTER);

        let data = [CLASS0.to_vec(), CLASS1.to_vec(), CLASS2.to_vec()].concat();
        let runner = DBScanRunner::new(&model, &data, |a: &f64, b: &f64| (a - b).abs()).unwrap();

        let point = dbg!(CLASS0.len() + CLASS1.len() - 1);
        dbg!(runner.distinct_core(point));

        for i in 0..data.len() {
            let Some(max) = runner.distinct_core(i) else { continue; };
            assert!(
                data[i..][..max]
                    .iter()
                    .all(|x| (x - data[i]).abs() <= model.epsilon),
                "{i}"
            );
        }
    }

    #[test]
    fn test_classify_and_distinct_core_match() {
        const DBSCAN_TH: f64 = 0.1;
        const DBSCAN_TH_CLUSTER: usize = 5;
        let model = DBScan::new(DBSCAN_TH, DBSCAN_TH_CLUSTER);
        let data = [CLASS0.to_vec(), CLASS1.to_vec(), CLASS2.to_vec()].concat();
        let runner = DBScanRunner::new(&model, &data, |a: &f64, b: &f64| (a - b).abs()).unwrap();

        assert!(model
            .oned_classify(&data, |a: &f64, b| (a - b).abs())
            .unwrap()
            .0
            .iter()
            .enumerate()
            .all(|(i, class)| match (class, runner.distinct_core(i)) {
                (Class::Core(_), None) => false,
                (Class::Edge(_), None) => true,
                (Class::Noise, None) => true,

                (Class::Core(_), Some(_)) => true,
                (Class::Edge(_), Some(_)) => true,
                (Class::Noise, Some(_)) => false,
            }));
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
