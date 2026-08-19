use rand::RngExt;

pub fn random_string(len: usize) -> String {
    rand::rng()
        .sample_iter(&rand::distr::Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
