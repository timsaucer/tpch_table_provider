pub const TPCH_QUERY_01: &'static str = include_str!("queries/q1.sql");
pub const TPCH_QUERY_02: &'static str = include_str!("queries/q2.sql");
pub const TPCH_QUERY_03: &'static str = include_str!("queries/q3.sql");
pub const TPCH_QUERY_04: &'static str = include_str!("queries/q4.sql");
pub const TPCH_QUERY_05: &'static str = include_str!("queries/q5.sql");
pub const TPCH_QUERY_06: &'static str = include_str!("queries/q6.sql");
pub const TPCH_QUERY_07: &'static str = include_str!("queries/q7.sql");
pub const TPCH_QUERY_08: &'static str = include_str!("queries/q8.sql");
pub const TPCH_QUERY_09: &'static str = include_str!("queries/q9.sql");
pub const TPCH_QUERY_10: &'static str = include_str!("queries/q10.sql");
pub const TPCH_QUERY_11: &'static str = include_str!("queries/q11.sql");
pub const TPCH_QUERY_12: &'static str = include_str!("queries/q12.sql");
pub const TPCH_QUERY_13: &'static str = include_str!("queries/q13.sql");
pub const TPCH_QUERY_14: &'static str = include_str!("queries/q14.sql");
pub const TPCH_QUERY_15: &'static str = include_str!("queries/q15.sql");
pub const TPCH_QUERY_16: &'static str = include_str!("queries/q16.sql");
pub const TPCH_QUERY_17: &'static str = include_str!("queries/q17.sql");
pub const TPCH_QUERY_18: &'static str = include_str!("queries/q18.sql");
pub const TPCH_QUERY_19: &'static str = include_str!("queries/q19.sql");
pub const TPCH_QUERY_20: &'static str = include_str!("queries/q20.sql");
pub const TPCH_QUERY_21: &'static str = include_str!("queries/q21.sql");
pub const TPCH_QUERY_22: &'static str = include_str!("queries/q22.sql");

pub fn get_tpch_query(query: usize) -> Result<&'static str, &'static str> {
    match query {
        1 => Ok(TPCH_QUERY_01),
        2 => Ok(TPCH_QUERY_02),
        3 => Ok(TPCH_QUERY_03),
        4 => Ok(TPCH_QUERY_04),
        5 => Ok(TPCH_QUERY_05),
        6 => Ok(TPCH_QUERY_06),
        7 => Ok(TPCH_QUERY_07),
        8 => Ok(TPCH_QUERY_08),
        9 => Ok(TPCH_QUERY_09),
        10 => Ok(TPCH_QUERY_10),
        11 => Ok(TPCH_QUERY_11),
        12 => Ok(TPCH_QUERY_12),
        13 => Ok(TPCH_QUERY_13),
        14 => Ok(TPCH_QUERY_14),
        15 => Ok(TPCH_QUERY_15),
        16 => Ok(TPCH_QUERY_16),
        17 => Ok(TPCH_QUERY_17),
        18 => Ok(TPCH_QUERY_18),
        19 => Ok(TPCH_QUERY_19),
        20 => Ok(TPCH_QUERY_20),
        21 => Ok(TPCH_QUERY_21),
        22 => Ok(TPCH_QUERY_22),
        _ => Err("Invalid query number. Must be 1-22"),
    }
}
