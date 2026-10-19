use std::time::{Duration, SystemTime};

use super::{
    eval::from_unix_seconds,
    model::{
        ABORT_DAYS_MIN, AND_ARITY, DATE_MIDNIGHT, DAYS_AND_DATE, DAYS_MIN, DELETE_MARKER_DAYS,
        FILTER_ARITY, MAX_RULE_ID, MAX_RULES, MAX_SIZE, NEWER_NEEDS_FILTER, NEWER_RANGE, NO_ACTION,
        NO_RULES, NONCURRENT_DAYS_MIN, RULE_ID_DUPLICATE, RULE_ID_TOO_LONG, SIZE_CEILING,
        SIZE_ORDER, TAG_FILTER_ACTION, TAG_KEYS_UNIQUE, TOO_MANY_RULES,
    },
    *,
};
use crate::object;

include!("shape_tests.rs");
include!("match_tests.rs");
