//! Mappings between TDS and and time crate types (with `time` feature flag
//! enabled).
//!
//! The time library offers better ergonomy and are highly recommended if
//! needing to modify and deal with date and time in SQL Server.

use std::time::Duration;
pub use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

use crate::protocol::wire::SqlValue;

#[inline]
fn from_days(days: u64, start_year: i32) -> Date {
    Date::from_calendar_date(start_year, Month::January, 1).unwrap()
        + Duration::from_secs(60 * 60 * 24 * days)
}

#[inline]
fn from_secs(secs: u64) -> Time {
    Time::from_hms(0, 0, 0).unwrap() + Duration::from_secs(secs)
}

#[inline]
fn from_sec_fragments(sec_fragments: u64) -> Time {
    Time::from_hms(0, 0, 0).unwrap() + Duration::from_nanos(sec_fragments * (1e9 as u64) / 300)
}

#[inline]
fn to_days(date: Date, start_year: i32) -> i64 {
    (date - Date::from_calendar_date(start_year, Month::January, 1).unwrap()).whole_days()
}

// Old TDS version support removed - TDS 7.4 only

from_sql!(
    PrimitiveDateTime:
        SqlValue::SmallDateTime(ref dt) => dt.map(|dt| PrimitiveDateTime::new(
            from_days(dt.days as u64, 1900),
            from_secs(dt.seconds_fragments as u64 * 60),
        )),
        SqlValue::DateTime2(ref dt) => dt.map(|dt| PrimitiveDateTime::new(
            from_days(dt.date.days() as u64, 1),
            Time::from_hms(0,0,0).unwrap() + Duration::from_nanos(dt.time.increments * 10u64.pow(9 - dt.time.scale as u32))
        )),
        SqlValue::DateTime(ref dt) => dt.map(|dt| PrimitiveDateTime::new(
            from_days(dt.days as u64, 1900),
            from_sec_fragments(dt.seconds_fragments as u64)
        ));
    Time:
        SqlValue::Time(ref time) => time.map(|time| {
            let ns = time.increments * 10u64.pow(9 - time.scale as u32);
            Time::from_hms(0,0,0).unwrap() + Duration::from_nanos(ns)
        });
    Date:
        SqlValue::Date(ref date) => date.map(|date| from_days(date.days() as u64, 1));
    OffsetDateTime:
        SqlValue::DateTimeOffset(ref dto) => dto.map(|dto| {
            let date = from_days(dto.datetime2.date.days() as u64, 1);
            let dt = dto.datetime2;

            let time = Time::from_hms(0,0,0).unwrap()
                + Duration::from_nanos(dt.time.increments * 10u64.pow(9 - dt.time.scale as u32));

            let offset = UtcOffset::from_whole_seconds(dto.offset as i32 * 60).unwrap();

            date.with_time(time).assume_utc().to_offset(offset)
        })
);

to_sql!(self_,
        Date: (SqlValue::Date, super::Date::new(to_days(*self_, 1) as u32));
        Time: (SqlValue::Time, {
            let nanos: u64 = (*self_ - Time::from_hms(0, 0, 0).unwrap()).whole_nanoseconds().try_into().unwrap();
            let increments = nanos / 100;

            super::Time {increments, scale: 7}
        });
        PrimitiveDateTime: (SqlValue::DateTime2, {
            let time = self_.time();
            let nanos: u64 = (time - Time::from_hms(0, 0, 0).unwrap()).whole_nanoseconds().try_into().unwrap();
            let increments = nanos / 100;

            let date = super::Date::new(to_days(self_.date(), 1) as u32);
            let time = super::Time {increments, scale: 7};

            super::DateTime2::new(date, time)
        });
        OffsetDateTime: (SqlValue::DateTimeOffset, {
            let tz = self_.offset();
            let offset = (tz.whole_seconds() / 60) as i16;

            let utc_date = self_.to_offset(UtcOffset::UTC);

            let nanos: u64 = (utc_date.time() - Time::from_hms(0, 0, 0).unwrap()).whole_nanoseconds().try_into().unwrap();

            let date = super::Date::new(to_days(utc_date.date(), 1) as u32);
            let time = super::Time { increments: nanos / 100, scale: 7 };

            super::DateTimeOffset::new(super::DateTime2::new(date, time), offset)
        });
);

// Old TDS version support removed - TDS 7.4 only
