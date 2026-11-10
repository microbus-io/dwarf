/*
Copyright (c) 2026 Microbus LLC and various contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package database

import "time"

// MaxSQLTime is the latest instant every dialect's time column holds.
var MaxSQLTime = time.Date(9999, 12, 31, 23, 59, 59, 999000000, time.UTC)

// SQLTime returns the SQL that reads t as a value of a time column, holding one placeholder, and the argument to bind
// to it. The time.Time itself is never bound: SQLite would compare the driver's RFC 3339 text (a T where the column
// has a space) against the stored text, PostgreSQL would shift a zoned bind by the session's offset against the
// zone-less column, and SQL Server's driver may send a datetimeoffset. The text, in UTC and cast to the column's type,
// involves no session time zone.
//
// t is rounded UP to the columns' precision, milliseconds on every dialect, which is exact for both kinds of bound: a
// stored value, a whole number of milliseconds, is at or after t exactly when it is at or after t rounded up, and before
// t exactly when it is before t rounded up. Truncating would drop the values in [t truncated, t) from an exclusive upper
// bound, and admit them to an inclusive lower one. On SQLite the text must also be spelled as NOW_UTC() writes it, to
// the millisecond: there the comparison is of text, and a longer spelling of the same instant sorts after the stored
// one.
func SQLTime(driverName string, t time.Time) (expr string, arg string) {
	// A bound past the year 9999 is written as its last instant: a five-digit year sorts before every stored time as
	// text on SQLite, is NULL to MySQL's CAST and an error to SQL Server's, so a far-future sentinel would mean nothing,
	// or fail, depending on the dialect.
	if t.After(MaxSQLTime) {
		t = MaxSQLTime
	}
	if r := t.Truncate(time.Millisecond); !r.Equal(t) {
		t = r.Add(time.Millisecond)
	}
	const layout = "2006-01-02 15:04:05.000"
	text := t.UTC().Format(layout)
	switch driverName {
	case "pgx":
		return "CAST(? AS TIMESTAMP(3))", text
	case "mysql", "mariadb":
		return "CAST(? AS DATETIME(3))", text
	case "mssql":
		return "CAST(? AS DATETIME2(3))", text
	default:
		return "?", text
	}
}
