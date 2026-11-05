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

package engine

import "time"

// sqlTime returns the SQL that reads t as a value of a time column, holding one placeholder, and the argument to bind
// to it. The time.Time itself is never bound: SQLite would compare the driver's RFC 3339 text (a T where the column
// has a space) against the stored text, PostgreSQL would shift a zoned bind by the session's offset against the
// zone-less column, and SQL Server's driver may send a datetimeoffset. The text, in UTC and cast to the column's type,
// involves no session time zone.
//
// t is rounded UP to the column's precision - microseconds on PostgreSQL, milliseconds elsewhere - which is exact for
// both kinds of bound: a stored value, a whole number of that unit, is at or after t exactly when it is at or after t
// rounded up, and before t exactly when it is before t rounded up. Truncating would drop the values in [t truncated,
// t) from an exclusive upper bound, and admit them to an inclusive lower one. On SQLite the text must also be spelled
// as NOW_UTC() writes it, to the millisecond: there the comparison is of text, and a longer spelling of the same
// instant sorts after the stored one.
func sqlTime(driverName string, t time.Time) (expr string, arg string) {
	unit, layout := time.Millisecond, "2006-01-02 15:04:05.000"
	if driverName == "pgx" {
		unit, layout = time.Microsecond, "2006-01-02 15:04:05.000000"
	}
	if r := t.Truncate(unit); !r.Equal(t) {
		t = r.Add(unit)
	}
	text := t.UTC().Format(layout)
	switch driverName {
	case "pgx":
		return "CAST(? AS TIMESTAMP)", text
	case "mysql":
		return "CAST(? AS DATETIME(3))", text
	case "mssql":
		return "CAST(? AS DATETIME2(3))", text
	default:
		return "?", text
	}
}
