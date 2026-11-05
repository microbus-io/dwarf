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

// sqlTimeLayout is the text of a time column's value as every dialect reads it: UTC, to the millisecond. It is also how
// SQLite stores one (sequel's NOW_UTC()), so there the comparison is of text against text.
const sqlTimeLayout = "2006-01-02 15:04:05.000"

// sqlTime returns the SQL that reads t as a value of a time column, holding one placeholder, and the argument to bind
// to it. The time.Time itself is never bound: SQLite would compare the driver's RFC 3339 text (a T where the column
// has a space) against the stored text, PostgreSQL would shift a zoned bind by the session's offset against the
// zone-less column, and SQL Server's driver may send a datetimeoffset. The text, cast to the column's type, involves
// no session time zone.
func sqlTime(driverName string, t time.Time) (expr string, arg string) {
	text := t.UTC().Format(sqlTimeLayout)
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
