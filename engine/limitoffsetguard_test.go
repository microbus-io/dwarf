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

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/microbus-io/testarossa"
)

// TestNoLimitOffsetWithoutOrderBy is a source-level tripwire for a dialect-portability rule: sequel's
// LIMIT_OFFSET(...) macro compiles to LIMIT...OFFSET on mysql/pgx/sqlite but to OFFSET...ROWS FETCH
// NEXT...ROWS ONLY on SQL Server, which is a SYNTAX ERROR without a
// preceding ORDER BY ("Invalid usage of the option NEXT in the FETCH statement"). Every LIMIT_OFFSET must
// therefore carry an ORDER BY - even a pure existence probe where ordering is semantically irrelevant.
//
// This exact class broke SQL Server CI once (the reaper's due-root SELECT) and passes silently on the other
// three dialects, so it is worth a permanent guard rather than a dialect-CI round-trip. The check walks each
// non-test source file's AST, flattens every string-concatenation chain (so an ORDER BY and a LIMIT_OFFSET in
// different `+`-joined literals of the same statement are seen together), and fails any flattened SQL that
// contains "LIMIT_OFFSET(" but not "ORDER BY".
func TestNoLimitOffsetWithoutOrderBy(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	goFiles, err := filepath.Glob("*.go")
	assert.NoError(err)

	fset := token.NewFileSet()
	for _, path := range goFiles {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if !assert.NoError(err, "parsing %s", path) {
			continue
		}

		check := func(sql string, pos token.Pos) {
			if strings.Contains(sql, "LIMIT_OFFSET(") && !strings.Contains(sql, "ORDER BY") {
				p := fset.Position(pos)
				t.Errorf("LIMIT_OFFSET without a preceding ORDER BY (invalid OFFSET/FETCH on SQL Server) at %s:%d: %q",
					filepath.Base(p.Filename), p.Line, sql)
			}
		}

		ast.Inspect(file, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.BinaryExpr:
				if x.Op == token.ADD {
					// Flatten the whole concatenation chain and check it as one SQL string, then stop
					// descending so nested sub-chains are not re-checked in isolation (which would false-
					// positive when the ORDER BY lives in a sibling operand).
					check(flattenStringConcat(x), x.Pos())
					return false
				}
			case *ast.BasicLit:
				// A lone string literal (not part of a `+` chain, since those are handled above and stop
				// descent) carrying the whole statement.
				if x.Kind == token.STRING {
					if s, err := strconv.Unquote(x.Value); err == nil {
						check(s, x.Pos())
					}
				}
			}
			return true
		})
	}
}

// flattenStringConcat reduces an expression to the concatenation of its string-literal parts, replacing every
// non-literal operand (an identifier such as a status constant, a call, etc.) with a single space so adjacent
// literals do not fuse across a dynamic gap. It is only meaningful for `+` chains of SQL fragments.
func flattenStringConcat(expr ast.Expr) string {
	switch x := expr.(type) {
	case *ast.BinaryExpr:
		if x.Op == token.ADD {
			return flattenStringConcat(x.X) + flattenStringConcat(x.Y)
		}
		return " "
	case *ast.BasicLit:
		if x.Kind == token.STRING {
			if s, err := strconv.Unquote(x.Value); err == nil {
				return s
			}
		}
		return " "
	default:
		return " "
	}
}
