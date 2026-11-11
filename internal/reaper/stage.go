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

package reaper

import (
	"context"
	"database/sql"
	"strconv"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// stageGroup is how many of a tree's descendant flows share one archive time in a staged copy. It is a fixed constant,
// never the batch budget, so a flow's archive time is a function of the tree alone.
const stageGroup = 4096

// errNotToday rolls back an attempt whose new time fell outside the filing day: past midnight, which the quiet margin
// keeps rare.
var errNotToday = errors.New("the attempt's time is not in today's archive day")

// stagePos is where a staged copy of one tree stands, in memory, held by the Reaper between slices (stages). at is the
// attempt it belongs to; seen counts the descendants copied whole, in flow_id order up to after; of flow, part of whose
// steps are copied (a descendant, or the root at the end), every step up to stepAfter is. A position for another
// attempt is not used: its fresh position is reconstructed from the destination's ordered prefixes.
type stagePos struct {
	at              time.Time
	seen, after     int
	flow, stepAfter int
}

// stage archives the live tree of root in stages into the day of its archive attempt: it starts the attempt or joins
// the one there, copies the tree in transactions of about BatchRows rows until the walk's slice (begun at sliceStart)
// ends, and publishes it - the archive root inserted and the live root stamped for deletion, attempt kept - in the last
// one. The live tree is then the deletion kind's. published is the flows archived when this call published the tree;
// finished is true when it did, or when the tree is not this walk's to stage now: stamped, no longer due, held by
// another transaction, or waiting for its day's next day to publish. A copy held or waiting keeps its position.
func (p *pass) stage(ctx context.Context, k kind, root int, d *filingDay, sliceStart time.Time) (published int, finished bool, err error) {
	key := stageKey{p.shard.Index, root}
	// yield ends the call on a tree this attempt cannot copy now: its position is forgotten only once the tree is not
	// the copy's at all - published, stamped, gone - and kept while another transaction merely holds its root.
	yield := func() (int, bool, error) {
		if p.treeGone(ctx, root) {
			p.r.stages.drop(key)
			p.r.prefer.drop(key)
		}
		return 0, true, nil
	}
	at, ok, err := p.claimAttempt(ctx, k, root, d, time.Time{})
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if !ok {
		return yield()
	}
	p.r.prefer.drop(key) // the attempt records it now
	pos := p.r.stages.get(key)
	if !pos.at.Equal(at) {
		pos = stagePos{at: at}
	}
	for {
		n, copied, done, err := p.stageChunk(ctx, root, d.archive, &pos)
		p.r.metrics.rowsStaged(ctx, p.shard.Index, copied)
		if sequel.IsUndefinedTableError(err) {
			// The attempt's day was dropped, and its rows with it: a fresh attempt into today, in a transaction of its
			// own, since the statement that found the day missing ended this one.
			p.r.stages.drop(key)
			_, _, err := p.claimAttempt(ctx, k, root, d, at)
			return 0, false, errors.Trace(err)
		}
		if err != nil {
			p.r.stages.put(key, pos)
			return 0, false, errors.Trace(err)
		}
		p.r.seams.Checkpoint(ctx, CheckpointStageChunkDone)
		if n > 0 {
			p.r.stages.drop(key)
			return n, true, nil
		}
		p.r.stages.put(key, pos)
		if done {
			return yield()
		}
		if ctx.Err() != nil || time.Since(sliceStart) >= p.cfg.Slice {
			return 0, false, nil
		}
	}
}

// treeGone reports whether root's tree is no longer a staged copy's to finish: its live root missing or stamped. A
// read that fails is taken for no, so the position is kept.
func (p *pass) treeGone(ctx context.Context, root int) bool {
	ctx, cancel := p.attemptCtx(ctx)
	defer cancel()
	var stamp int
	err := p.shard.DB.QueryRowContext(ctx, "SELECT delete_after_ms FROM dwarf_flows WHERE flow_id=?", root).Scan(&stamp)
	return err == sql.ErrNoRows || (err == nil && stamp > 0)
}

// claimAttempt starts or joins root's archive attempt, under the live root's lock, while root is still due for k: an
// attempt there is joined, and none there starts one at the database's now. stale, when not zero, is an attempt whose
// day was dropped: found still there, it is replaced by a new one. A new attempt starts only before d's stop and in d's
// day, so its stamp groups have the quiet margin's room. ok is false when no attempt holds: root not due, held, or no
// room to start one.
func (p *pass) claimAttempt(ctx context.Context, k kind, root int, d *filingDay, stale time.Time) (at time.Time, ok bool, err error) {
	ctx, cancel := p.attemptCtx(ctx)
	defer cancel()
	db := p.shard.DB
	id := strconv.Itoa(root)
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		at, ok = time.Time{}, false
		locked, err := lockRoots(ctx, tx, live.flows, true, "flow_id="+id+" AND "+k.due, k.dueArgs...)
		if err != nil || len(locked) == 0 {
			return errors.Trace(err)
		}
		if at, err = attemptOf(ctx, tx, root); err != nil {
			return errors.Trace(err)
		}
		if at.After(noAttempt) && !at.Equal(stale) {
			ok = true // join
			return nil
		}
		if !d.startable() {
			return nil // no room left in the day, or tomorrow not whole: a later pass starts it
		}
		if _, err := tx.ExecContext(ctx, "UPDATE dwarf_flows SET archive_attempt_at=NOW_UTC() WHERE flow_id="+id); err != nil {
			return errors.Trace(err)
		}
		if at, err = attemptOf(ctx, tx, root); err != nil {
			return errors.Trace(err)
		}
		if archiver.DayOf(at, d.archive.DayLength()) != d.day {
			return errNotToday
		}
		ok = true
		return nil
	})
	if errors.Is(err, errNotToday) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, errors.Trace(err)
	}
	return at, ok, nil
}

// attemptOf reads the live root's archive attempt, as the instant it holds.
func attemptOf(ctx context.Context, tx *sequel.Tx, root int) (time.Time, error) {
	var at time.Time
	err := tx.QueryRowContext(ctx, "SELECT archive_attempt_at FROM dwarf_flows WHERE flow_id=?", root).Scan(&at)
	return database.StoredTime(at), errors.Trace(err)
}

// stageChunk copies, in one transaction under the live root's lock, up to about BatchRows rows of root's tree into the
// day of the attempt pos holds, from pos on: descendants in flow_id order, each flow's steps then its flow row and its
// directory row; then the root's steps; then, when nothing is left, it publishes. Every write inserts only what is not
// there, so a chunk run again writes the same rows. It returns the flows archived when it published, and done when the
// tree cannot be copied now: stamped, its attempt changed, held, or every row copied while the day after the attempt's
// is not whole - a root published there could not be located. pos advances only when the chunk commits.
func (p *pass) stageChunk(ctx context.Context, root int, a *archiver.Archive, pos *stagePos) (published, copied int, done bool, err error) {
	ctx, cancel := p.attemptCtx(ctx)
	defer cancel()
	db := p.shard.DB
	driver := db.DriverName()
	id := strconv.Itoa(root)
	day := archiver.DayOf(pos.at, a.DayLength())
	c := &stageCopy{
		driver: driver, at: pos.at, dayEnds: a.DayEnds(day), day: day,
		flows: migrations.FlowsTable(day), steps: migrations.StepsTable(day),
	}
	c.dirBefore, c.dirFrom = a.DirectoryHours(day)
	atExpr, atArg := database.SQLTime(driver, pos.at)
	var at stagePos
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		published, done, at = 0, false, *pos
		c.copied = 0
		locked, err := lockRoots(ctx, tx, live.flows, true, "flow_id="+id+" AND status IN ("+terminalStatusesSQL+")"+
			" AND surgraph_flow_id=0 AND delete_after_ms=0 AND archive_attempt_at="+atExpr, atArg)
		if err != nil {
			return errors.Trace(err)
		}
		if len(locked) == 0 {
			done = true
			return nil
		}
		if p.r.seams.Enabled() && p.r.seams.IsFault(join(FaultStageChunk, id)) {
			return errors.New("injected fault: " + FaultStageChunk)
		}
		// FaultArchiveErr fails every archive write, a staging chunk as a move.
		if p.r.seams.IsFault(FaultArchiveErr) {
			return errors.New("injected fault: " + FaultArchiveErr)
		}
		if at == (stagePos{at: pos.at}) {
			// A fresh position - a restart, another replica - resumes after what the day already holds of this
			// attempt: descendants are copied in flow_id order and each flow row after its steps, so every descendant
			// up to the largest there is whole, and their count numbers the next one's stamp group.
			if err := tx.QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(MAX(flow_id), 0) FROM "+c.flows+
				" WHERE root_flow_id="+id+" AND flow_id<>"+id).Scan(&at.seen, &at.after); err != nil {
				return errors.Trace(err)
			}
			children, open, err := descendantsAfter(ctx, tx, root, at.after)
			if err != nil {
				return errors.Trace(err)
			}
			if open {
				return errors.New("tree %d holds a flow that is not closed", root)
			}
			at.flow = root
			if len(children) > 0 {
				at.flow = children[0]
			}
			if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(step_id), 0) FROM "+c.steps+" WHERE flow_id="+strconv.Itoa(at.flow)).Scan(&at.stepAfter); err != nil {
				return errors.Trace(err)
			}
			// No partial child: keep the ordinary batched descendant path.
			if at.flow != root && at.stepAfter == 0 {
				at.flow = 0
			}
		}
		budget := max(p.cfg.BatchRows, 1)
		for budget > 0 && at.flow != root {
			if at.flow != 0 {
				// A descendant part copied by an earlier chunk: go on from its position.
				n, last, err := c.stepsFrom(ctx, tx, at.flow, at.stepAfter, budget)
				if err != nil {
					return errors.Trace(err)
				}
				if n == budget {
					at.stepAfter = last
					return nil
				}
				if err := c.flowRows(ctx, tx, []int{at.flow}, at.seen); err != nil {
					return errors.Trace(err)
				}
				budget -= n + 1
				at.seen, at.after, at.flow, at.stepAfter = at.seen+1, at.flow, 0, 0
				continue
			}
			children, open, err := descendantsAfter(ctx, tx, root, at.after)
			if err != nil {
				return errors.Trace(err)
			}
			if open {
				return errors.New("tree %d holds a flow that is not closed", root)
			}
			if len(children) == 0 {
				at.flow, at.stepAfter = root, 0
				break
			}
			// The longest leading run whose steps and flow rows fit what is left of the budget. The budget is charged
			// what the run reads, whether or not a row is there already.
			run, steps := len(children), 0
			for run > 0 {
				room := budget - run
				if room >= 0 {
					n, err := stepsUpTo(ctx, tx, live.steps, children[:run], room+1)
					if err != nil {
						return errors.Trace(err)
					}
					if n <= room {
						steps = n
						break
					}
				}
				run /= 2
			}
			if run == 0 {
				// The next descendant alone is over what is left: part of its steps now, the rest in the next chunk.
				_, last, err := c.stepsFrom(ctx, tx, children[0], 0, budget)
				if err != nil {
					return errors.Trace(err)
				}
				at.flow, at.stepAfter = children[0], last
				return nil
			}
			list := intCSV(children[:run])
			res, err := tx.ExecContext(ctx, "INSERT INTO "+c.steps+" SELECT * FROM dwarf_steps s WHERE s.flow_id IN ("+list+")"+
				" AND NOT EXISTS (SELECT 1 FROM "+c.steps+" a WHERE a.step_id=s.step_id)")
			if err != nil {
				return errors.Trace(err)
			}
			c.inserted(res)
			if err := c.flowRows(ctx, tx, children[:run], at.seen); err != nil {
				return errors.Trace(err)
			}
			budget -= steps + run
			at.seen, at.after = at.seen+run, children[run-1]
		}
		if budget <= 0 {
			return nil
		}
		// The root's steps, then the root itself: the publication.
		n, last, err := c.stepsFrom(ctx, tx, root, at.stepAfter, budget)
		if err != nil {
			return errors.Trace(err)
		}
		if n == budget {
			at.stepAfter = last
			return nil
		}
		if n > 0 {
			at.stepAfter = last
		}
		if !a.Whole(day + 1) {
			// A locate finds a flow only where the day after its own is whole: publish once it is. Unless the
			// attempt's own day is gone, which a statement on it proves (the archive's record may be stale) and
			// which replaces the attempt rather than waiting for a day that is never made again.
			var one int
			err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.flows+" WHERE flow_id="+id).Scan(&one)
			if err != nil {
				return errors.Trace(err)
			}
			done = true
			return nil
		}
		groups := (at.seen + stageGroup - 1) / stageGroup
		if err := c.flowRows(ctx, tx, []int{root}, groups*stageGroup); err != nil {
			return errors.Trace(err)
		}
		if _, err := tx.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1, archived_at=(SELECT archived_at FROM "+c.flows+" WHERE flow_id="+id+") WHERE flow_id="+id); err != nil {
			return errors.Trace(err)
		}
		published = at.seen + 1
		return nil
	})
	if err != nil {
		return 0, 0, false, errors.Trace(err)
	}
	*pos = at
	if p.r.seams.Enabled() && p.r.seams.IsFault(join(FaultStageLost, id)) {
		return 0, c.copied, false, errors.New("injected fault: " + FaultStageLost)
	}
	return published, c.copied, done, nil
}

// descendantsAfter returns up to treeChunk of root's descendants after the flow after, in flow_id order, and whether
// any of them is not closed.
func descendantsAfter(ctx context.Context, tx *sequel.Tx, root, after int) (children []int, open bool, err error) {
	rows, err := tx.QueryContext(ctx, "SELECT flow_id, CASE WHEN status IN ("+terminalStatusesSQL+") THEN 0 ELSE 1 END"+
		" FROM dwarf_flows WHERE root_flow_id="+strconv.Itoa(root)+" AND flow_id<>"+strconv.Itoa(root)+
		" AND flow_id>"+strconv.Itoa(after)+" ORDER BY flow_id LIMIT_OFFSET(?, 0)", treeChunk)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, notClosed int
		if err := rows.Scan(&id, &notClosed); err != nil {
			return nil, false, errors.Trace(err)
		}
		children = append(children, id)
		open = open || notClosed == 1
	}
	return children, open, errors.Trace(rows.Err())
}

// stageCopy is what a staging chunk writes into: the attempt's day, its tables, and when it ends.
type stageCopy struct {
	driver             string
	at, dayEnds        time.Time
	day                int
	flows, steps       string
	dirBefore, dirFrom int
	// copied counts the rows the chunk inserted, for dwarf_archive_staged_rows.
	copied int
}

// inserted adds the rows an insert wrote to the chunk's count.
func (c *stageCopy) inserted(res sql.Result) {
	if n, err := res.RowsAffected(); err == nil {
		c.copied += int(n)
	}
}

// stepsFrom copies up to limit steps of flow after the step after, in step_id order, and returns how many it read and
// the last one's id. The ids are read first and copied by a literal list, as a chunked deletion reads them.
func (c *stageCopy) stepsFrom(ctx context.Context, tx *sequel.Tx, flow, after, limit int) (n, last int, err error) {
	ids, err := ints(ctx, tx, "SELECT step_id FROM dwarf_steps WHERE flow_id="+strconv.Itoa(flow)+" AND step_id>"+
		strconv.Itoa(after)+" ORDER BY step_id LIMIT_OFFSET(?, 0)", limit)
	if err != nil || len(ids) == 0 {
		return 0, after, errors.Trace(err)
	}
	res, err := tx.ExecContext(ctx, "INSERT INTO "+c.steps+" SELECT * FROM dwarf_steps s WHERE s.step_id IN ("+intCSV(ids)+")"+
		" AND NOT EXISTS (SELECT 1 FROM "+c.steps+" a WHERE a.step_id=s.step_id)")
	if err != nil {
		return 0, after, errors.Trace(err)
	}
	c.inserted(res)
	return len(ids), ids[len(ids)-1], nil
}

// flowRows copies the flow rows of flows, the first of which is the tree's flow number first in flow_id order (the root
// being numbered past every descendant group), each stamped with its stamp group's archive time, at + its group in
// milliseconds; and writes the directory's rows for those a locate would not find by their creation hour. A stamp past
// the day's end is an error: nothing is clamped, since a clamp would put a large tree's flows on one archive time.
func (c *stageCopy) flowRows(ctx context.Context, tx *sequel.Tx, flows []int, first int) error {
	for i := 0; i < len(flows); {
		g := (first + i) / stageGroup
		j := min(len(flows), (g+1)*stageGroup-first) // the next group's first flow
		stamp := c.at.Add(time.Duration(g) * time.Millisecond)
		if !stamp.Before(c.dayEnds) {
			return errors.New("the tree's archive times run past the end of day %d", c.day)
		}
		expr, arg := database.SQLTime(c.driver, stamp)
		list := intCSV(flows[i:j])
		res, err := tx.ExecContext(ctx, "INSERT INTO "+c.flows+" ("+archivedFlowColumns+", archived_at)"+
			" SELECT "+archivedFlowColumns+", "+expr+" FROM dwarf_flows f WHERE f.flow_id IN ("+list+")"+
			" AND NOT EXISTS (SELECT 1 FROM "+c.flows+" a WHERE a.flow_id=f.flow_id)", arg)
		if err != nil {
			return errors.Trace(err)
		}
		c.inserted(res)
		// A replaced attempt moves a flow's directory row from the dropped day to this one.
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+archiver.DirectoryTable+" WHERE flow_id IN ("+list+")"+
			" AND day<>"+strconv.Itoa(c.day)); err != nil {
			return errors.Trace(err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+archiver.DirectoryTable+" (flow_id, day) SELECT flow_id, "+
			strconv.Itoa(c.day)+" FROM dwarf_flows f WHERE f.flow_id IN ("+list+")"+
			" AND (f.created_hour<"+strconv.Itoa(c.dirBefore)+" OR f.created_hour>="+strconv.Itoa(c.dirFrom)+")"+
			" AND NOT EXISTS (SELECT 1 FROM "+archiver.DirectoryTable+" x WHERE x.flow_id=f.flow_id)"); err != nil {
			return errors.Trace(err)
		}
		i = j
	}
	return nil
}
