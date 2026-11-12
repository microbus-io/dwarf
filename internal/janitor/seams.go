package janitor

// The faults and checkpoints the jobs consult, inert unless the janitor's seams are enabled. A targeted fault is the
// name joined with a root's flow id by a colon.
const (
	FaultMidTree    = "reapMidTree"    // a move fails after its steps, before its flows; optionally targeted
	FaultSizeErr    = "reapSizeErr"    // sizing a batch fails; targeted
	FaultSelectErr  = "reapSelectErr"  // a scan of due roots fails
	FaultContention = "reapContention" // an archive move fails on lock contention
	FaultArchiveErr = "reapArchiveErr" // an archive move fails with an error that is not lock contention
	FaultConnErr    = "janitorConnErr" // a batch cannot take its connection

	CheckpointBatchLocked = "reapBatchLocked"  // a batch has locked its due roots, inside its transaction
	CheckpointBatchConn   = "janitorBatchConn" // a batch has taken its connection, before its first statement
)
