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

// calcConnPoolSizes sizes the connection pool of a shard given the sizing parameters. This is engine policy
// (it reads scheduling concepts - worker count, workers-per-connection), kept out of the database layer, which
// receives only the two resolved integers.
func calcConnPoolSizes(workers, shards, workersPerConn, cap int) (idle, open int) {
	if shards < 1 {
		shards = 1
	}
	if workersPerConn < 1 {
		workersPerConn = 1
	}
	if cap < 1 {
		cap = 1
	}
	denom := shards * workersPerConn
	idle = (workers + denom - 1) / denom // ceil(workers / (shards*workersPerConn))
	idle = max(idle, 2)                  // at least 2 connections per shard
	open = idle*2 + 2                    // warm core + burst headroom
	if open > cap {
		open = cap
	}
	if idle > open { // a tight ceiling can pull open below the formula idle
		idle = open
	}
	return idle, open
}

// poolSizes computes the per-shard idle/open connection sizes from the engine's live config.
func (e *Engine) poolSizes() (idle, open int) {
	return calcConnPoolSizes(int(e.workers.Load()), int(e.numShards.Load()), int(e.workersPerConn.Load()), int(e.maxOpenConns.Load()))
}
