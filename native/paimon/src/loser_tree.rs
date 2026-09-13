// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Adapted from apache/paimon-rust 6824487813c69b1d4975e1add8c37343b3ead75b,
// table/sort_merge.rs. Only module visibility changes.
/// A LoserTree (tournament tree) for k-way merge.
///
/// Layout follows DataFusion's `SortPreservingMergeStream`:
/// - `nodes[0]` = overall winner index
/// - `nodes[1..k]` = loser at each internal node
///
/// Reference: <https://en.wikipedia.org/wiki/K-way_merge_algorithm#Tournament_Tree>
pub(crate) struct LoserTree {
    /// nodes[0] = winner, nodes[1..] = losers
    nodes: Vec<usize>,
    num_streams: usize,
}

impl LoserTree {
    pub(crate) fn new(num_streams: usize) -> Self {
        Self {
            nodes: vec![usize::MAX; num_streams],
            num_streams,
        }
    }

    pub(crate) fn winner(&self) -> usize {
        self.nodes[0]
    }

    /// Leaf node index for a given stream index.
    pub(crate) fn leaf_index(&self, stream_idx: usize) -> usize {
        (self.num_streams + stream_idx) / 2
    }

    pub(crate) fn parent_index(node_idx: usize) -> usize {
        node_idx / 2
    }

    /// Build the tree from scratch given a comparison function.
    /// `is_gt(a, b)` returns true if stream `a` > stream `b`.
    pub(crate) fn init(&mut self, is_gt: impl Fn(usize, usize) -> bool) {
        self.nodes.fill(usize::MAX);
        for i in 0..self.num_streams {
            let mut winner = i;
            let mut cmp_node = self.leaf_index(i);
            while cmp_node != 0 && self.nodes[cmp_node] != usize::MAX {
                let challenger = self.nodes[cmp_node];
                if is_gt(winner, challenger) {
                    self.nodes[cmp_node] = winner;
                    winner = challenger;
                }
                cmp_node = Self::parent_index(cmp_node);
            }
            self.nodes[cmp_node] = winner;
        }
    }

    /// Update the tree after the winner has been consumed/advanced.
    pub(crate) fn update(&mut self, is_gt: impl Fn(usize, usize) -> bool) {
        let mut winner = self.nodes[0];
        let mut cmp_node = self.leaf_index(winner);
        while cmp_node != 0 {
            let challenger = self.nodes[cmp_node];
            if is_gt(winner, challenger) {
                self.nodes[cmp_node] = winner;
                winner = challenger;
            }
            cmp_node = Self::parent_index(cmp_node);
        }
        self.nodes[0] = winner;
    }
}
