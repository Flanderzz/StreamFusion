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

// Ported from Apache Paimon 2.0.0 mergetree/compact/LoserTree.java.
// Arrow cursors own the records; this tree keeps Java's leaf states and tie traversal.
use std::cmp::Ordering;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    LoserNew,
    LoserSame,
    LoserPopped,
    WinnerNew,
    WinnerSame,
    WinnerPopped,
}
impl State {
    fn winner(self) -> bool {
        matches!(
            self,
            Self::WinnerNew | Self::WinnerSame | Self::WinnerPopped
        )
    }
}

struct Leaf {
    state: State,
    first_same: usize,
}
impl Leaf {
    fn first_same(&mut self, index: usize) {
        if self.first_same == usize::MAX {
            self.first_same = index;
        }
    }
}

pub(crate) struct LoserTree {
    nodes: Vec<usize>,
    leaves: Vec<Leaf>,
}
impl LoserTree {
    pub fn new(runs: usize) -> Self {
        Self {
            nodes: vec![usize::MAX; runs],
            leaves: (0..runs)
                .map(|_| Leaf {
                    state: State::WinnerNew,
                    first_same: usize::MAX,
                })
                .collect(),
        }
    }

    pub fn winner(&self) -> usize {
        self.nodes[0]
    }
    pub fn popped(&self) -> bool {
        self.leaves[self.winner()].state == State::WinnerPopped
    }

    pub fn advance(
        &mut self,
        run: usize,
        key: impl Fn(usize, usize) -> Ordering,
        sequence: impl Fn(usize, usize) -> Ordering,
    ) {
        self.leaves[run] = Leaf {
            state: State::WinnerNew,
            first_same: usize::MAX,
        };
        self.adjust(run, key, sequence);
    }

    pub fn pop(
        &mut self,
        key: impl Fn(usize, usize) -> Ordering,
        sequence: impl Fn(usize, usize) -> Ordering,
    ) {
        let winner = self.winner();
        assert!(!self.popped());
        self.leaves[winner].state = State::WinnerPopped;
        self.adjust(winner, key, sequence);
    }

    fn adjust(
        &mut self,
        mut winner: usize,
        key: impl Fn(usize, usize) -> Ordering,
        sequence: impl Fn(usize, usize) -> Ordering,
    ) {
        let mut index = (winner + self.leaves.len()) / 2;
        while index > 0 && winner != usize::MAX {
            let parent = self.nodes[index];
            if parent == usize::MAX {
                self.leaves[winner].state = State::LoserNew;
            } else {
                match self.leaves[winner].state {
                    State::WinnerNew => match self.leaves[parent].state {
                        State::LoserNew => match key(parent, winner) {
                            Ordering::Equal => {
                                if sequence(parent, winner).is_gt() {
                                    self.leaves[parent].state = State::LoserSame;
                                    self.leaves[winner].first_same(index);
                                } else {
                                    self.leaves[winner].state = State::LoserSame;
                                    self.leaves[parent].state = State::WinnerNew;
                                    self.leaves[parent].first_same(index);
                                }
                            }
                            Ordering::Less => {
                                self.leaves[parent].state = State::WinnerNew;
                                self.leaves[winner].state = State::LoserNew;
                            }
                            Ordering::Greater => {}
                        },
                        State::LoserPopped => {
                            self.leaves[parent].state = State::WinnerPopped;
                            self.leaves[parent].first_same = usize::MAX;
                            self.leaves[winner].state = State::LoserNew;
                        }
                        state => panic!("Unexpected parent state {state:?} for new winner"),
                    },
                    State::WinnerSame => match self.leaves[parent].state {
                        State::LoserSame => {
                            if sequence(parent, winner).is_lt() {
                                self.leaves[parent].state = State::WinnerSame;
                                self.leaves[winner].state = State::LoserSame;
                                self.leaves[parent].first_same(index);
                            } else {
                                self.leaves[winner].first_same(index);
                            }
                        }
                        State::LoserNew | State::LoserPopped => {}
                        state => panic!("Unexpected parent state {state:?} for same-key winner"),
                    },
                    State::WinnerPopped => {
                        if self.leaves[winner].first_same == usize::MAX {
                            break;
                        }
                        index = self.leaves[winner].first_same;
                        self.leaves[winner].state = State::LoserPopped;
                        self.leaves[self.nodes[index]].state = State::WinnerSame;
                    }
                    state => panic!("Unexpected winner state {state:?}"),
                }
            }
            if !self.leaves[winner].state.winner() {
                std::mem::swap(&mut winner, &mut self.nodes[index]);
            }
            index /= 2;
        }
        self.nodes[0] = winner;
    }
}
