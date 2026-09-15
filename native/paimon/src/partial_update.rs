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

//! Column selection adapted from apache/paimon-rust
//! 6824487813c69b1d4975e1add8c37343b3ead75b, table/sort_merge.rs.
//! Delete handling and singleton reduction follow released Paimon 2.0.0's
//! PartialUpdateMergeFunction and ReducerMergeFunctionWrapper.
use crate::merge::{Cursor, Result};
use arrow::array::Array;

pub(crate) struct PartialUpdate {
    pub cells: Vec<Option<usize>>,
    value_start: usize,
    ignore_delete: bool,
    remove_on_delete: bool,
}

impl PartialUpdate {
    pub fn new(
        fields: usize,
        value_start: usize,
        ignore_delete: bool,
        remove_on_delete: bool,
    ) -> Self {
        Self {
            cells: vec![None; fields - value_start],
            value_start,
            ignore_delete,
            remove_on_delete,
        }
    }

    pub fn select(&mut self, group: &[(usize, i8)], cursors: &[Option<Cursor>]) -> Result<i8> {
        self.cells.fill(None);
        if group.len() == 1 {
            // Java's reducer wrapper passes a singleton through without invoking the merger.
            self.cells.fill(Some(group[0].0));
            return Ok(group[0].1);
        }
        let mut filled = false;
        let mut saw_add = false;
        let mut deleted = false;
        for &(run, kind) in group {
            deleted = false;
            let cursor = cursors[run].as_ref().expect("partial-update cursor");
            let retract = matches!(kind, 1 | 3);
            // Java initializes from the first retract before checking ignore-delete.
            let replace_all =
                retract && (!filled || (!self.ignore_delete && self.remove_on_delete && kind == 3));
            if !retract || replace_all {
                for (i, selected) in self.cells.iter_mut().enumerate() {
                    let column = self.value_start + i;
                    let null = cursor.batch.column(column).is_null(cursor.row);
                    if null && !cursor.batch.schema_ref().field(column).is_nullable() {
                        return Err(format!("Partial-update field {i} cannot be null"));
                    }
                    if replace_all || !null {
                        *selected = Some(run);
                    }
                }
                filled = true;
            }
            if retract {
                if self.ignore_delete {
                    continue;
                }
                if !self.remove_on_delete {
                    return Err(
                        "Partial-update requires a delete policy for retract records".into(),
                    );
                }
                deleted = kind == 3;
            } else {
                saw_add = true;
            }
        }
        Ok(if deleted || !saw_add { 3 } else { 0 })
    }
}
