// Based on Apache Spark 4.0.1's port of Android TimSort.
// Copyright (C) 2008 The Android Open Source Project
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
// See the License for the specific language governing permissions and limitations.

//! Sort indices with Java's run/gallop schedule, including invalid-comparator detection.
use std::cmp::Ordering;

pub(super) fn sort(
    values: &mut [usize],
    compare: impl Fn(usize, usize) -> Ordering,
) -> Result<(), ()> {
    if values.len() < 2 {
        return Ok(());
    }
    if values.len() < 32 {
        let run = count_run(values, 0, &compare);
        binary_sort(values, 0, values.len(), run, &compare);
        return Ok(());
    }
    let mut n = values.len();
    let mut remainder = 0;
    while n >= 32 {
        remainder |= n & 1;
        n >>= 1;
    }
    let minimum = n + remainder;
    let length = values.len();
    let mut state = TimSort {
        values,
        compare: &compare,
        runs: Vec::new(),
        temporary: Vec::new(),
        min_gallop: 7,
    };
    let mut first = 0;
    while first < length {
        let mut run = count_run(state.values, first, &compare);
        if run < minimum {
            let force = minimum.min(length - first);
            binary_sort(state.values, first, first + force, first + run, &compare);
            run = force;
        }
        state.runs.push((first, run));
        while state.runs.len() > 1 {
            let mut index = state.runs.len() - 2;
            let len = |i: usize| state.runs[i].1;
            if index > 0 && len(index - 1) <= len(index) + len(index + 1)
                || index > 1 && len(index - 2) <= len(index) + len(index - 1)
            {
                if len(index - 1) < len(index + 1) {
                    index -= 1;
                }
            } else if len(index) > len(index + 1) {
                break;
            }
            state.merge_at(index)?;
        }
        first += run;
    }
    while state.runs.len() > 1 {
        let mut index = state.runs.len() - 2;
        if index > 0 && state.runs[index - 1].1 < state.runs[index + 1].1 {
            index -= 1;
        }
        state.merge_at(index)?;
    }
    Ok(())
}

fn count_run(
    values: &mut [usize],
    first: usize,
    compare: &impl Fn(usize, usize) -> Ordering,
) -> usize {
    let mut end = first + 1;
    if end == values.len() {
        return 1;
    }
    let descending = compare(values[end], values[first]).is_lt();
    end += 1;
    while end < values.len() && compare(values[end], values[end - 1]).is_lt() == descending {
        end += 1;
    }
    if descending {
        values[first..end].reverse();
    }
    end - first
}

fn binary_sort(
    values: &mut [usize],
    first: usize,
    end: usize,
    start: usize,
    compare: &impl Fn(usize, usize) -> Ordering,
) {
    for index in start.max(first + 1)..end {
        let pivot = values[index];
        let (mut left, mut right) = (first, index);
        while left < right {
            let middle = left + (right - left) / 2;
            if compare(pivot, values[middle]).is_lt() {
                right = middle;
            } else {
                left = middle + 1;
            }
        }
        values.copy_within(left..index, left + 1);
        values[left] = pivot;
    }
}

fn gallop(
    key: usize,
    values: &[usize],
    base: usize,
    length: usize,
    hint: usize,
    right: bool,
    compare: &impl Fn(usize, usize) -> Ordering,
) -> usize {
    let (base, length, hint) = (base as isize, length as isize, hint as isize);
    let before = |position: isize| {
        let order = compare(key, values[(base + position) as usize]);
        if right {
            order.is_lt()
        } else {
            !order.is_gt()
        }
    };
    let (mut last, mut offset) = (0_isize, 1_isize);
    if before(hint) {
        let maximum = hint + 1;
        while offset < maximum && before(hint - offset) {
            last = offset;
            offset = (offset * 2 + 1).min(maximum);
        }
        offset = offset.min(maximum);
        let previous = last;
        last = hint - offset;
        offset = hint - previous;
    } else {
        let maximum = length - hint;
        while offset < maximum && !before(hint + offset) {
            last = offset;
            offset = (offset * 2 + 1).min(maximum);
        }
        offset = offset.min(maximum);
        last += hint;
        offset += hint;
    }
    last += 1;
    while last < offset {
        let middle = last + (offset - last) / 2;
        if before(middle) {
            offset = middle;
        } else {
            last = middle + 1;
        }
    }
    offset as usize
}

struct TimSort<'a, F> {
    values: &'a mut [usize],
    compare: &'a F,
    runs: Vec<(usize, usize)>,
    temporary: Vec<usize>,
    min_gallop: isize,
}

impl<F: Fn(usize, usize) -> Ordering> TimSort<'_, F> {
    fn merge_at(&mut self, index: usize) -> Result<(), ()> {
        let (mut left, mut nl) = self.runs[index];
        let (right, mut nr) = self.runs[index + 1];
        self.runs[index].1 += nr;
        self.runs.remove(index + 1);
        let skip = gallop(
            self.values[right],
            self.values,
            left,
            nl,
            0,
            true,
            self.compare,
        );
        left += skip;
        nl -= skip;
        if nl == 0 {
            return Ok(());
        }
        nr = gallop(
            self.values[left + nl - 1],
            self.values,
            right,
            nr,
            nr - 1,
            false,
            self.compare,
        );
        if nr == 0 {
            return Ok(());
        }
        if nl <= nr {
            self.merge_low(left, nl, right, nr)
        } else {
            self.merge_high(left, nl, right, nr)
        }
    }

    fn merge_low(
        &mut self,
        left: usize,
        mut nl: usize,
        right: usize,
        mut nr: usize,
    ) -> Result<(), ()> {
        self.temporary.clear();
        self.temporary
            .extend_from_slice(&self.values[left..left + nl]);
        let (mut c1, mut c2, mut dest) = (0, right, left);
        self.values[dest] = self.values[c2];
        dest += 1;
        c2 += 1;
        nr -= 1;
        if nr == 0 {
            self.values[dest..dest + nl].copy_from_slice(&self.temporary[c1..c1 + nl]);
            return Ok(());
        }
        if nl == 1 {
            self.values.copy_within(c2..c2 + nr, dest);
            self.values[dest + nr] = self.temporary[c1];
            return Ok(());
        }
        let mut minimum = self.min_gallop;
        'outer: loop {
            let (mut n1, mut n2) = (0, 0);
            loop {
                if (self.compare)(self.values[c2], self.temporary[c1]).is_lt() {
                    self.values[dest] = self.values[c2];
                    dest += 1;
                    c2 += 1;
                    n2 += 1;
                    n1 = 0;
                    nr -= 1;
                    if nr == 0 {
                        break 'outer;
                    }
                } else {
                    self.values[dest] = self.temporary[c1];
                    dest += 1;
                    c1 += 1;
                    n1 += 1;
                    n2 = 0;
                    nl -= 1;
                    if nl == 1 {
                        break 'outer;
                    }
                }
                if (n1 | n2) as isize >= minimum {
                    break;
                }
            }
            loop {
                n1 = gallop(
                    self.values[c2],
                    &self.temporary,
                    c1,
                    nl,
                    0,
                    true,
                    self.compare,
                );
                if n1 != 0 {
                    self.values[dest..dest + n1].copy_from_slice(&self.temporary[c1..c1 + n1]);
                    dest += n1;
                    c1 += n1;
                    nl -= n1;
                    if nl <= 1 {
                        break 'outer;
                    }
                }
                self.values[dest] = self.values[c2];
                dest += 1;
                c2 += 1;
                nr -= 1;
                if nr == 0 {
                    break 'outer;
                }
                n2 = gallop(
                    self.temporary[c1],
                    self.values,
                    c2,
                    nr,
                    0,
                    false,
                    self.compare,
                );
                if n2 != 0 {
                    self.values.copy_within(c2..c2 + n2, dest);
                    dest += n2;
                    c2 += n2;
                    nr -= n2;
                    if nr == 0 {
                        break 'outer;
                    }
                }
                self.values[dest] = self.temporary[c1];
                dest += 1;
                c1 += 1;
                nl -= 1;
                if nl == 1 {
                    break 'outer;
                }
                minimum -= 1;
                if n1 < 7 && n2 < 7 {
                    break;
                }
            }
            minimum = minimum.max(0) + 2;
        }
        self.min_gallop = minimum.max(1);
        if nl == 1 {
            self.values.copy_within(c2..c2 + nr, dest);
            self.values[dest + nr] = self.temporary[c1];
        } else if nl == 0 {
            return Err(());
        } else {
            self.values[dest..dest + nl].copy_from_slice(&self.temporary[c1..c1 + nl]);
        }
        Ok(())
    }

    fn merge_high(
        &mut self,
        left: usize,
        mut nl: usize,
        right: usize,
        mut nr: usize,
    ) -> Result<(), ()> {
        self.temporary.clear();
        self.temporary
            .extend_from_slice(&self.values[right..right + nr]);
        let (mut c1, mut c2, mut dest) = (
            (left + nl - 1) as isize,
            (nr - 1) as isize,
            (right + nr - 1) as isize,
        );
        self.values[dest as usize] = self.values[c1 as usize];
        dest -= 1;
        c1 -= 1;
        nl -= 1;
        if nl == 0 {
            let start = (dest - nr as isize + 1) as usize;
            self.values[start..start + nr].copy_from_slice(&self.temporary[..nr]);
            return Ok(());
        }
        if nr == 1 {
            dest -= nl as isize;
            c1 -= nl as isize;
            self.values.copy_within(
                (c1 + 1) as usize..(c1 + 1) as usize + nl,
                (dest + 1) as usize,
            );
            self.values[dest as usize] = self.temporary[c2 as usize];
            return Ok(());
        }
        let mut minimum = self.min_gallop;
        'outer: loop {
            let (mut n1, mut n2) = (0, 0);
            loop {
                if (self.compare)(self.temporary[c2 as usize], self.values[c1 as usize]).is_lt() {
                    self.values[dest as usize] = self.values[c1 as usize];
                    dest -= 1;
                    c1 -= 1;
                    n1 += 1;
                    n2 = 0;
                    nl -= 1;
                    if nl == 0 {
                        break 'outer;
                    }
                } else {
                    self.values[dest as usize] = self.temporary[c2 as usize];
                    dest -= 1;
                    c2 -= 1;
                    n2 += 1;
                    n1 = 0;
                    nr -= 1;
                    if nr == 1 {
                        break 'outer;
                    }
                }
                if (n1 | n2) as isize >= minimum {
                    break;
                }
            }
            loop {
                n1 = nl
                    - gallop(
                        self.temporary[c2 as usize],
                        self.values,
                        left,
                        nl,
                        nl - 1,
                        true,
                        self.compare,
                    );
                if n1 != 0 {
                    dest -= n1 as isize;
                    c1 -= n1 as isize;
                    nl -= n1;
                    self.values.copy_within(
                        (c1 + 1) as usize..(c1 + 1) as usize + n1,
                        (dest + 1) as usize,
                    );
                    if nl == 0 {
                        break 'outer;
                    }
                }
                self.values[dest as usize] = self.temporary[c2 as usize];
                dest -= 1;
                c2 -= 1;
                nr -= 1;
                if nr == 1 {
                    break 'outer;
                }
                n2 = nr
                    - gallop(
                        self.values[c1 as usize],
                        &self.temporary,
                        0,
                        nr,
                        nr - 1,
                        false,
                        self.compare,
                    );
                if n2 != 0 {
                    dest -= n2 as isize;
                    c2 -= n2 as isize;
                    nr -= n2;
                    self.values[(dest + 1) as usize..(dest + 1) as usize + n2].copy_from_slice(
                        &self.temporary[(c2 + 1) as usize..(c2 + 1) as usize + n2],
                    );
                    if nr <= 1 {
                        break 'outer;
                    }
                }
                self.values[dest as usize] = self.values[c1 as usize];
                dest -= 1;
                c1 -= 1;
                nl -= 1;
                if nl == 0 {
                    break 'outer;
                }
                minimum -= 1;
                if n1 < 7 && n2 < 7 {
                    break;
                }
            }
            minimum = minimum.max(0) + 2;
        }
        self.min_gallop = minimum.max(1);
        if nr == 1 {
            dest -= nl as isize;
            c1 -= nl as isize;
            self.values.copy_within(
                (c1 + 1) as usize..(c1 + 1) as usize + nl,
                (dest + 1) as usize,
            );
            self.values[dest as usize] = self.temporary[c2 as usize];
        } else if nr == 0 {
            return Err(());
        } else {
            let start = (dest - nr as isize + 1) as usize;
            self.values[start..start + nr].copy_from_slice(&self.temporary[..nr]);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Case {
        keys: Vec<Option<i32>>,
        ascending: bool,
        #[serde(rename = "nullFirst")]
        null_first: bool,
        cycle: bool,
        output: Option<Vec<usize>>,
    }

    #[test]
    fn matches_jdk17_order_and_comparator_failures_for_480_independent_cases() {
        let json = zstd::stream::decode_all(
            &include_bytes!("../../tests/fixtures/java-sort-oracle.json.zst")[..],
        )
        .unwrap();
        let cases: Vec<Case> = serde_json::from_slice(&json).unwrap();
        assert_eq!(cases.len(), 480);
        let mut errors = 0;
        for (number, case) in cases.iter().enumerate() {
            let mut indices = (0..case.keys.len()).collect::<Vec<_>>();
            let compare = |left: usize, right: usize| match (case.keys[left], case.keys[right]) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => {
                    if case.null_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (Some(_), None) => {
                    if case.null_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                (Some(a), Some(b)) => {
                    let greater = if case.cycle {
                        (a - b).rem_euclid(3) == 1
                    } else {
                        a > b
                    };
                    if greater == case.ascending {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
            };
            let result = sort(&mut indices, compare);
            match &case.output {
                Some(expected) => {
                    assert_eq!(result, Ok(()), "case {number}");
                    assert_eq!(&indices, expected, "case {number}");
                }
                None => {
                    assert_eq!(result, Err(()), "case {number}");
                    errors += 1;
                }
            }
        }
        assert_eq!(errors, 107);
    }
}
