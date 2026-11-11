//! Minimal slab: stable `usize` handles without per-lookup key allocation, so the
//! per-frame forwarding path never clones string ids.

pub(crate) struct Slab<T> {
    entries: Vec<Option<T>>,
    free: Vec<usize>,
    len: usize,
}

impl<T> Slab<T> {
    pub(crate) fn new() -> Self {
        Self {
            entries: Vec::new(),
            free: Vec::new(),
            len: 0,
        }
    }

    pub(crate) fn insert(&mut self, v: T) -> usize {
        self.len += 1;
        if let Some(i) = self.free.pop() {
            self.entries[i] = Some(v);
            i
        } else {
            self.entries.push(Some(v));
            self.entries.len() - 1
        }
    }

    pub(crate) fn remove(&mut self, i: usize) -> Option<T> {
        let v = self.entries.get_mut(i)?.take();
        if v.is_some() {
            self.len -= 1;
            self.free.push(i);
        }
        v
    }

    pub(crate) fn get(&self, i: usize) -> Option<&T> {
        self.entries.get(i).and_then(|e| e.as_ref())
    }

    pub(crate) fn get_mut(&mut self, i: usize) -> Option<&mut T> {
        self.entries.get_mut(i).and_then(|e| e.as_mut())
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (usize, &T)> {
        self.entries
            .iter()
            .enumerate()
            .filter_map(|(i, e)| e.as_ref().map(|v| (i, v)))
    }

    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = (usize, &mut T)> {
        self.entries
            .iter_mut()
            .enumerate()
            .filter_map(|(i, e)| e.as_mut().map(|v| (i, v)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuses_freed_slots() {
        let mut s = Slab::new();
        let a = s.insert(1);
        let b = s.insert(2);
        assert_eq!(s.len(), 2);
        assert_eq!(s.remove(a), Some(1));
        assert_eq!(s.remove(a), None);
        let c = s.insert(3);
        assert_eq!(c, a);
        assert_eq!(s.get(b), Some(&2));
        assert_eq!(s.iter().count(), 2);
    }
}
