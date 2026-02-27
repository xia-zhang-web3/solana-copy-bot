pub(crate) fn levenshtein_distance(left: &str, right: &str) -> usize {
    if left == right {
        return 0;
    }
    let right_len = right.chars().count();
    if right_len == 0 {
        return left.chars().count();
    }
    let left_len = left.chars().count();
    if left_len == 0 {
        return right_len;
    }
    let mut prev: Vec<usize> = (0..=right_len).collect();
    let mut curr = vec![0usize; right_len + 1];
    for (i, left_char) in left.chars().enumerate() {
        curr[0] = i + 1;
        for (j, right_char) in right.chars().enumerate() {
            let cost = usize::from(left_char != right_char);
            curr[j + 1] = (prev[j + 1] + 1).min(curr[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[right_len]
}

pub(crate) fn closest_match<'a>(candidate: &str, options: &'a [&str]) -> Option<(&'a str, usize)> {
    let mut best: Option<(&str, usize)> = None;
    for option in options {
        let distance = levenshtein_distance(candidate, option);
        match best {
            Some((_, best_distance)) if best_distance <= distance => {}
            _ => best = Some((option, distance)),
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::{closest_match, levenshtein_distance};

    #[test]
    fn levenshtein_distance_matches_known_cases() {
        assert_eq!(levenshtein_distance("fastlane", "fastlane"), 0);
        assert_eq!(levenshtein_distance("faslane", "fastlane"), 1);
        assert_eq!(levenshtein_distance("rpc", "jito"), 4);
        assert_eq!(levenshtein_distance("", "abc"), 3);
    }

    #[test]
    fn closest_match_returns_lowest_distance_candidate() {
        let candidates = ["fastlane", "rpc", "jito"];
        let (value, distance) = closest_match("faslane", &candidates).expect("must match");
        assert_eq!(value, "fastlane");
        assert_eq!(distance, 1);
    }
}
