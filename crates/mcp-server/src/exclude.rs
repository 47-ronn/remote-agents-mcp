//! Exclude-pattern matching for folder sync (`SyncDirTo` / `DirManifest`).
//!
//! gitignore/rsync-flavored and dependency-free. Patterns are matched against a
//! path **relative to the sync root**, with `/` separators. The same patterns
//! are applied on both ends of a sync so an excluded file is neither transferred
//! nor (with `delete`) removed from the destination.
//!
//! Rules:
//! - A bare pattern with no `/` (e.g. `*.log`, `node_modules`, `.git`) matches
//!   that name as **any path segment**, so it excludes the file at any depth and
//!   prunes whole directories of that name.
//! - A pattern containing a `/` is matched against the full relative path; a
//!   leading `/` anchors it to the root (`/build` excludes only the top-level
//!   `build`, not `src/build`).
//! - A trailing `/` (e.g. `target/`) is a directory hint; it's stripped and the
//!   pattern still prunes everything under that directory via the segment rule.
//! - Globs: `?` = one non-`/` char, `*` = any run of non-`/` chars, `**` = any
//!   run including `/`.

/// True if `rel_path` (forward-slash separated, relative to the sync root)
/// matches any of the exclude `patterns`. An empty pattern list excludes nothing.
pub fn is_excluded(rel_path: &str, patterns: &[String]) -> bool {
    patterns.iter().any(|p| matches_pattern(rel_path, p))
}

/// True if any path component of `rel_path` is itself an excluded **directory**.
/// Used by the walk to prune a subtree before descending: a directory `dir`
/// (relative path) is pruned when a pattern matches `dir` as a whole or any of
/// its segments. This is the directory-prefix counterpart to [`is_excluded`],
/// which classifies leaf files.
pub fn dir_excluded(rel_dir: &str, patterns: &[String]) -> bool {
    !rel_dir.is_empty() && patterns.iter().any(|p| matches_pattern(rel_dir, p))
}

fn matches_pattern(rel: &str, pattern: &str) -> bool {
    let pat = pattern.trim();
    if pat.is_empty() {
        return false;
    }
    // Trailing slash: directory hint. Strip it — the segment rule below still
    // prunes everything under a directory of that name.
    let pat = pat.strip_suffix('/').unwrap_or(pat);
    if pat.is_empty() {
        return false;
    }
    // Leading slash anchors to the root: match the whole relative path only.
    if let Some(anchored) = pat.strip_prefix('/') {
        return path_or_subtree(anchored, rel);
    }
    if pat.contains('/') {
        // Path-shaped pattern: match the full relative path, or anything under
        // it when it names a directory (`build/out` also excludes `build/out/x`).
        path_or_subtree(pat, rel)
    } else {
        // Bare name: match against any single segment so it applies at any depth.
        rel.split('/').any(|seg| glob_match(pat, seg, false))
    }
}

/// True if `pat` matches `rel` exactly, or matches a directory prefix of it
/// (so a pattern naming a directory also excludes everything under it).
fn path_or_subtree(pat: &str, rel: &str) -> bool {
    glob_match(pat, rel, true) || glob_match(&format!("{pat}/**"), rel, true)
}

/// Backtracking glob matcher. `*` matches a run of non-`/` chars, `**` matches a
/// run including `/`, `?` matches one char (never `/` when `slash_sensitive`).
fn glob_match(pat: &str, text: &str, slash_sensitive: bool) -> bool {
    let p: Vec<char> = pat.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut i, mut j) = (0usize, 0usize);
    // Backtrack anchor for the most recent `*` / `**`.
    let mut star_j: Option<usize> = None; // pattern index to resume at, past the star
    let mut star_i = 0usize; // next text index the star may absorb
    let mut star_double = false; // `**` crosses `/`, `*` does not

    while i < t.len() {
        if j < p.len() && p[j] == '*' {
            let double = j + 1 < p.len() && p[j + 1] == '*';
            j += if double { 2 } else { 1 };
            star_j = Some(j);
            star_i = i;
            star_double = double;
            continue;
        }
        let lit = j < p.len()
            && (p[j] == t[i] || (p[j] == '?' && !(slash_sensitive && t[i] == '/')));
        if lit {
            i += 1;
            j += 1;
            continue;
        }
        match star_j {
            Some(sj) => {
                // Extend the star to absorb t[star_i]; a single `*` can't cross `/`.
                if !star_double && slash_sensitive && t[star_i] == '/' {
                    return false;
                }
                star_i += 1;
                i = star_i;
                j = sj;
            }
            None => return false,
        }
    }
    // Trailing stars in the pattern match the empty remainder.
    while j < p.len() && p[j] == '*' {
        j += 1;
    }
    j == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ex(path: &str, pats: &[&str]) -> bool {
        let v: Vec<String> = pats.iter().map(|s| s.to_string()).collect();
        is_excluded(path, &v)
    }

    #[test]
    fn empty_patterns_match_nothing() {
        assert!(!ex("a/b.txt", &[]));
    }

    #[test]
    fn bare_name_matches_any_segment() {
        assert!(ex("node_modules/lib/x.js", &["node_modules"]));
        assert!(ex("src/node_modules/x.js", &["node_modules"]));
        assert!(ex(".git/config", &[".git"]));
        assert!(!ex("src/app.js", &["node_modules"]));
    }

    #[test]
    fn glob_extension_at_any_depth() {
        assert!(ex("a/b/c.log", &["*.log"]));
        assert!(ex("debug.log", &["*.log"]));
        assert!(!ex("a/b/c.txt", &["*.log"]));
        // `*` does not cross a slash within a segment match (no slash in a segment).
        assert!(ex("x.test.js", &["*.test.js"]));
    }

    #[test]
    fn question_mark_one_char() {
        assert!(ex("a/f1.tmp", &["f?.tmp"]));
        assert!(!ex("a/f12.tmp", &["f?.tmp"]));
    }

    #[test]
    fn path_shaped_pattern_matches_full_path() {
        assert!(ex("build/out/x", &["build/out/x"]));
        assert!(!ex("build/out/x", &["out/x"])); // not anchored substring
        assert!(ex("src/a.test.js", &["src/*.test.js"]));
        assert!(!ex("src/sub/a.test.js", &["src/*.test.js"])); // `*` won't cross `/`
    }

    #[test]
    fn double_star_crosses_slashes() {
        assert!(ex("src/sub/a.test.js", &["src/**/*.test.js"]));
        assert!(ex("src/a.test.js", &["src/**"]));
        assert!(ex("a/b/c/d", &["**/d"]));
    }

    #[test]
    fn leading_slash_anchors_to_root() {
        assert!(ex("build/x", &["/build"]));
        assert!(!ex("src/build/x", &["/build"])); // anchored: only top-level
        assert!(ex("src/build/x", &["build"])); // bare name still matches at depth
    }

    #[test]
    fn trailing_slash_is_a_directory_hint() {
        assert!(ex("target/debug/app", &["target/"]));
        assert!(ex("a/target/x", &["target/"]));
    }

    #[test]
    fn dir_excluded_prunes_subtrees() {
        let pats: Vec<String> = ["node_modules", "/build"].iter().map(|s| s.to_string()).collect();
        assert!(dir_excluded("node_modules", &pats));
        assert!(dir_excluded("src/node_modules", &pats));
        assert!(dir_excluded("build", &pats));
        assert!(!dir_excluded("src/build", &pats)); // anchored pattern, deeper dir
        assert!(!dir_excluded("", &pats)); // the root is never pruned
    }

    #[test]
    fn multiple_patterns_any_match() {
        let pats = ["*.log", "node_modules", ".git"];
        assert!(ex("a/x.log", &pats));
        assert!(ex("node_modules/x", &pats));
        assert!(!ex("src/main.rs", &pats));
    }
}
