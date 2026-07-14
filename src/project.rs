use std::path::{Path, PathBuf};

use serde_json::Value;

const WORKING_DIRECTORY_PREFIXES: [&str; 3] = [
    "Primary working directory:",
    "Working directory:",
    "Current working directory:",
];

pub fn name_from_system(system: Option<&Value>) -> Option<String> {
    system_text(system)
        .lines()
        .find_map(working_directory_from_line)
        .and_then(name_from_working_directory)
}

fn system_text(system: Option<&Value>) -> String {
    match system {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(blocks)) => blocks
            .iter()
            .filter_map(|block| block.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn working_directory_from_line(line: &str) -> Option<&str> {
    let line = line.trim().strip_prefix("- ").unwrap_or(line.trim());
    WORKING_DIRECTORY_PREFIXES.iter().find_map(|prefix| {
        line.strip_prefix(prefix)
            .map(str::trim)
            .filter(|path| !path.is_empty())
    })
}

fn name_from_working_directory(path: &str) -> Option<String> {
    let working_directory = Path::new(path);
    let repository_root = working_directory
        .ancestors()
        .find(|ancestor| ancestor.join(".git").exists());

    repository_root
        .and_then(repository_name)
        .or_else(|| path_name(working_directory))
}

fn repository_name(root: &Path) -> Option<String> {
    let git_marker = root.join(".git");
    if git_marker.is_dir() {
        return path_name(root);
    }

    let contents = std::fs::read_to_string(git_marker).ok()?;
    let git_dir = contents.trim().strip_prefix("gitdir:")?.trim();
    let git_dir = if Path::new(git_dir).is_absolute() {
        PathBuf::from(git_dir)
    } else {
        root.join(git_dir)
    };
    git_dir
        .ancestors()
        .find(|ancestor| ancestor.file_name().is_some_and(|name| name == ".git"))
        .and_then(Path::parent)
        .and_then(path_name)
        .or_else(|| path_name(root))
}

fn path_name(path: &Path) -> Option<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn reads_primary_working_directory_from_system_blocks() {
        let root = tempdir().unwrap();
        fs::create_dir(root.path().join(".git")).unwrap();
        let system = json!([
            {"type": "text", "text": "instructions"},
            {"type": "text", "text": format!(
                "# Environment\n - Primary working directory: {}\n - Platform: linux",
                root.path().display()
            )}
        ]);

        assert_eq!(
            name_from_system(Some(&system)).as_deref(),
            root.path().file_name().and_then(|name| name.to_str())
        );
    }

    #[test]
    fn reads_legacy_working_directory_from_string_system_prompt() {
        let system = json!("<env>\nWorking directory: /home/user/example\n</env>");

        assert_eq!(name_from_system(Some(&system)).as_deref(), Some("example"));
    }

    #[test]
    fn resolves_linked_worktree_to_main_repository_name() {
        let temp = tempdir().unwrap();
        let main = temp.path().join("project");
        let worktree = temp.path().join("worktrees").join("feature");
        let git_dir = main.join(".git").join("worktrees").join("feature");
        fs::create_dir_all(&git_dir).unwrap();
        fs::create_dir_all(&worktree).unwrap();
        fs::write(
            worktree.join(".git"),
            format!("gitdir: {}\n", git_dir.display()),
        )
        .unwrap();

        assert_eq!(
            name_from_working_directory(worktree.to_str().unwrap()).as_deref(),
            Some("project")
        );
    }

    #[test]
    fn returns_none_without_working_directory_metadata() {
        assert_eq!(name_from_system(Some(&json!("instructions"))), None);
        assert_eq!(name_from_system(None), None);
    }
}
