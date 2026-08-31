//! Code-fence skipping utility for line-oriented parsers.
//!
//! Replaces lines inside fenced code blocks (triple-backtick regions)
//! with empty lines so downstream parsers don't match syntax inside
//! code examples. Line count is preserved for offset stability.

/// Minimum number of backticks to open a fenced code block.
const MIN_FENCE_BACKTICKS: usize = 3;

/// Replace lines inside fenced code blocks with empty lines.
///
/// A fence opens on a line starting with three or more backticks and
/// closes on the next line with at least as many backticks. Lines
/// between fences (inclusive of the fence delimiters) are blanked.
/// Unclosed fences blank through to the end of the input.
pub fn skip_fenced_lines(content: &str) -> String {
    let lines: Vec<&str> = content.lines().collect();
    let mut result = Vec::with_capacity(lines.len());
    let mut fence_level: Option<usize> = None;

    for line in &lines {
        let trimmed = line.trim_start();
        let backtick_count = trimmed.chars().take_while(|&c| c == '`').count();

        if let Some(open_level) = fence_level {
            // Inside a fence — check for closing delimiter.
            if backtick_count >= open_level && trimmed[backtick_count..].trim().is_empty() {
                fence_level = None;
            }
            result.push("");
        } else if backtick_count >= MIN_FENCE_BACKTICKS {
            // Opening a new fence.
            fence_level = Some(backtick_count);
            result.push("");
        } else {
            result.push(line);
        }
    }

    // Preserve trailing newline if original had one.
    let joined = result.join("\n");
    if content.ends_with('\n') {
        joined + "\n"
    } else {
        joined
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skip_fenced_code_blocks() {
        let input = "before\n```\ncode line\nmore code\n```\nafter\n";
        let output = skip_fenced_lines(input);
        assert_eq!(output, "before\n\n\n\n\nafter\n");
    }

    #[test]
    fn test_no_fences_unchanged() {
        let input = "line one\nline two\nline three";
        let output = skip_fenced_lines(input);
        assert_eq!(output, input);
    }

    #[test]
    fn test_unclosed_fence_skips_to_end() {
        let input = "before\n```\ncode\nmore code";
        let output = skip_fenced_lines(input);
        assert_eq!(output, "before\n\n\n");
    }
}
