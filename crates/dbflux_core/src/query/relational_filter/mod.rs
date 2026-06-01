pub mod count;
pub mod parser;
pub mod resolver;

/// Placeholder entry point — replaced by the full implementation in T19.
#[allow(dead_code)]
pub fn parse_and_resolve(_input: &str) {}

#[cfg(test)]
mod tests {
    #[test]
    fn module_exists() {
        super::parse_and_resolve("");
    }
}
