pub mod count;
pub mod parser;
pub mod resolver;

/// Placeholder entry point. Replaced by the full implementation in T19.
pub fn parse_and_resolve(_input: &str) {}

#[cfg(test)]
mod tests {
    #[test]
    fn module_exists() {
        super::parse_and_resolve("");
    }
}
