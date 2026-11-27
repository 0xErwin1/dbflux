//! Plain-language glossary of SHOWPLAN physical operators.

const GLOSSARY: &[(&str, &str)] = &[
    (
        "Clustered Index Seek",
        "Finds the matching rows through the clustered index instead of reading the whole table.",
    ),
    (
        "Clustered Index Scan",
        "Reads every row of the table in clustered index order.",
    ),
    (
        "Clustered Index Update",
        "Changes the rows its input returns in the clustered index.",
    ),
    (
        "Clustered Index Insert",
        "Adds the rows its input returns to the clustered index.",
    ),
    (
        "Clustered Index Delete",
        "Removes the rows its input returns from the clustered index.",
    ),
    (
        "Index Seek",
        "Finds the matching rows through an index instead of reading the whole table.",
    ),
    ("Index Scan", "Reads every entry of an index."),
    (
        "Table Scan",
        "Reads every row of the table, which has no clustered index.",
    ),
    (
        "Table Insert",
        "Adds the rows its input returns to the table.",
    ),
    ("Table Update", "Changes the rows its input returns."),
    (
        "Table Delete",
        "Removes the rows its input returns from the table.",
    ),
    (
        "Key Lookup",
        "Fetches the remaining columns of a row from the clustered index, using the key an index seek found.",
    ),
    (
        "RID Lookup",
        "Fetches the remaining columns of a row from the table, using the row identifier an index seek found.",
    ),
    (
        "Nested Loops",
        "For each row from the first input, looks up the matching rows in the second.",
    ),
    (
        "Hash Match",
        "Builds a hash table from one input and probes it with each row of the other to find matches or groups.",
    ),
    (
        "Merge Join",
        "Walks two sorted inputs together and pairs the rows that match.",
    ),
    (
        "Adaptive Join",
        "Chooses between a hash join and a nested loops join while the query runs.",
    ),
    ("Sort", "Orders the rows by the sort key."),
    (
        "Stream Aggregate",
        "Computes the aggregates of each group from rows that arrive already grouped.",
    ),
    ("Top", "Stops after the number of rows the query asks for."),
    (
        "Compute Scalar",
        "Computes a value for each row, such as an expression.",
    ),
    ("Filter", "Keeps only the rows that satisfy the predicate."),
    (
        "Concatenation",
        "Appends the rows of several inputs one after another.",
    ),
    (
        "Table Spool",
        "Stores the rows of its input so they can be read again without recomputing them.",
    ),
    (
        "Index Spool",
        "Stores the rows of its input in a temporary index so they can be looked up again.",
    ),
    (
        "Segment",
        "Splits the rows into groups for the operator above it.",
    ),
    (
        "Sequence Project",
        "Computes ranking and window values for each row.",
    ),
    (
        "Window Spool",
        "Stores the rows of a window frame while window functions are computed.",
    ),
    (
        "Parallelism",
        "Distributes, gathers or repartitions the rows between parallel threads.",
    ),
    (
        "Constant Scan",
        "Produces rows from constant values without reading a table.",
    ),
    (
        "Assert",
        "Checks a condition on the rows and raises an error when it fails.",
    ),
    (
        "Nested Loops Join",
        "For each row from the first input, looks up the matching rows in the second.",
    ),
    ("Distinct Sort", "Orders the rows and removes duplicates."),
];

pub(super) fn describe(operation: &str) -> Option<&'static str> {
    GLOSSARY
        .iter()
        .find(|(name, _)| *name == operation)
        .map(|(_, text)| *text)
}
