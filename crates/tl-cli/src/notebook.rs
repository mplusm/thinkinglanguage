// ThinkingLanguage — Notebook Format
// Licensed under MIT OR Apache-2.0
//
// .tlnb JSON format for interactive notebooks with persistent VM state.

use serde::{Deserialize, Serialize};

/// A TL notebook document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notebook {
    pub metadata: NotebookMetadata,
    pub cells: Vec<Cell>,
}

/// Notebook-level metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotebookMetadata {
    pub tl_version: String,
    pub created: String,
    pub modified: String,
}

/// A single cell in the notebook.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cell {
    pub cell_type: CellType,
    pub source: String,
    #[serde(default)]
    pub outputs: Vec<CellOutput>,
    #[serde(default)]
    pub execution_count: Option<u32>,
}

/// Cell type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CellType {
    Code,
    Markdown,
}

/// Output from executing a cell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellOutput {
    pub output_type: OutputType,
    pub text: String,
}

/// Type of cell output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputType {
    Result,
    Stdout,
    Error,
}

impl Notebook {
    /// Create a new empty notebook.
    pub fn new() -> Self {
        let now = chrono::Utc::now().to_rfc3339();
        Self {
            metadata: NotebookMetadata {
                tl_version: env!("CARGO_PKG_VERSION").to_string(),
                created: now.clone(),
                modified: now,
            },
            cells: vec![Cell {
                cell_type: CellType::Code,
                source: String::new(),
                outputs: Vec::new(),
                execution_count: None,
            }],
        }
    }

    /// Load from a .tlnb file.
    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Cannot read notebook: {e}"))?;
        serde_json::from_str(&content)
            .map_err(|e| format!("Cannot parse notebook: {e}"))
    }

    /// Save to a .tlnb file.
    pub fn save(&mut self, path: &std::path::Path) -> Result<(), String> {
        self.metadata.modified = chrono::Utc::now().to_rfc3339();
        let content = serde_json::to_string_pretty(self)
            .map_err(|e| format!("Cannot serialize notebook: {e}"))?;
        std::fs::write(path, content)
            .map_err(|e| format!("Cannot write notebook: {e}"))
    }

    /// Export all code cells as a plain .tl file.
    pub fn export_tl(&self) -> String {
        let mut out = String::new();
        for cell in &self.cells {
            if cell.cell_type == CellType::Code && !cell.source.trim().is_empty() {
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str(&cell.source);
                out.push('\n');
            }
        }
        out
    }

    /// Add a new cell at the given index.
    pub fn add_cell(&mut self, index: usize, cell_type: CellType) {
        let cell = Cell {
            cell_type,
            source: String::new(),
            outputs: Vec::new(),
            execution_count: None,
        };
        if index >= self.cells.len() {
            self.cells.push(cell);
        } else {
            self.cells.insert(index, cell);
        }
    }

    /// Remove a cell at the given index.
    pub fn remove_cell(&mut self, index: usize) -> bool {
        if index < self.cells.len() && self.cells.len() > 1 {
            self.cells.remove(index);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_notebook() {
        let nb = Notebook::new();
        assert_eq!(nb.cells.len(), 1);
        assert_eq!(nb.cells[0].cell_type, CellType::Code);
    }

    #[test]
    fn roundtrip_json() {
        let mut nb = Notebook::new();
        nb.cells[0].source = "let x = 42".to_string();
        nb.cells[0].outputs.push(CellOutput {
            output_type: OutputType::Result,
            text: "42".to_string(),
        });

        let json = serde_json::to_string_pretty(&nb).unwrap();
        let nb2: Notebook = serde_json::from_str(&json).unwrap();
        assert_eq!(nb2.cells.len(), 1);
        assert_eq!(nb2.cells[0].source, "let x = 42");
        assert_eq!(nb2.cells[0].outputs[0].text, "42");
    }

    #[test]
    fn export_tl() {
        let mut nb = Notebook::new();
        nb.cells[0].source = "let x = 1".to_string();
        nb.add_cell(1, CellType::Markdown);
        nb.cells[1].source = "# Comment".to_string();
        nb.add_cell(2, CellType::Code);
        nb.cells[2].source = "let y = 2".to_string();

        let exported = nb.export_tl();
        assert!(exported.contains("let x = 1"));
        assert!(!exported.contains("# Comment"));
        assert!(exported.contains("let y = 2"));
    }

    #[test]
    fn add_remove_cells() {
        let mut nb = Notebook::new();
        assert_eq!(nb.cells.len(), 1);

        nb.add_cell(1, CellType::Code);
        assert_eq!(nb.cells.len(), 2);

        nb.add_cell(1, CellType::Markdown);
        assert_eq!(nb.cells.len(), 3);
        assert_eq!(nb.cells[1].cell_type, CellType::Markdown);

        assert!(nb.remove_cell(1));
        assert_eq!(nb.cells.len(), 2);

        // Cannot remove the last cell
        assert!(nb.remove_cell(0));
        assert!(!nb.remove_cell(0));
        assert_eq!(nb.cells.len(), 1);
    }

    #[test]
    fn save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.tlnb");

        let mut nb = Notebook::new();
        nb.cells[0].source = "print(\"hello\")".to_string();
        nb.save(&path).unwrap();

        let nb2 = Notebook::load(&path).unwrap();
        assert_eq!(nb2.cells[0].source, "print(\"hello\")");
    }
}
