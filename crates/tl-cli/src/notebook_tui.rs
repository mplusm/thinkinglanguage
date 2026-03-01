// ThinkingLanguage — Notebook TUI
// Licensed under MIT OR Apache-2.0
//
// Terminal UI for interactive .tlnb notebooks using ratatui + crossterm.

use std::io;
use std::path::PathBuf;

use crossterm::{
    ExecutableCommand,
    event::{self, Event, KeyCode, KeyModifiers},
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{prelude::*, widgets::*};

use tl_compiler::{Vm, VmValue, compile};
use tl_parser::parse;

use crate::notebook::{CellOutput, CellType, Notebook, OutputType};

/// TUI application mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Normal,
    Edit,
}

/// TUI application state.
pub struct NotebookApp {
    notebook: Notebook,
    file_path: PathBuf,
    vm: Vm,
    selected: usize,
    mode: Mode,
    edit_buffer: String,
    scroll_offset: u16,
    execution_counter: u32,
    status_msg: String,
    should_quit: bool,
}

impl NotebookApp {
    /// Create a new notebook app.
    pub fn new(notebook: Notebook, file_path: PathBuf) -> Self {
        Self {
            notebook,
            file_path,
            vm: Vm::new(),
            selected: 0,
            mode: Mode::Normal,
            edit_buffer: String::new(),
            scroll_offset: 0,
            execution_counter: 0,
            status_msg: "Press 'h' for help, 'q' to quit".to_string(),
            should_quit: false,
        }
    }

    /// Run the TUI event loop.
    pub fn run(&mut self) -> io::Result<()> {
        enable_raw_mode()?;
        io::stdout().execute(EnterAlternateScreen)?;
        let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;

        while !self.should_quit {
            terminal.draw(|frame| self.draw(frame))?;
            if event::poll(std::time::Duration::from_millis(100))? {
                if let Event::Key(key) = event::read()? {
                    match self.mode {
                        Mode::Normal => self.handle_normal_key(key.code, key.modifiers),
                        Mode::Edit => self.handle_edit_key(key.code, key.modifiers),
                    }
                }
            }
        }

        disable_raw_mode()?;
        io::stdout().execute(LeaveAlternateScreen)?;
        Ok(())
    }

    /// Handle key events in Normal mode.
    fn handle_normal_key(&mut self, code: KeyCode, _mods: KeyModifiers) {
        match code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('j') | KeyCode::Down => {
                if self.selected + 1 < self.notebook.cells.len() {
                    self.selected += 1;
                }
            }
            KeyCode::Char('k') | KeyCode::Up => {
                if self.selected > 0 {
                    self.selected -= 1;
                }
            }
            KeyCode::Enter => self.execute_cell(),
            KeyCode::Char('e') => {
                self.edit_buffer = self.notebook.cells[self.selected].source.clone();
                self.mode = Mode::Edit;
                self.status_msg = "EDIT MODE — Esc to finish, Ctrl-C to cancel".to_string();
            }
            KeyCode::Char('a') => {
                self.notebook.add_cell(self.selected + 1, CellType::Code);
                self.selected += 1;
                self.edit_buffer = String::new();
                self.mode = Mode::Edit;
                self.status_msg = "New code cell — Esc to finish".to_string();
            }
            KeyCode::Char('A') => {
                self.notebook
                    .add_cell(self.selected + 1, CellType::Markdown);
                self.selected += 1;
                self.edit_buffer = String::new();
                self.mode = Mode::Edit;
                self.status_msg = "New markdown cell — Esc to finish".to_string();
            }
            KeyCode::Char('d') => {
                if self.notebook.remove_cell(self.selected) {
                    if self.selected >= self.notebook.cells.len() {
                        self.selected = self.notebook.cells.len() - 1;
                    }
                    self.status_msg = "Cell deleted".to_string();
                }
            }
            KeyCode::Char('s') => match self.notebook.save(&self.file_path) {
                Ok(()) => {
                    self.status_msg = format!("Saved to {}", self.file_path.display());
                }
                Err(e) => {
                    self.status_msg = format!("Save error: {e}");
                }
            },
            KeyCode::Char('x') => {
                let tl_path = self.file_path.with_extension("tl");
                let content = self.notebook.export_tl();
                match std::fs::write(&tl_path, content) {
                    Ok(()) => {
                        self.status_msg = format!("Exported to {}", tl_path.display());
                    }
                    Err(e) => {
                        self.status_msg = format!("Export error: {e}");
                    }
                }
            }
            KeyCode::Char('h') => {
                self.status_msg =
                    "j/k:nav Enter:run e:edit a/A:add code/md d:del s:save x:export q:quit"
                        .to_string();
            }
            _ => {}
        }
    }

    /// Handle key events in Edit mode.
    fn handle_edit_key(&mut self, code: KeyCode, mods: KeyModifiers) {
        match code {
            KeyCode::Esc => {
                // Commit edit
                self.notebook.cells[self.selected].source = self.edit_buffer.clone();
                self.mode = Mode::Normal;
                self.status_msg = "Edit saved".to_string();
            }
            KeyCode::Char('c') if mods.contains(KeyModifiers::CONTROL) => {
                // Cancel edit
                self.mode = Mode::Normal;
                self.status_msg = "Edit cancelled".to_string();
            }
            KeyCode::Char(c) => {
                self.edit_buffer.push(c);
            }
            KeyCode::Enter => {
                self.edit_buffer.push('\n');
            }
            KeyCode::Backspace => {
                self.edit_buffer.pop();
            }
            KeyCode::Tab => {
                self.edit_buffer.push_str("    ");
            }
            _ => {}
        }
    }

    /// Execute the currently selected cell.
    fn execute_cell(&mut self) {
        let cell = &self.notebook.cells[self.selected];
        if cell.cell_type != CellType::Code || cell.source.trim().is_empty() {
            return;
        }

        let source = cell.source.clone();
        self.execution_counter += 1;
        let exec_count = self.execution_counter;

        // Clear previous outputs
        self.notebook.cells[self.selected].outputs.clear();

        // Clear VM output buffer
        self.vm.output.clear();

        // Parse
        let program = match parse(&source) {
            Ok(p) => p,
            Err(e) => {
                self.notebook.cells[self.selected].outputs.push(CellOutput {
                    output_type: OutputType::Error,
                    text: format!("Parse error: {e}"),
                });
                self.notebook.cells[self.selected].execution_count = Some(exec_count);
                self.status_msg = "Parse error".to_string();
                return;
            }
        };

        // Compile
        let proto = match compile(&program) {
            Ok(p) => p,
            Err(e) => {
                self.notebook.cells[self.selected].outputs.push(CellOutput {
                    output_type: OutputType::Error,
                    text: format!("Compile error: {e}"),
                });
                self.notebook.cells[self.selected].execution_count = Some(exec_count);
                self.status_msg = "Compile error".to_string();
                return;
            }
        };

        // Execute
        match self.vm.execute(&proto) {
            Ok(val) => {
                // Capture stdout output
                for line in &self.vm.output {
                    self.notebook.cells[self.selected].outputs.push(CellOutput {
                        output_type: OutputType::Stdout,
                        text: line.clone(),
                    });
                }
                // Capture result value
                if !matches!(val, VmValue::None) {
                    self.notebook.cells[self.selected].outputs.push(CellOutput {
                        output_type: OutputType::Result,
                        text: format!("{val}"),
                    });
                }
                self.status_msg = format!("Cell [{}] executed", exec_count);
            }
            Err(e) => {
                // Capture any stdout before the error
                for line in &self.vm.output {
                    self.notebook.cells[self.selected].outputs.push(CellOutput {
                        output_type: OutputType::Stdout,
                        text: line.clone(),
                    });
                }
                self.notebook.cells[self.selected].outputs.push(CellOutput {
                    output_type: OutputType::Error,
                    text: format!("{e}"),
                });
                self.status_msg = "Runtime error".to_string();
            }
        }

        self.notebook.cells[self.selected].execution_count = Some(exec_count);
    }

    /// Draw the TUI.
    fn draw(&self, frame: &mut Frame) {
        let area = frame.area();

        // Layout: header + cells + status
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2), // header
                Constraint::Min(3),    // cells
                Constraint::Length(1), // status
            ])
            .split(area);

        // Header
        let mode_str = match self.mode {
            Mode::Normal => "NORMAL",
            Mode::Edit => "EDIT",
        };
        let title = format!(
            " TL Notebook — {} [{mode_str}]",
            self.file_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
        );
        let header = Paragraph::new(title)
            .style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )
            .block(Block::default().borders(Borders::BOTTOM));
        frame.render_widget(header, chunks[0]);

        // Cells area
        self.draw_cells(frame, chunks[1]);

        // Status bar
        let status = Paragraph::new(format!(" {}", self.status_msg))
            .style(Style::default().fg(Color::Yellow));
        frame.render_widget(status, chunks[2]);
    }

    /// Draw all cells in the scrollable area.
    fn draw_cells(&self, frame: &mut Frame, area: Rect) {
        let mut lines: Vec<Line> = Vec::new();

        for (i, cell) in self.notebook.cells.iter().enumerate() {
            let is_selected = i == self.selected;

            // Cell header
            let marker = if is_selected { ">" } else { " " };
            let type_str = match cell.cell_type {
                CellType::Code => "Code",
                CellType::Markdown => "Md",
            };
            let exec_str = cell
                .execution_count
                .map(|n| format!("[{n}]"))
                .unwrap_or_default();

            let header_style = if is_selected {
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::DarkGray)
            };

            lines.push(Line::from(Span::styled(
                format!("{marker} --- {type_str} {exec_str} ---"),
                header_style,
            )));

            // Cell source
            let source = if is_selected && self.mode == Mode::Edit {
                &self.edit_buffer
            } else {
                &cell.source
            };

            let source_style = if is_selected {
                Style::default().fg(Color::White)
            } else {
                Style::default().fg(Color::Gray)
            };

            if source.is_empty() {
                lines.push(Line::from(Span::styled(
                    "  (empty)",
                    Style::default().fg(Color::DarkGray),
                )));
            } else {
                for line in source.lines() {
                    lines.push(Line::from(Span::styled(format!("  {line}"), source_style)));
                }
            }

            // Cell outputs
            for output in &cell.outputs {
                let (prefix, style) = match output.output_type {
                    OutputType::Result => ("=>", Style::default().fg(Color::Cyan)),
                    OutputType::Stdout => ("  ", Style::default().fg(Color::White)),
                    OutputType::Error => ("!!", Style::default().fg(Color::Red)),
                };
                for line in output.text.lines() {
                    lines.push(Line::from(Span::styled(
                        format!("  {prefix} {line}"),
                        style,
                    )));
                }
            }

            // Blank line between cells
            lines.push(Line::from(""));
        }

        let paragraph = Paragraph::new(lines).scroll((self.scroll_offset, 0));
        frame.render_widget(paragraph, area);
    }
}
