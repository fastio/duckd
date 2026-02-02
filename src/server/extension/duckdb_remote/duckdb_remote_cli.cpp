//===----------------------------------------------------------------------===//
//                         DuckDB Remote CLI
//
// duckdb_remote_cli.cpp
//
// Interactive CLI client for DuckDB Server - mimics DuckDB shell interface
//===----------------------------------------------------------------------===//

#include "include/remote_client.hpp"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <readline/readline.h>
#include <readline/history.h>

using namespace duckdb;

//===----------------------------------------------------------------------===//
// Output Modes
//===----------------------------------------------------------------------===//

enum class OutputMode {
    BOX,      // Unicode box drawing (default)
    TABLE,    // ASCII table
    CSV,      // CSV format
    LINE,     // One value per line
    LIST,     // Values separated by delimiter
    COLUMN,   // Aligned columns
    MARKDOWN, // Markdown table
    JSON,     // JSON format
    JSONLINES // JSON Lines format
};

//===----------------------------------------------------------------------===//
// CLI State
//===----------------------------------------------------------------------===//

struct CLIState {
    std::string host = "localhost";
    uint16_t port = 9999;
    std::string username;
    OutputMode mode = OutputMode::BOX;
    std::string separator = "|";
    bool headers = true;
    bool timing = false;
    bool connected = false;
    std::unique_ptr<RemoteClient> client;
    int width = 0;  // 0 = auto
};

//===----------------------------------------------------------------------===//
// String Utilities
//===----------------------------------------------------------------------===//

std::string Trim(const std::string& str) {
    size_t start = str.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) return "";
    size_t end = str.find_last_not_of(" \t\n\r");
    return str.substr(start, end - start + 1);
}

std::string ToLower(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
}

std::string ValueToString(const Value& val) {
    if (val.IsNull()) {
        return "NULL";
    }
    return val.ToString();
}

size_t DisplayWidth(const std::string& str) {
    // Simple implementation - doesn't handle Unicode width correctly
    // but good enough for basic use
    return str.length();
}

//===----------------------------------------------------------------------===//
// Output Formatters
//===----------------------------------------------------------------------===//

void PrintBoxTable(const RemoteQueryResult& result, bool headers) {
    if (result.columns.empty()) return;

    size_t num_cols = result.columns.size();
    std::vector<size_t> widths(num_cols, 0);

    // Calculate column widths
    for (size_t i = 0; i < num_cols; i++) {
        widths[i] = DisplayWidth(result.columns[i].name);
    }
    for (const auto& row : result.rows) {
        for (size_t i = 0; i < std::min(num_cols, row.size()); i++) {
            widths[i] = std::max(widths[i], DisplayWidth(ValueToString(row[i])));
        }
    }

    // Box drawing characters
    const char* h = "─";
    const char* v = "│";
    const char* tl = "┌"; const char* tm = "┬"; const char* tr = "┐";
    const char* ml = "├"; const char* mm = "┼"; const char* mr = "┤";
    const char* bl = "└"; const char* bm = "┴"; const char* br = "┘";

    // Print top border
    std::cout << tl;
    for (size_t i = 0; i < num_cols; i++) {
        for (size_t j = 0; j < widths[i] + 2; j++) std::cout << h;
        std::cout << (i < num_cols - 1 ? tm : tr);
    }
    std::cout << "\n";

    // Print header
    if (headers) {
        std::cout << v;
        for (size_t i = 0; i < num_cols; i++) {
            std::cout << " " << std::left << std::setw(widths[i]) << result.columns[i].name << " " << v;
        }
        std::cout << "\n";

        // Print header separator
        std::cout << ml;
        for (size_t i = 0; i < num_cols; i++) {
            for (size_t j = 0; j < widths[i] + 2; j++) std::cout << h;
            std::cout << (i < num_cols - 1 ? mm : mr);
        }
        std::cout << "\n";
    }

    // Print rows
    for (const auto& row : result.rows) {
        std::cout << v;
        for (size_t i = 0; i < num_cols; i++) {
            std::string val = i < row.size() ? ValueToString(row[i]) : "";
            std::cout << " " << std::left << std::setw(widths[i]) << val << " " << v;
        }
        std::cout << "\n";
    }

    // Print bottom border
    std::cout << bl;
    for (size_t i = 0; i < num_cols; i++) {
        for (size_t j = 0; j < widths[i] + 2; j++) std::cout << h;
        std::cout << (i < num_cols - 1 ? bm : br);
    }
    std::cout << "\n";
}

void PrintASCIITable(const RemoteQueryResult& result, bool headers) {
    if (result.columns.empty()) return;

    size_t num_cols = result.columns.size();
    std::vector<size_t> widths(num_cols, 0);

    for (size_t i = 0; i < num_cols; i++) {
        widths[i] = DisplayWidth(result.columns[i].name);
    }
    for (const auto& row : result.rows) {
        for (size_t i = 0; i < std::min(num_cols, row.size()); i++) {
            widths[i] = std::max(widths[i], DisplayWidth(ValueToString(row[i])));
        }
    }

    auto print_separator = [&]() {
        std::cout << "+";
        for (size_t i = 0; i < num_cols; i++) {
            for (size_t j = 0; j < widths[i] + 2; j++) std::cout << "-";
            std::cout << "+";
        }
        std::cout << "\n";
    };

    print_separator();

    if (headers) {
        std::cout << "|";
        for (size_t i = 0; i < num_cols; i++) {
            std::cout << " " << std::left << std::setw(widths[i]) << result.columns[i].name << " |";
        }
        std::cout << "\n";
        print_separator();
    }

    for (const auto& row : result.rows) {
        std::cout << "|";
        for (size_t i = 0; i < num_cols; i++) {
            std::string val = i < row.size() ? ValueToString(row[i]) : "";
            std::cout << " " << std::left << std::setw(widths[i]) << val << " |";
        }
        std::cout << "\n";
    }

    print_separator();
}

void PrintCSV(const RemoteQueryResult& result, bool headers) {
    if (headers) {
        for (size_t i = 0; i < result.columns.size(); i++) {
            if (i > 0) std::cout << ",";
            std::cout << "\"" << result.columns[i].name << "\"";
        }
        std::cout << "\n";
    }

    for (const auto& row : result.rows) {
        for (size_t i = 0; i < row.size(); i++) {
            if (i > 0) std::cout << ",";
            std::string val = ValueToString(row[i]);
            if (val.find(',') != std::string::npos || val.find('"') != std::string::npos) {
                // Escape quotes and wrap in quotes
                std::string escaped;
                for (char c : val) {
                    if (c == '"') escaped += "\"\"";
                    else escaped += c;
                }
                std::cout << "\"" << escaped << "\"";
            } else {
                std::cout << val;
            }
        }
        std::cout << "\n";
    }
}

void PrintLine(const RemoteQueryResult& result) {
    for (size_t row_idx = 0; row_idx < result.rows.size(); row_idx++) {
        const auto& row = result.rows[row_idx];
        for (size_t i = 0; i < result.columns.size(); i++) {
            std::cout << result.columns[i].name << " = "
                      << (i < row.size() ? ValueToString(row[i]) : "") << "\n";
        }
        if (row_idx < result.rows.size() - 1) {
            std::cout << "\n";
        }
    }
}

void PrintList(const RemoteQueryResult& result, const std::string& sep, bool headers) {
    if (headers) {
        for (size_t i = 0; i < result.columns.size(); i++) {
            if (i > 0) std::cout << sep;
            std::cout << result.columns[i].name;
        }
        std::cout << "\n";
    }

    for (const auto& row : result.rows) {
        for (size_t i = 0; i < row.size(); i++) {
            if (i > 0) std::cout << sep;
            std::cout << ValueToString(row[i]);
        }
        std::cout << "\n";
    }
}

void PrintMarkdown(const RemoteQueryResult& result) {
    if (result.columns.empty()) return;

    size_t num_cols = result.columns.size();
    std::vector<size_t> widths(num_cols, 0);

    for (size_t i = 0; i < num_cols; i++) {
        widths[i] = std::max(widths[i], DisplayWidth(result.columns[i].name));
    }
    for (const auto& row : result.rows) {
        for (size_t i = 0; i < std::min(num_cols, row.size()); i++) {
            widths[i] = std::max(widths[i], DisplayWidth(ValueToString(row[i])));
        }
    }

    // Header
    std::cout << "|";
    for (size_t i = 0; i < num_cols; i++) {
        std::cout << " " << std::left << std::setw(widths[i]) << result.columns[i].name << " |";
    }
    std::cout << "\n";

    // Separator
    std::cout << "|";
    for (size_t i = 0; i < num_cols; i++) {
        std::cout << "-" << std::string(widths[i], '-') << "-|";
    }
    std::cout << "\n";

    // Rows
    for (const auto& row : result.rows) {
        std::cout << "|";
        for (size_t i = 0; i < num_cols; i++) {
            std::string val = i < row.size() ? ValueToString(row[i]) : "";
            std::cout << " " << std::left << std::setw(widths[i]) << val << " |";
        }
        std::cout << "\n";
    }
}

void PrintJSON(const RemoteQueryResult& result) {
    std::cout << "[\n";
    for (size_t row_idx = 0; row_idx < result.rows.size(); row_idx++) {
        const auto& row = result.rows[row_idx];
        std::cout << "  {";
        for (size_t i = 0; i < result.columns.size(); i++) {
            if (i > 0) std::cout << ", ";
            std::cout << "\"" << result.columns[i].name << "\": ";
            if (i < row.size() && !row[i].IsNull()) {
                std::string val = row[i].ToString();
                // Check if numeric
                if (result.columns[i].type.IsNumeric()) {
                    std::cout << val;
                } else {
                    std::cout << "\"" << val << "\"";
                }
            } else {
                std::cout << "null";
            }
        }
        std::cout << "}" << (row_idx < result.rows.size() - 1 ? "," : "") << "\n";
    }
    std::cout << "]\n";
}

void PrintJSONLines(const RemoteQueryResult& result) {
    for (const auto& row : result.rows) {
        std::cout << "{";
        for (size_t i = 0; i < result.columns.size(); i++) {
            if (i > 0) std::cout << ", ";
            std::cout << "\"" << result.columns[i].name << "\": ";
            if (i < row.size() && !row[i].IsNull()) {
                std::string val = row[i].ToString();
                if (result.columns[i].type.IsNumeric()) {
                    std::cout << val;
                } else {
                    std::cout << "\"" << val << "\"";
                }
            } else {
                std::cout << "null";
            }
        }
        std::cout << "}\n";
    }
}

void PrintResult(const RemoteQueryResult& result, const CLIState& state) {
    if (result.columns.empty() && result.rows.empty()) {
        return;
    }

    switch (state.mode) {
    case OutputMode::BOX:
        PrintBoxTable(result, state.headers);
        break;
    case OutputMode::TABLE:
        PrintASCIITable(result, state.headers);
        break;
    case OutputMode::CSV:
        PrintCSV(result, state.headers);
        break;
    case OutputMode::LINE:
        PrintLine(result);
        break;
    case OutputMode::LIST:
        PrintList(result, state.separator, state.headers);
        break;
    case OutputMode::COLUMN:
        PrintASCIITable(result, state.headers);  // Same as table for now
        break;
    case OutputMode::MARKDOWN:
        PrintMarkdown(result);
        break;
    case OutputMode::JSON:
        PrintJSON(result);
        break;
    case OutputMode::JSONLINES:
        PrintJSONLines(result);
        break;
    }
}

//===----------------------------------------------------------------------===//
// Meta Commands
//===----------------------------------------------------------------------===//

void PrintHelp() {
    std::cout << R"(
DuckDB Remote CLI - Connected to remote DuckDB Server

Meta Commands:
  .help                 Show this help message
  .quit, .exit          Exit the CLI
  .mode MODE            Set output mode (box, table, csv, line, list, markdown, json, jsonlines)
  .headers on|off       Toggle column headers
  .separator SEP        Set separator for list mode
  .timing on|off        Toggle timing display
  .status               Show connection status
  .tables               List tables in remote database
  .schema [TABLE]       Show schema of table(s)

SQL Commands:
  Enter any SQL statement followed by semicolon (;) or press Enter on a complete statement.
  Multi-line statements are supported.

Examples:
  SELECT * FROM my_table;
  CREATE TABLE test (id INTEGER, name VARCHAR);
  INSERT INTO test VALUES (1, 'hello');

)";
}

bool HandleMetaCommand(const std::string& cmd, CLIState& state) {
    std::string lower = ToLower(Trim(cmd));

    if (lower == ".help" || lower == ".h") {
        PrintHelp();
        return true;
    }

    if (lower == ".quit" || lower == ".exit" || lower == ".q") {
        std::cout << "Goodbye!\n";
        exit(0);
    }

    if (lower == ".status") {
        std::cout << "Connected: " << (state.connected ? "yes" : "no") << "\n";
        std::cout << "Server: " << state.host << ":" << state.port << "\n";
        if (state.connected) {
            std::cout << "Session ID: " << state.client->GetSessionId() << "\n";
        }
        std::cout << "Output mode: ";
        switch (state.mode) {
            case OutputMode::BOX: std::cout << "box"; break;
            case OutputMode::TABLE: std::cout << "table"; break;
            case OutputMode::CSV: std::cout << "csv"; break;
            case OutputMode::LINE: std::cout << "line"; break;
            case OutputMode::LIST: std::cout << "list"; break;
            case OutputMode::COLUMN: std::cout << "column"; break;
            case OutputMode::MARKDOWN: std::cout << "markdown"; break;
            case OutputMode::JSON: std::cout << "json"; break;
            case OutputMode::JSONLINES: std::cout << "jsonlines"; break;
        }
        std::cout << "\n";
        return true;
    }

    if (lower.substr(0, 5) == ".mode") {
        std::string mode_str = Trim(cmd.substr(5));
        mode_str = ToLower(mode_str);

        if (mode_str == "box") state.mode = OutputMode::BOX;
        else if (mode_str == "table") state.mode = OutputMode::TABLE;
        else if (mode_str == "csv") state.mode = OutputMode::CSV;
        else if (mode_str == "line") state.mode = OutputMode::LINE;
        else if (mode_str == "list") state.mode = OutputMode::LIST;
        else if (mode_str == "column") state.mode = OutputMode::COLUMN;
        else if (mode_str == "markdown" || mode_str == "md") state.mode = OutputMode::MARKDOWN;
        else if (mode_str == "json") state.mode = OutputMode::JSON;
        else if (mode_str == "jsonlines" || mode_str == "ndjson") state.mode = OutputMode::JSONLINES;
        else {
            std::cout << "Unknown mode: " << mode_str << "\n";
            std::cout << "Available modes: box, table, csv, line, list, column, markdown, json, jsonlines\n";
        }
        return true;
    }

    if (lower.substr(0, 8) == ".headers") {
        std::string val = Trim(ToLower(cmd.substr(8)));
        if (val == "on" || val == "1" || val == "true") {
            state.headers = true;
        } else if (val == "off" || val == "0" || val == "false") {
            state.headers = false;
        } else {
            std::cout << "Usage: .headers on|off\n";
        }
        return true;
    }

    if (lower.substr(0, 10) == ".separator") {
        state.separator = Trim(cmd.substr(10));
        if (state.separator.empty()) state.separator = "|";
        return true;
    }

    if (lower.substr(0, 7) == ".timing") {
        std::string val = Trim(ToLower(cmd.substr(7)));
        if (val == "on" || val == "1" || val == "true") {
            state.timing = true;
        } else if (val == "off" || val == "0" || val == "false") {
            state.timing = false;
        } else {
            std::cout << "Usage: .timing on|off\n";
        }
        return true;
    }

    if (lower == ".tables") {
        if (!state.connected) {
            std::cout << "Not connected\n";
            return true;
        }
        auto result = state.client->Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name");
        if (result->has_error) {
            std::cout << "Error: " << result->error_message << "\n";
        } else {
            PrintResult(*result, state);
        }
        return true;
    }

    if (lower.substr(0, 7) == ".schema") {
        if (!state.connected) {
            std::cout << "Not connected\n";
            return true;
        }
        std::string table = Trim(cmd.substr(7));
        std::string sql;
        if (table.empty()) {
            sql = "SELECT table_name, column_name, data_type FROM information_schema.columns ORDER BY table_name, ordinal_position";
        } else {
            sql = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '" + table + "' ORDER BY ordinal_position";
        }
        auto result = state.client->Query(sql);
        if (result->has_error) {
            std::cout << "Error: " << result->error_message << "\n";
        } else {
            PrintResult(*result, state);
        }
        return true;
    }

    std::cout << "Unknown command: " << cmd << "\n";
    std::cout << "Use .help for available commands\n";
    return true;
}

//===----------------------------------------------------------------------===//
// Main REPL
//===----------------------------------------------------------------------===//

void PrintBanner(const CLIState& state) {
    std::cout << R"(
DuckDB Remote CLI v1.0.0
Connected to )" << state.host << ":" << state.port << R"(
Enter ".help" for usage hints.
)";
}

std::string GetPrompt(bool continuation) {
    return continuation ? "   ...> " : "D remote> ";
}

int main(int argc, char* argv[]) {
    CLIState state;

    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if ((arg == "-h" || arg == "--host") && i + 1 < argc) {
            state.host = argv[++i];
        } else if ((arg == "-p" || arg == "--port") && i + 1 < argc) {
            state.port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if ((arg == "-u" || arg == "--user") && i + 1 < argc) {
            state.username = argv[++i];
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [OPTIONS]\n";
            std::cout << "\nOptions:\n";
            std::cout << "  -h, --host HOST    Server hostname (default: localhost)\n";
            std::cout << "  -p, --port PORT    Server port (default: 9999)\n";
            std::cout << "  -u, --user USER    Username\n";
            std::cout << "  --help             Show this help\n";
            return 0;
        } else if (arg[0] != '-') {
            // Assume it's host:port
            size_t colon = arg.find(':');
            if (colon != std::string::npos) {
                state.host = arg.substr(0, colon);
                state.port = static_cast<uint16_t>(std::stoi(arg.substr(colon + 1)));
            } else {
                state.host = arg;
            }
        }
    }

    // Connect to server
    state.client = std::make_unique<RemoteClient>();
    try {
        state.client->Connect(state.host, state.port, state.username);
        state.connected = true;
    } catch (const std::exception& e) {
        std::cerr << "Failed to connect to " << state.host << ":" << state.port << "\n";
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    PrintBanner(state);

    // Setup readline
    using_history();

    std::string query_buffer;
    bool in_multiline = false;

    while (true) {
        char* line = readline(GetPrompt(in_multiline).c_str());
        if (!line) {
            // EOF (Ctrl+D)
            std::cout << "\nGoodbye!\n";
            break;
        }

        std::string input(line);
        free(line);

        // Skip empty lines
        if (Trim(input).empty()) {
            continue;
        }

        // Add to history
        add_history(input.c_str());

        // Check for meta commands (only at start of input)
        if (!in_multiline && input[0] == '.') {
            HandleMetaCommand(input, state);
            continue;
        }

        // Accumulate query
        if (!query_buffer.empty()) {
            query_buffer += "\n";
        }
        query_buffer += input;

        // Check if query is complete (ends with semicolon)
        std::string trimmed = Trim(query_buffer);
        if (trimmed.empty()) {
            continue;
        }

        if (trimmed.back() != ';') {
            in_multiline = true;
            continue;
        }

        in_multiline = false;

        // Execute query
        try {
            auto start = std::chrono::steady_clock::now();
            auto result = state.client->Query(query_buffer);
            auto end = std::chrono::steady_clock::now();

            if (result->has_error) {
                std::cout << "Error: " << result->error_message << "\n";
            } else {
                PrintResult(*result, state);

                // Print row count and timing
                if (!result->rows.empty() || !result->columns.empty()) {
                    std::cout << result->rows.size() << " row" << (result->rows.size() != 1 ? "s" : "");
                    if (state.timing) {
                        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                        if (duration.count() < 1000) {
                            std::cout << " (" << duration.count() << " us)";
                        } else if (duration.count() < 1000000) {
                            std::cout << " (" << std::fixed << std::setprecision(2)
                                      << (duration.count() / 1000.0) << " ms)";
                        } else {
                            std::cout << " (" << std::fixed << std::setprecision(2)
                                      << (duration.count() / 1000000.0) << " s)";
                        }
                    }
                    std::cout << "\n";
                }
            }
        } catch (const std::exception& e) {
            std::cout << "Error: " << e.what() << "\n";
        }

        query_buffer.clear();
    }

    // Cleanup
    if (state.connected) {
        state.client->Disconnect();
    }

    return 0;
}
