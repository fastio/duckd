//===----------------------------------------------------------------------===//
//                         DuckD CLI
//
// programs/client/main.cpp
//
// Interactive SQL shell for DuckD server via Arrow Flight SQL.
// Embeds a local DuckDB instance with the duckd_client extension loaded and
// ATTACHes the remote server as a named catalog.
//
// Usage:
//   duckd-cli grpc://host:8815          -- default catalog alias "remote"
//   duckd-cli grpc://host:8815 mydb     -- custom alias
//
// Once connected, all standard DuckDB SQL is available.
// Tables on the remote server are accessed as:
//   SELECT * FROM remote.main.my_table;
// or simply set the search path:
//   SET search_path = 'remote.main';
//   SELECT * FROM my_table;
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include <iostream>
#include <string>

#ifdef DUCKD_WITH_FLIGHT_SQL
// Extension entry point from the statically linked duckd_flight_client_static
extern "C" void duckd_client_init(duckdb::DatabaseInstance &db);
#endif

#include <readline/readline.h>
#include <readline/history.h>

static std::string Trim(const std::string &s) {
    auto b = s.find_first_not_of(" \t\n\r");
    if (b == std::string::npos) return "";
    auto e = s.find_last_not_of(" \t\n\r");
    return s.substr(b, e - b + 1);
}

static void PrintHelp(const std::string &alias) {
    std::cout <<
        "\nDuckD CLI – connected via Arrow Flight SQL\n"
        "\nMeta commands:\n"
        "  .help           Show this message\n"
        "  .quit / .exit   Exit the shell\n"
        "  .tables         List remote tables  (main schema)\n"
        "  .schema TABLE   Show column info for TABLE\n"
        "\nSQL tips:\n"
        "  Terminate statements with ';'\n"
        "  Remote tables: " << alias << ".main.tablename\n"
        "  Or: SET search_path='" << alias << ".main'; SELECT * FROM tablename;\n\n";
}

static void PrintResult(duckdb::QueryResult &res) {
    if (res.HasError()) {
        std::cerr << "Error: " << res.GetError() << "\n";
        return;
    }
    auto &mat = res.Cast<duckdb::MaterializedQueryResult>();

    // header
    std::cout << "\n";
    for (idx_t c = 0; c < mat.ColumnCount(); c++) {
        if (c) std::cout << "\t";
        std::cout << mat.ColumnName(c);
    }
    std::cout << "\n";
    for (idx_t c = 0; c < mat.ColumnCount(); c++) {
        if (c) std::cout << "\t";
        std::cout << std::string(mat.ColumnName(c).size(), '-');
    }
    std::cout << "\n";

    idx_t rows = mat.RowCount();
    for (idx_t r = 0; r < rows; r++) {
        for (idx_t c = 0; c < mat.ColumnCount(); c++) {
            if (c) std::cout << "\t";
            std::cout << mat.GetValue(c, r).ToString();
        }
        std::cout << "\n";
    }
    std::cout << "(" << rows << " row" << (rows != 1 ? "s" : "") << ")\n\n";
}

int main(int argc, char *argv[]) {
    std::string url   = "grpc://localhost:8815";
    std::string alias = "remote";

    if (argc >= 2) {
        std::string a = argv[1];
        if (a == "--help" || a == "-h") {
            std::cout <<
                "Usage: " << argv[0] << " [grpc://HOST:PORT] [ALIAS]\n"
                "\n"
                "  grpc://HOST:PORT   DuckD Flight SQL endpoint  (default: grpc://localhost:8815)\n"
                "  ALIAS              Local catalog alias         (default: remote)\n";
            return 0;
        }
        url = a;
    }
    if (argc >= 3) alias = argv[2];

#ifndef DUCKD_WITH_FLIGHT_SQL
    std::cerr << "duckd-cli was built without Arrow Flight SQL support.\n"
                 "Rebuild with -DWITH_FLIGHT_SQL=ON.\n";
    return 1;
#else
    // Bootstrap local DuckDB + extension
    duckdb::DuckDB local_db(nullptr);
    duckd_client_init(*local_db.instance);
    duckdb::Connection conn(local_db);

    // Attach remote server
    {
        auto r = conn.Query(
            "ATTACH '" + url + "' AS " + alias + " (TYPE duckd)");
        if (r->HasError()) {
            std::cerr << "Failed to connect to " << url << ":\n"
                      << r->GetError() << "\n";
            return 1;
        }
    }

    std::cout << "DuckD CLI – connected to " << url
              << " (catalog alias: " << alias << ")\n"
                 "Enter SQL followed by ';'  |  .help for tips  |  .quit to exit\n\n";

    using_history();

    std::string buf;
    bool multiline = false;

    while (true) {
        const char *prompt = multiline ? "   ...> " : "duckd> ";
        char *raw = readline(prompt);
        if (!raw) { std::cout << "\nBye!\n"; break; }

        std::string line(raw);
        free(raw);

        if (Trim(line).empty()) continue;
        add_history(line.c_str());

        // Meta commands (only at start of a fresh statement)
        if (!multiline) {
            std::string low;
            low.resize(line.size());
            std::transform(line.begin(), line.end(), low.begin(), ::tolower);
            std::string trimlow = Trim(low);

            if (trimlow == ".quit" || trimlow == ".exit" || trimlow == ".q") {
                std::cout << "Bye!\n"; break;
            }
            if (trimlow == ".help" || trimlow == ".h") {
                PrintHelp(alias); continue;
            }
            if (trimlow == ".tables") {
                auto r = conn.Query(
                    "SELECT table_name FROM duckd_query('" + url + "',"
                    " 'SELECT table_name FROM information_schema.tables"
                    "  WHERE table_schema = ''main'' ORDER BY table_name')");
                if (!r->HasError()) PrintResult(*r);
                else std::cerr << "Error: " << r->GetError() << "\n";
                continue;
            }
            if (Trim(line).substr(0, 7) == ".schema") {
                std::string tbl = Trim(line.substr(7));
                if (tbl.empty()) {
                    std::cerr << "Usage: .schema TABLENAME\n";
                } else {
                    auto r = conn.Query(
                        "SELECT column_name, data_type, is_nullable"
                        " FROM duckd_query('" + url + "',"
                        " 'SELECT column_name, data_type, is_nullable"
                        "  FROM information_schema.columns"
                        "  WHERE table_name = ''" + tbl + "''"
                        "  ORDER BY ordinal_position')");
                    if (!r->HasError()) PrintResult(*r);
                    else std::cerr << "Error: " << r->GetError() << "\n";
                }
                continue;
            }
        }

        buf += (multiline ? "\n" : "") + line;

        if (Trim(buf).back() != ';') { multiline = true; continue; }
        multiline = false;

        auto r = conn.Query(buf);
        PrintResult(*r);
        buf.clear();
    }

    conn.Query("DETACH " + alias);
    return 0;
#endif
}
