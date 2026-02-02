//===----------------------------------------------------------------------===//
//                         DuckDB Remote Extension Test
//
// test_extension.cpp
//
// Simple test program to verify extension functionality
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "include/duckdb_remote.hpp"
#include "include/remote_client.hpp"
#include <iostream>

using namespace duckdb;

int main(int argc, char* argv[]) {
    std::string host = "localhost";
    uint16_t port = 9999;

    if (argc > 1) host = argv[1];
    if (argc > 2) port = static_cast<uint16_t>(std::stoi(argv[2]));

    std::cout << "=== DuckDB Remote Extension Test ===" << std::endl;
    std::cout << "Server: " << host << ":" << port << std::endl;

    try {
        // Create DuckDB instance
        DuckDB db(nullptr);
        Connection conn(db);

        // Load extension
        std::cout << "\n1. Loading extension..." << std::endl;
        ExtensionLoader loader(*db.instance, "duckdb_remote");
        DuckDBRemoteExtension extension;
        extension.Load(loader);
        std::cout << "   Extension loaded successfully!" << std::endl;

        // Test ping function
        std::cout << "\n2. Testing duckdb_remote_ping function..." << std::endl;
        auto result = conn.Query("SELECT duckdb_remote_ping('" + host + "', " + std::to_string(port) + ") AS ping_result");
        if (result->HasError()) {
            std::cerr << "   Error: " << result->GetError() << std::endl;
        } else {
            result->Print();
        }

        // Test remote_query function
        std::cout << "\n3. Testing remote_query function..." << std::endl;
        result = conn.Query("SELECT * FROM remote_query('" + host + "', " + std::to_string(port) + ", 'SELECT 1 + 1 AS result')");
        if (result->HasError()) {
            std::cerr << "   Error: " << result->GetError() << std::endl;
        } else {
            result->Print();
        }

        // Test more complex query
        std::cout << "\n4. Testing complex remote_query..." << std::endl;
        result = conn.Query("SELECT * FROM remote_query('" + host + "', " + std::to_string(port) + ", 'SELECT 42 AS answer, ''hello'' AS greeting')");
        if (result->HasError()) {
            std::cerr << "   Error: " << result->GetError() << std::endl;
        } else {
            result->Print();
        }

        std::cout << "\n=== Test completed ===" << std::endl;
        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
