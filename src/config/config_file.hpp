//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// config/config_file.hpp
//
// Configuration file parser (simple key=value format)
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <unordered_map>
#include <fstream>
#include <sstream>
#include <algorithm>

namespace duckdb_server {

class ConfigFile {
public:
    bool Load(const std::string& path) {
        std::ifstream file(path);
        if (!file) {
            error_ = "Cannot open config file: " + path;
            return false;
        }

        std::string line;
        int line_num = 0;
        while (std::getline(file, line)) {
            line_num++;
            // Trim whitespace
            line.erase(0, line.find_first_not_of(" \t"));
            if (line.empty() || line[0] == '#' || line[0] == ';') {
                continue;  // Skip empty lines and comments
            }

            auto eq_pos = line.find('=');
            if (eq_pos == std::string::npos) {
                error_ = "Invalid syntax at line " + std::to_string(line_num);
                return false;
            }

            std::string key = line.substr(0, eq_pos);
            std::string value = line.substr(eq_pos + 1);

            // Trim key and value
            key.erase(key.find_last_not_of(" \t") + 1);
            value.erase(0, value.find_first_not_of(" \t"));
            value.erase(value.find_last_not_of(" \t") + 1);

            // Remove quotes if present
            if (value.size() >= 2 &&
                ((value.front() == '"' && value.back() == '"') ||
                 (value.front() == '\'' && value.back() == '\''))) {
                value = value.substr(1, value.size() - 2);
            }

            values_[key] = value;
        }

        return true;
    }

    std::string GetString(const std::string& key, const std::string& default_val = "") const {
        auto it = values_.find(key);
        return it != values_.end() ? it->second : default_val;
    }

    int GetInt(const std::string& key, int default_val = 0) const {
        auto it = values_.find(key);
        if (it == values_.end()) return default_val;
        try {
            return std::stoi(it->second);
        } catch (...) {
            return default_val;
        }
    }

    bool GetBool(const std::string& key, bool default_val = false) const {
        auto it = values_.find(key);
        if (it == values_.end()) return default_val;
        std::string val = it->second;
        std::transform(val.begin(), val.end(), val.begin(), ::tolower);
        return val == "true" || val == "yes" || val == "1" || val == "on";
    }

    bool Has(const std::string& key) const {
        return values_.find(key) != values_.end();
    }

    const std::string& GetError() const { return error_; }

private:
    std::unordered_map<std::string, std::string> values_;
    std::string error_;
};

} // namespace duckdb_server
