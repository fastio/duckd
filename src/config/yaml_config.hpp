//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// config/yaml_config.hpp
//
// YAML configuration file parser
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <yaml-cpp/yaml.h>

namespace duckdb_server {

class YamlConfig {
public:
    bool Load(const std::string& path) {
        try {
            root_ = YAML::LoadFile(path);
            return true;
        } catch (const YAML::Exception& e) {
            error_ = "YAML parse error: " + std::string(e.what());
            return false;
        } catch (const std::exception& e) {
            error_ = "Cannot open config file: " + std::string(e.what());
            return false;
        }
    }

    template<typename T>
    T Get(const std::string& path, const T& default_val) const {
        try {
            YAML::Node node = GetNode(path);
            if (node && !node.IsNull()) {
                return node.as<T>();
            }
        } catch (...) {
            // Fall through to default
        }
        return default_val;
    }

    std::string GetString(const std::string& path, const std::string& default_val = "") const {
        return Get<std::string>(path, default_val);
    }

    int GetInt(const std::string& path, int default_val = 0) const {
        return Get<int>(path, default_val);
    }

    int64_t GetInt64(const std::string& path, int64_t default_val = 0) const {
        return Get<int64_t>(path, default_val);
    }

    bool GetBool(const std::string& path, bool default_val = false) const {
        return Get<bool>(path, default_val);
    }

    bool Has(const std::string& path) const {
        try {
            YAML::Node node = GetNode(path);
            return node && !node.IsNull();
        } catch (...) {
            return false;
        }
    }

    const std::string& GetError() const { return error_; }

private:
    // Get node by dot-separated path (e.g., "server.port")
    YAML::Node GetNode(const std::string& path) const {
        YAML::Node current = YAML::Clone(root_);

        size_t start = 0;
        size_t end;

        while ((end = path.find('.', start)) != std::string::npos) {
            std::string key = path.substr(start, end - start);
            if (!current[key]) {
                return YAML::Node();
            }
            current = current[key];
            start = end + 1;
        }

        std::string final_key = path.substr(start);
        return current[final_key];
    }

    YAML::Node root_;
    std::string error_;
};

} // namespace duckdb_server