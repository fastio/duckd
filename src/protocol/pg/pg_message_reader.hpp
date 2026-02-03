//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// protocol/pg/pg_message_reader.hpp
//
// PostgreSQL message reader/parser
//===----------------------------------------------------------------------===//

#pragma once

#include "protocol/pg/pg_protocol.hpp"
#include <vector>
#include <string>
#include <cstring>
#include <optional>
#include <unordered_map>

namespace duckdb_server {
namespace pg {

//===----------------------------------------------------------------------===//
// Startup Message
//===----------------------------------------------------------------------===//
struct StartupMessage {
    int32_t protocol_version;
    std::unordered_map<std::string, std::string> parameters;

    std::string GetParameter(const std::string& key, const std::string& default_val = "") const {
        auto it = parameters.find(key);
        return it != parameters.end() ? it->second : default_val;
    }

    std::string GetUser() const { return GetParameter("user"); }
    std::string GetDatabase() const { return GetParameter("database"); }
};

//===----------------------------------------------------------------------===//
// Query Message
//===----------------------------------------------------------------------===//
struct QueryMessage {
    std::string query;
};

//===----------------------------------------------------------------------===//
// Parse Message (Extended Query)
//===----------------------------------------------------------------------===//
struct ParseMessage {
    std::string statement_name;
    std::string query;
    std::vector<int32_t> param_types;
};

//===----------------------------------------------------------------------===//
// Bind Message (Extended Query)
//===----------------------------------------------------------------------===//
struct BindMessage {
    std::string portal_name;
    std::string statement_name;
    std::vector<int16_t> param_formats;
    std::vector<std::optional<std::vector<uint8_t>>> param_values;
    std::vector<int16_t> result_formats;
};

//===----------------------------------------------------------------------===//
// Execute Message (Extended Query)
//===----------------------------------------------------------------------===//
struct ExecuteMessage {
    std::string portal_name;
    int32_t max_rows;
};

//===----------------------------------------------------------------------===//
// Describe Message
//===----------------------------------------------------------------------===//
struct DescribeMessage {
    char describe_type;  // 'S' for statement, 'P' for portal
    std::string name;
};

//===----------------------------------------------------------------------===//
// Close Message
//===----------------------------------------------------------------------===//
struct CloseMessage {
    char close_type;  // 'S' for statement, 'P' for portal
    std::string name;
};

//===----------------------------------------------------------------------===//
// Message Reader
//===----------------------------------------------------------------------===//
class PgMessageReader {
public:
    PgMessageReader(const uint8_t* data, size_t len)
        : data_(data), len_(len), pos_(0) {}

    bool HasRemaining(size_t bytes = 1) const {
        return pos_ + bytes <= len_;
    }

    size_t Remaining() const {
        return len_ - pos_;
    }

    //===------------------------------------------------------------------===//
    // Startup Message (no type byte)
    //===------------------------------------------------------------------===//
    bool ReadStartupMessage(StartupMessage& msg) {
        if (!HasRemaining(4)) return false;

        int32_t length = ReadInt32();
        if (length < 8 || !HasRemaining(length - 4)) return false;

        msg.protocol_version = ReadInt32();

        // Check for SSL or Cancel request
        if (msg.protocol_version == SSL_REQUEST_CODE ||
            msg.protocol_version == CANCEL_REQUEST_CODE) {
            return true;
        }

        // Read parameters
        while (HasRemaining() && data_[pos_] != 0) {
            std::string key = ReadString();
            if (!HasRemaining()) return false;
            std::string value = ReadString();
            msg.parameters[key] = value;
        }

        if (HasRemaining()) {
            pos_++;  // Skip final null byte
        }

        return true;
    }

    //===------------------------------------------------------------------===//
    // Query Message
    //===------------------------------------------------------------------===//
    bool ReadQueryMessage(QueryMessage& msg) {
        msg.query = ReadString();
        return true;
    }

    //===------------------------------------------------------------------===//
    // Parse Message
    //===------------------------------------------------------------------===//
    bool ReadParseMessage(ParseMessage& msg) {
        msg.statement_name = ReadString();
        msg.query = ReadString();

        if (!HasRemaining(2)) return false;
        int16_t num_params = ReadInt16();

        for (int16_t i = 0; i < num_params; i++) {
            if (!HasRemaining(4)) return false;
            msg.param_types.push_back(ReadInt32());
        }

        return true;
    }

    //===------------------------------------------------------------------===//
    // Bind Message
    //===------------------------------------------------------------------===//
    bool ReadBindMessage(BindMessage& msg) {
        msg.portal_name = ReadString();
        msg.statement_name = ReadString();

        if (!HasRemaining(2)) return false;
        int16_t num_format_codes = ReadInt16();

        for (int16_t i = 0; i < num_format_codes; i++) {
            if (!HasRemaining(2)) return false;
            msg.param_formats.push_back(ReadInt16());
        }

        if (!HasRemaining(2)) return false;
        int16_t num_params = ReadInt16();

        for (int16_t i = 0; i < num_params; i++) {
            if (!HasRemaining(4)) return false;
            int32_t param_len = ReadInt32();

            if (param_len == -1) {
                msg.param_values.push_back(std::nullopt);  // NULL
            } else {
                if (!HasRemaining(param_len)) return false;
                std::vector<uint8_t> value(data_ + pos_, data_ + pos_ + param_len);
                pos_ += param_len;
                msg.param_values.push_back(std::move(value));
            }
        }

        if (!HasRemaining(2)) return false;
        int16_t num_result_formats = ReadInt16();

        for (int16_t i = 0; i < num_result_formats; i++) {
            if (!HasRemaining(2)) return false;
            msg.result_formats.push_back(ReadInt16());
        }

        return true;
    }

    //===------------------------------------------------------------------===//
    // Execute Message
    //===------------------------------------------------------------------===//
    bool ReadExecuteMessage(ExecuteMessage& msg) {
        msg.portal_name = ReadString();
        if (!HasRemaining(4)) return false;
        msg.max_rows = ReadInt32();
        return true;
    }

    //===------------------------------------------------------------------===//
    // Describe Message
    //===------------------------------------------------------------------===//
    bool ReadDescribeMessage(DescribeMessage& msg) {
        if (!HasRemaining(1)) return false;
        msg.describe_type = static_cast<char>(ReadByte());
        msg.name = ReadString();
        return true;
    }

    //===------------------------------------------------------------------===//
    // Close Message
    //===------------------------------------------------------------------===//
    bool ReadCloseMessage(CloseMessage& msg) {
        if (!HasRemaining(1)) return false;
        msg.close_type = static_cast<char>(ReadByte());
        msg.name = ReadString();
        return true;
    }

private:
    uint8_t ReadByte() {
        return data_[pos_++];
    }

    int16_t ReadInt16() {
        int16_t value;
        std::memcpy(&value, data_ + pos_, 2);
        pos_ += 2;
        return NetworkToHost16(value);
    }

    int32_t ReadInt32() {
        int32_t value;
        std::memcpy(&value, data_ + pos_, 4);
        pos_ += 4;
        return NetworkToHost32(value);
    }

    std::string ReadString() {
        const char* start = reinterpret_cast<const char*>(data_ + pos_);
        size_t len = strnlen(start, len_ - pos_);
        pos_ += len + 1;  // Include null terminator
        return std::string(start, len);
    }

private:
    const uint8_t* data_;
    size_t len_;
    size_t pos_;
};

} // namespace pg
} // namespace duckdb_server
