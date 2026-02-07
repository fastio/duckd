//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// protocol/pg/pg_message_writer.hpp
//
// PostgreSQL message writer
//===----------------------------------------------------------------------===//

#pragma once

#include "protocol/pg/pg_protocol.hpp"
#include "protocol/pg/pg_types.hpp"
#include "duckdb.hpp"
#include <vector>
#include <string>
#include <cstring>

namespace duckdb_server {
namespace pg {

class PgMessageWriter {
public:
    PgMessageWriter() {
        buffer_.reserve(4096);
    }

    // Get the buffer
    const std::vector<uint8_t>& GetBuffer() const { return buffer_; }
    std::vector<uint8_t>&& TakeBuffer() { return std::move(buffer_); }

    // Clear the buffer
    void Clear() { buffer_.clear(); }

    //===------------------------------------------------------------------===//
    // Authentication Messages
    //===------------------------------------------------------------------===//
    void WriteAuthenticationOk() {
        StartMessage(BackendMessage::Authentication);
        WriteInt32(AuthType::Ok);
        EndMessage();
    }

    void WriteAuthenticationCleartextPassword() {
        StartMessage(BackendMessage::Authentication);
        WriteInt32(AuthType::CleartextPassword);
        EndMessage();
    }

    void WriteAuthenticationMD5Password(const uint8_t salt[4]) {
        StartMessage(BackendMessage::Authentication);
        WriteInt32(AuthType::MD5Password);
        WriteBytes(salt, 4);
        EndMessage();
    }

    //===------------------------------------------------------------------===//
    // Startup Messages
    //===------------------------------------------------------------------===//
    void WriteParameterStatus(const std::string& name, const std::string& value) {
        StartMessage(BackendMessage::ParameterStatus);
        WriteString(name);
        WriteString(value);
        EndMessage();
    }

    void WriteBackendKeyData(int32_t process_id, int32_t secret_key) {
        StartMessage(BackendMessage::BackendKeyData);
        WriteInt32(process_id);
        WriteInt32(secret_key);
        EndMessage();
    }

    void WriteReadyForQuery(char transaction_status) {
        StartMessage(BackendMessage::ReadyForQuery);
        WriteByte(transaction_status);
        EndMessage();
    }

    //===------------------------------------------------------------------===//
    // Query Response Messages
    //===------------------------------------------------------------------===//
    void WriteRowDescription(const std::vector<std::string>& names,
                             const std::vector<duckdb::LogicalType>& types) {
        StartMessage(BackendMessage::RowDescription);
        WriteInt16(static_cast<int16_t>(names.size()));

        for (size_t i = 0; i < names.size(); i++) {
            WriteString(names[i]);           // Field name
            WriteInt32(0);                   // Table OID (0 = not a table column)
            WriteInt16(0);                   // Column attribute number
            int32_t type_oid = DuckDBTypeToOid(types[i]);
            WriteInt32(type_oid);            // Type OID
            WriteInt16(GetTypeSize(type_oid)); // Type size
            WriteInt32(-1);                  // Type modifier
            WriteInt16(FormatCode::Text);    // Format code (text)
        }
        EndMessage();
    }

    void WriteDataRow(const std::vector<duckdb::Value>& values) {
        StartMessage(BackendMessage::DataRow);
        WriteInt16(static_cast<int16_t>(values.size()));

        for (const auto& value : values) {
            if (value.IsNull()) {
                WriteInt32(-1);  // NULL
            } else {
                FormatValueInto(value, format_buffer_);
                WriteInt32(static_cast<int32_t>(format_buffer_.size()));
                WriteRawBytes(format_buffer_.data(), format_buffer_.size());
            }
        }
        EndMessage();
    }

    void WriteCommandComplete(const std::string& tag) {
        StartMessage(BackendMessage::CommandComplete);
        WriteString(tag);
        EndMessage();
    }

    void WriteEmptyQueryResponse() {
        StartMessage(BackendMessage::EmptyQueryResponse);
        EndMessage();
    }

    //===------------------------------------------------------------------===//
    // Extended Query Messages
    //===------------------------------------------------------------------===//
    void WriteParseComplete() {
        StartMessage(BackendMessage::ParseComplete);
        EndMessage();
    }

    void WriteBindComplete() {
        StartMessage(BackendMessage::BindComplete);
        EndMessage();
    }

    void WriteCloseComplete() {
        StartMessage(BackendMessage::CloseComplete);
        EndMessage();
    }

    void WriteNoData() {
        StartMessage(BackendMessage::NoData);
        EndMessage();
    }

    void WriteParameterDescription(const std::vector<int32_t>& param_types) {
        StartMessage(BackendMessage::ParameterDescription);
        WriteInt16(static_cast<int16_t>(param_types.size()));
        for (int32_t oid : param_types) {
            WriteInt32(oid);
        }
        EndMessage();
    }

    //===------------------------------------------------------------------===//
    // Error/Notice Messages
    //===------------------------------------------------------------------===//
    void WriteErrorResponse(const std::string& severity,
                            const std::string& code,
                            const std::string& message,
                            const std::string& detail = "",
                            const std::string& hint = "") {
        StartMessage(BackendMessage::ErrorResponse);
        WriteErrorField(ErrorField::Severity, severity);
        WriteErrorField(ErrorField::SeverityNonLocalized, severity);
        WriteErrorField(ErrorField::Code, code);
        WriteErrorField(ErrorField::Message, message);
        if (!detail.empty()) {
            WriteErrorField(ErrorField::Detail, detail);
        }
        if (!hint.empty()) {
            WriteErrorField(ErrorField::Hint, hint);
        }
        WriteByte(0);  // Terminator
        EndMessage();
    }

    void WriteNoticeResponse(const std::string& severity,
                             const std::string& code,
                             const std::string& message) {
        StartMessage(BackendMessage::NoticeResponse);
        WriteErrorField(ErrorField::Severity, severity);
        WriteErrorField(ErrorField::Code, code);
        WriteErrorField(ErrorField::Message, message);
        WriteByte(0);  // Terminator
        EndMessage();
    }

private:
    void StartMessage(char type) {
        message_start_ = buffer_.size();
        buffer_.push_back(static_cast<uint8_t>(type));
        // Reserve space for length (will be filled in EndMessage)
        buffer_.resize(buffer_.size() + 4);
    }

    void EndMessage() {
        // Calculate and write message length (includes length field itself)
        int32_t length = static_cast<int32_t>(buffer_.size() - message_start_ - 1);
        int32_t network_length = HostToNetwork32(length);
        std::memcpy(buffer_.data() + message_start_ + 1, &network_length, 4);
    }

    void WriteByte(uint8_t value) {
        buffer_.push_back(value);
    }

    void WriteBytes(const uint8_t* data, size_t len) {
        buffer_.insert(buffer_.end(), data, data + len);
    }

    void WriteRawBytes(const char* data, size_t len) {
        buffer_.insert(buffer_.end(), data, data + len);
    }

    void WriteInt16(int16_t value) {
        int16_t network_value = HostToNetwork16(value);
        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&network_value);
        buffer_.insert(buffer_.end(), ptr, ptr + 2);
    }

    void WriteInt32(int32_t value) {
        int32_t network_value = HostToNetwork32(value);
        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&network_value);
        buffer_.insert(buffer_.end(), ptr, ptr + 4);
    }

    void WriteString(const std::string& str) {
        buffer_.insert(buffer_.end(), str.begin(), str.end());
        buffer_.push_back(0);  // Null terminator
    }

    void WriteErrorField(char field_type, const std::string& value) {
        buffer_.push_back(static_cast<uint8_t>(field_type));
        WriteString(value);
    }

private:
    std::vector<uint8_t> buffer_;
    size_t message_start_ = 0;
    std::string format_buffer_;  // Reusable formatting buffer for WriteDataRow
};

} // namespace pg
} // namespace duckdb_server
