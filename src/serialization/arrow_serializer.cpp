//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// serialization/arrow_serializer.cpp
//
// Arrow IPC serialization implementation
//===----------------------------------------------------------------------===//

#include "serialization/arrow_serializer.hpp"
#include "utils/logger.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"

namespace duckdb_server {

ArrowSerializer::ArrowSerializer(const SerializerConfig& config)
    : config_(config) {
    // Initialize client properties
    client_props_.arrow_offset_size = duckdb::ArrowOffsetSize::REGULAR;
}

ArrowSerializer::~ArrowSerializer() {
}

std::vector<uint8_t> ArrowSerializer::SerializeSchema(
    const duckdb::vector<duckdb::LogicalType>& types,
    const duckdb::vector<duckdb::string>& names) {
    
    ArrowSchema schema;
    duckdb::ArrowConverter::ToArrowSchema(&schema, types, names, client_props_);
    
    auto result = SerializeArrowSchema(&schema);
    
    if (schema.release) {
        schema.release(&schema);
    }
    
    return result;
}

std::vector<uint8_t> ArrowSerializer::SerializeChunk(
    duckdb::DataChunk& chunk,
    const duckdb::vector<duckdb::LogicalType>& types,
    const duckdb::vector<duckdb::string>& names) {
    
    ArrowSchema schema;
    ArrowArray array;
    
    duckdb::ArrowConverter::ToArrowSchema(&schema, types, names, client_props_);
    
    // Convert chunk to Arrow array
    duckdb::unordered_map<duckdb::idx_t, const duckdb::shared_ptr<duckdb::ArrowTypeExtensionData>> extension_types;
    duckdb::ArrowConverter::ToArrowArray(chunk, &array, client_props_, extension_types);
    
    auto result = SerializeArrowArray(&schema, &array);
    
    if (array.release) {
        array.release(&array);
    }
    if (schema.release) {
        schema.release(&schema);
    }
    
    return result;
}

std::vector<uint8_t> ArrowSerializer::SerializeResultHeader(duckdb::QueryResult& result) {
    // Use the result's client_properties which has a valid ClientContext
    ArrowSchema schema;
    duckdb::ArrowConverter::ToArrowSchema(&schema, result.types, result.names, result.client_properties);

    auto buffer = SerializeArrowSchema(&schema);

    if (schema.release) {
        schema.release(&schema);
    }

    return buffer;
}

std::vector<uint8_t> ArrowSerializer::SerializeResultBatch(
    duckdb::DataChunk& chunk,
    duckdb::QueryResult& result) {

    // Use the result's client_properties which has a valid ClientContext
    ArrowSchema schema;
    ArrowArray array;

    duckdb::ArrowConverter::ToArrowSchema(&schema, result.types, result.names, result.client_properties);

    // Convert chunk to Arrow array
    duckdb::unordered_map<duckdb::idx_t, const duckdb::shared_ptr<duckdb::ArrowTypeExtensionData>> extension_types;
    duckdb::ArrowConverter::ToArrowArray(chunk, &array, result.client_properties, extension_types);

    auto buffer = SerializeArrowArray(&schema, &array);

    if (array.release) {
        array.release(&array);
    }
    if (schema.release) {
        schema.release(&schema);
    }

    return buffer;
}

std::vector<uint8_t> ArrowSerializer::SerializeArrowSchema(ArrowSchema* schema) {
    // Simple serialization of Arrow schema
    // In a full implementation, this would use Arrow IPC format
    // For now, we serialize the essential information
    
    std::vector<uint8_t> buffer;
    
    // Number of fields
    int64_t n_children = schema->n_children;
    for (int i = 0; i < 8; ++i) {
        buffer.push_back((n_children >> (i * 8)) & 0xFF);
    }
    
    // For each field: name length, name, format length, format
    for (int64_t i = 0; i < n_children; ++i) {
        ArrowSchema* child = schema->children[i];
        
        // Name
        std::string name = child->name ? child->name : "";
        uint32_t name_len = static_cast<uint32_t>(name.size());
        buffer.push_back(name_len & 0xFF);
        buffer.push_back((name_len >> 8) & 0xFF);
        buffer.push_back((name_len >> 16) & 0xFF);
        buffer.push_back((name_len >> 24) & 0xFF);
        buffer.insert(buffer.end(), name.begin(), name.end());
        
        // Format (Arrow type)
        std::string format = child->format ? child->format : "";
        uint32_t format_len = static_cast<uint32_t>(format.size());
        buffer.push_back(format_len & 0xFF);
        buffer.push_back((format_len >> 8) & 0xFF);
        buffer.push_back((format_len >> 16) & 0xFF);
        buffer.push_back((format_len >> 24) & 0xFF);
        buffer.insert(buffer.end(), format.begin(), format.end());
        
        // Nullable flag
        buffer.push_back((child->flags & ARROW_FLAG_NULLABLE) ? 1 : 0);
    }
    
    return buffer;
}

std::vector<uint8_t> ArrowSerializer::SerializeArrowArray(ArrowSchema* schema, ArrowArray* array) {
    // Simple serialization of Arrow array
    // In a full implementation, this would use Arrow IPC format
    // For now, we serialize raw data for basic types
    
    std::vector<uint8_t> buffer;
    
    // Length of the array
    int64_t length = array->length;
    for (int i = 0; i < 8; ++i) {
        buffer.push_back((length >> (i * 8)) & 0xFF);
    }
    
    // Null count
    int64_t null_count = array->null_count;
    for (int i = 0; i < 8; ++i) {
        buffer.push_back((null_count >> (i * 8)) & 0xFF);
    }
    
    // Number of buffers
    int64_t n_buffers = array->n_buffers;
    for (int i = 0; i < 8; ++i) {
        buffer.push_back((n_buffers >> (i * 8)) & 0xFF);
    }
    
    // For each child array, serialize recursively
    int64_t n_children = array->n_children;
    for (int i = 0; i < 8; ++i) {
        buffer.push_back((n_children >> (i * 8)) & 0xFF);
    }
    
    // Serialize each column
    for (int64_t i = 0; i < n_children; ++i) {
        ArrowSchema* child_schema = schema->children[i];
        ArrowArray* child_array = array->children[i];
        
        // Child length
        int64_t child_length = child_array->length;
        for (int j = 0; j < 8; ++j) {
            buffer.push_back((child_length >> (j * 8)) & 0xFF);
        }
        
        // Child null count
        int64_t child_null_count = child_array->null_count;
        for (int j = 0; j < 8; ++j) {
            buffer.push_back((child_null_count >> (j * 8)) & 0xFF);
        }
        
        // Number of buffers for this child
        int64_t child_n_buffers = child_array->n_buffers;
        for (int j = 0; j < 8; ++j) {
            buffer.push_back((child_n_buffers >> (j * 8)) & 0xFF);
        }
        
        // Serialize each buffer
        // Arrow buffer layout (for most types):
        // - buffer[0]: validity bitmap (can be nullptr if no nulls)
        // - buffer[1]: data buffer
        // For variable-length types (strings):
        // - buffer[0]: validity bitmap
        // - buffer[1]: offsets (int32 or int64)
        // - buffer[2]: data

        std::string format = child_schema->format ? child_schema->format : "";

        for (int64_t b = 0; b < child_n_buffers; ++b) {
            const void* buf_ptr = child_array->buffers[b];
            size_t buf_size = 0;

            if (buf_ptr == nullptr) {
                // Null buffer - write 0 size
                for (int j = 0; j < 8; ++j) {
                    buffer.push_back(0);
                }
                continue;
            }

            // Calculate buffer size based on buffer index and type
            if (b == 0) {
                // Buffer 0 is always validity bitmap
                buf_size = (child_length + 7) / 8;
            } else if (format == "l" || format == "L") {
                // INT64/UINT64 - buffer 1 is data
                buf_size = child_length * 8;
            } else if (format == "i" || format == "I") {
                // INT32/UINT32 - buffer 1 is data
                buf_size = child_length * 4;
            } else if (format == "s" || format == "S") {
                // INT16/UINT16 - buffer 1 is data
                buf_size = child_length * 2;
            } else if (format == "c" || format == "C") {
                // INT8/UINT8 - buffer 1 is data
                buf_size = child_length;
            } else if (format == "g") {
                // DOUBLE - buffer 1 is data
                buf_size = child_length * 8;
            } else if (format == "f") {
                // FLOAT - buffer 1 is data
                buf_size = child_length * 4;
            } else if (format == "b") {
                // BOOL - buffer 1 is data (bit-packed)
                buf_size = (child_length + 7) / 8;
            } else if (format == "u" || format == "U") {
                // UTF-8 string (32-bit offsets)
                if (b == 1) {
                    // Offsets buffer: (n+1) * 4 bytes
                    buf_size = (child_length + 1) * 4;
                } else if (b == 2) {
                    // Data buffer: get size from last offset
                    const int32_t* offsets = static_cast<const int32_t*>(child_array->buffers[1]);
                    if (offsets) {
                        buf_size = offsets[child_length];
                    }
                }
            } else if (format == "z" || format == "Z") {
                // Binary (32-bit offsets)
                if (b == 1) {
                    buf_size = (child_length + 1) * 4;
                } else if (b == 2) {
                    const int32_t* offsets = static_cast<const int32_t*>(child_array->buffers[1]);
                    if (offsets) {
                        buf_size = offsets[child_length];
                    }
                }
            }

            // Write buffer size
            for (int j = 0; j < 8; ++j) {
                buffer.push_back((buf_size >> (j * 8)) & 0xFF);
            }

            // Write buffer data
            if (buf_size > 0) {
                const uint8_t* data = static_cast<const uint8_t*>(buf_ptr);
                buffer.insert(buffer.end(), data, data + buf_size);
            }
        }
    }
    
    return buffer;
}

} // namespace duckdb_server
