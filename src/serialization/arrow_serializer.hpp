//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// serialization/arrow_serializer.hpp
//
// Arrow IPC serialization
//===----------------------------------------------------------------------===//

#pragma once

#include "common.hpp"
#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow.hpp"

namespace duckdb_server {

struct SerializerConfig {
    size_t batch_size = DEFAULT_ARROW_BATCH_SIZE;
    bool enable_compression = false;
    bool enable_dictionary = true;
};

class ArrowSerializer {
public:
    explicit ArrowSerializer(const SerializerConfig& config = SerializerConfig());
    ~ArrowSerializer();
    
    // Serialize schema to Arrow IPC format
    std::vector<uint8_t> SerializeSchema(const duckdb::vector<duckdb::LogicalType>& types,
                                          const duckdb::vector<duckdb::string>& names);
    
    // Serialize a DataChunk to Arrow IPC format
    std::vector<uint8_t> SerializeChunk(duckdb::DataChunk& chunk,
                                         const duckdb::vector<duckdb::LogicalType>& types,
                                         const duckdb::vector<duckdb::string>& names);
    
    // Serialize query result header (schema)
    std::vector<uint8_t> SerializeResultHeader(duckdb::QueryResult& result);
    
    // Serialize a single data batch from query result
    std::vector<uint8_t> SerializeResultBatch(duckdb::DataChunk& chunk,
                                               duckdb::QueryResult& result);
    
    // Get configuration
    const SerializerConfig& GetConfig() const { return config_; }

private:
    // Serialize Arrow schema to IPC format
    std::vector<uint8_t> SerializeArrowSchema(ArrowSchema* schema);
    
    // Serialize Arrow array to IPC format
    std::vector<uint8_t> SerializeArrowArray(ArrowSchema* schema, ArrowArray* array);

private:
    SerializerConfig config_;
    duckdb::ClientProperties client_props_;
};

} // namespace duckdb_server
