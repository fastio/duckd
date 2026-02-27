//===----------------------------------------------------------------------===//
//                         DuckD Client Extension
//
// src/client/duckd-client/duckd_catalog.hpp
//
// DuckDB Catalog, Schema, and Table entry implementations for remote duckd
//===----------------------------------------------------------------------===//

#pragma once

#include "flight_client.hpp"

#include <arrow/api.h>
#include <arrow/type.h>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace duckdb {

//===----------------------------------------------------------------------===//
// DuckdTransaction  –  DuckDB transaction that maps to a Flight SQL transaction
//===----------------------------------------------------------------------===//

class DuckdTransaction : public Transaction {
public:
    DuckdTransaction(TransactionManager &manager, ClientContext &context,
                     arrow::flight::sql::Transaction flight_txn)
        : Transaction(manager, context)
        , flight_txn_(std::move(flight_txn)) {}
    ~DuckdTransaction() override = default;

    // True if this transaction has an active server-side Flight SQL transaction.
    bool HasFlightTxn() const { return flight_txn_.is_valid(); }

    // The underlying Flight SQL transaction handle (only valid if HasFlightTxn()).
    const arrow::flight::sql::Transaction &FlightTxn() const { return flight_txn_; }

private:
    arrow::flight::sql::Transaction flight_txn_;
};

//===----------------------------------------------------------------------===//
// DuckdTransactionManager  –  maps DuckDB transactions to Flight SQL transactions
//===----------------------------------------------------------------------===//

class DuckdTransactionManager : public TransactionManager {
public:
    DuckdTransactionManager(AttachedDatabase &db,
                             std::shared_ptr<duckdb_client::DuckdFlightClient> client)
        : TransactionManager(db), client_(std::move(client)) {}

    Transaction &StartTransaction(ClientContext &context) override;
    ErrorData   CommitTransaction(ClientContext &context, Transaction &transaction) override;
    void        RollbackTransaction(Transaction &transaction) override;
    void        Checkpoint(ClientContext &context, bool force = false) override {}

private:
    std::shared_ptr<duckdb_client::DuckdFlightClient>              client_;
    std::mutex                                                     mutex_;
    // Keyed by raw Transaction* for O(1) Commit/Rollback lookup.
    std::unordered_map<Transaction *, unique_ptr<DuckdTransaction>> active_transactions_;
};

//===----------------------------------------------------------------------===//
// DuckdTableEntry  –  TableCatalogEntry for a remote duckd table
//===----------------------------------------------------------------------===//

struct DuckdScanBindData : public TableFunctionData {
    string url;
    string schema_name;
    string table_name;
    vector<string>      column_names; // all columns
    vector<LogicalType> column_types; // all column types (parallel to column_names)
    std::shared_ptr<duckdb_client::DuckdFlightClient> client;
    Catalog *catalog = nullptr;       // non-owning; for active-transaction lookup

    // LIMIT pushdown: if > 0, DuckdScanInitGlobal appends "LIMIT N" to the SQL.
    // Set by the DuckdLimitPushdown optimizer extension when a constant LIMIT
    // (with no offset) wraps this scan in the logical plan.
    idx_t limit_val = 0;
};

class DuckdTableEntry : public TableCatalogEntry {
public:
    DuckdTableEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                    CreateTableInfo &info, string url, string schema_name,
                    std::shared_ptr<duckdb_client::DuckdFlightClient> client);

    // TableCatalogEntry interface
    DataTable &GetStorage() override;
    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
    TableFunction GetScanFunction(ClientContext &context,
                                  unique_ptr<FunctionData> &bind_data) override;
    TableStorageInfo GetStorageInfo(ClientContext &context) override;
    unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;

    // Estimated row count (set during PopulateTableCache, 0 = unknown).
    idx_t estimated_cardinality = 0;

private:
    string url_;
    string schema_name_;
    std::shared_ptr<duckdb_client::DuckdFlightClient> client_;
};

//===----------------------------------------------------------------------===//
// DuckdSchemaEntry  –  SchemaCatalogEntry for a remote duckd schema
//===----------------------------------------------------------------------===//

class DuckdSchemaEntry : public SchemaCatalogEntry {
public:
    DuckdSchemaEntry(Catalog &catalog, CreateSchemaInfo &info,
                     string url,
                     std::shared_ptr<duckdb_client::DuckdFlightClient> client);

    // SchemaCatalogEntry pure virtual interface
    optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction,
                                           BoundCreateTableInfo &info) override;
    optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction,
                                              CreateFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction,
                                           CreateIndexInfo &info,
                                           TableCatalogEntry &table) override;
    optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction,
                                          CreateViewInfo &info) override;
    optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction,
                                              CreateSequenceInfo &info) override;
    optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
                                                   CreateTableFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
                                                  CreateCopyFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
                                                    CreatePragmaFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction,
                                               CreateCollationInfo &info) override;
    optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction,
                                          CreateTypeInfo &info) override;

    optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction,
                                           const EntryLookupInfo &lookup_info) override;

    void Scan(ClientContext &context, CatalogType type,
              const std::function<void(CatalogEntry &)> &callback) override;
    void Scan(CatalogType type,
              const std::function<void(CatalogEntry &)> &callback) override;

    void DropEntry(ClientContext &context, DropInfo &info) override;
    void Alter(CatalogTransaction transaction, AlterInfo &info) override;

private:
    string                                                 url_;
    std::shared_ptr<duckdb_client::DuckdFlightClient>      client_;

    std::mutex                                             entry_mutex_;
    std::unordered_map<string, unique_ptr<DuckdTableEntry>> table_cache_;

    optional_ptr<DuckdTableEntry> GetOrFetchTable(const string &table_name);
    void PopulateTableCache(); // fills table_cache_ from remote listing
    bool cache_populated_ = false;

    // Table cache TTL: 60 s.  After expiry the next Scan/lookup triggers a
    // fresh information_schema.columns query so remote schema changes become
    // visible without requiring DETACH + ATTACH.
    static constexpr std::chrono::seconds kTableCacheTTL{60};
    std::chrono::steady_clock::time_point cache_populated_at_{};
};

//===----------------------------------------------------------------------===//
// DuckdCatalog  –  Catalog implementation backed by a remote duckd server
//===----------------------------------------------------------------------===//

class DuckdCatalog : public Catalog {
public:
    DuckdCatalog(AttachedDatabase &db, string url);

    // Catalog pure virtual interface
    void   Initialize(bool load_builtin) override;
    string GetCatalogType() override { return "duckd"; }

    optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction,
                                            CreateSchemaInfo &info) override;
    void ScanSchemas(ClientContext &context,
                     std::function<void(SchemaCatalogEntry &)> callback) override;
    optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction,
                                                   const EntryLookupInfo &schema_lookup,
                                                   OnEntryNotFound if_not_found) override;

    PhysicalOperator &PlanCreateTableAs(ClientContext &context,
                                        PhysicalPlanGenerator &planner,
                                        LogicalCreateTable &op,
                                        PhysicalOperator &plan) override;
    PhysicalOperator &PlanInsert(ClientContext &context,
                                  PhysicalPlanGenerator &planner,
                                  LogicalInsert &op,
                                  optional_ptr<PhysicalOperator> plan) override;
    PhysicalOperator &PlanDelete(ClientContext &context,
                                  PhysicalPlanGenerator &planner,
                                  LogicalDelete &op,
                                  PhysicalOperator &plan) override;
    PhysicalOperator &PlanUpdate(ClientContext &context,
                                  PhysicalPlanGenerator &planner,
                                  LogicalUpdate &op,
                                  PhysicalOperator &plan) override;

    DatabaseSize GetDatabaseSize(ClientContext &context) override;
    bool   InMemory() override { return false; }
    string GetDBPath() override { return url_; }
    void   DropSchema(ClientContext &context, DropInfo &info) override;

    // Accessors
    const string &GetURL() const { return url_; }
    duckdb_client::DuckdFlightClient &GetClient() { return *client_; }
    std::shared_ptr<duckdb_client::DuckdFlightClient> GetSharedClient() const { return client_; }

private:
    string                                                    url_;
    std::shared_ptr<duckdb_client::DuckdFlightClient>         client_;

    std::mutex                                                schema_mutex_;
    std::unordered_map<string, unique_ptr<DuckdSchemaEntry>>  schema_cache_;

    // Schema cache TTL: 300 s.  Schemas change less often than tables.
    static constexpr std::chrono::seconds kSchemaCacheTTL{300};
    std::chrono::steady_clock::time_point schema_cache_populated_at_{};
    bool schema_cache_populated_ = false;

    optional_ptr<DuckdSchemaEntry> GetOrCreateSchema(const string &schema_name);
};

//===----------------------------------------------------------------------===//
// Arrow ↔ DuckDB conversion utilities (shared with duckd_client_extension)
//===----------------------------------------------------------------------===//

LogicalType ArrowToDuckDBType(const arrow::DataType &type);
void FillColumnFromArrow(Vector &dst, const arrow::Array &src,
                         int64_t src_offset, idx_t count);

} // namespace duckdb
