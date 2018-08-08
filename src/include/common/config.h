/**
 * config.h
 *
 * Database system configuration
 */

#pragma once

#include <atomic>
#include <chrono>
#pragma once

#include <atomic>
#include <chrono>
extern std::chrono::duration<long long int> LOG_TIMEOUT;

extern std::atomic<bool> ENABLE_LOGGING;

#include <cstdint>

#define INVALID_LSN -1     // representing an invalid lsn
namespace cmudb {

extern std::chrono::duration<long long int> LOG_TIMEOUT;

extern std::atomic<bool> ENABLE_LOGGING;

#define INVALID_PAGE_ID -1 // representing an invalid page id
#define INVALID_TXN_ID -1  // representing an invalid txn id
#define INVALID_LSN -1     // representing an invalid lsn
#define HEADER_PAGE_ID 0   // the header page id
#define PAGE_SIZE 512     // size of a data page in byte
#define LOG_BUFFER_SIZE                                                            \
  ((BUFFER_POOL_SIZE + 1) * PAGE_SIZE) // size of a log buffer in byte
#define BUCKET_SIZE 50                 // size of extendible hash bucket
#define BUFFER_POOL_SIZE 10            // size of buffer pool

//Helper defs
typedef int32_t lsn_t;     // log sequence number type
typedef int32_t lsn_t;     // log sequence number type
#define INVALID_INDEX -1
#define NO_PARENT -1 

typedef int32_t page_id_t; // page id type
typedef int32_t txn_id_t;  // transaction id type

} // namespace cmudb