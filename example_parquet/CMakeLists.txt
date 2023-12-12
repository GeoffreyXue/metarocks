add_executable(parquet_example parquet_example.cc)
target_link_libraries(parquet_example ${ROCKSDB_LIB})