add_executable(remote-compactor${ARTIFACT_SUFFIX} remote-compactor.cc)
target_link_libraries(remote-compactor${ARTIFACT_SUFFIX}
  ${ROCKSDB_LIB} ${THIRDPARTY_LIBS})
list(APPEND example_deps remote-compactor)
