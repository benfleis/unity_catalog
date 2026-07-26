#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

// uc_read_deletion_vector(path [, content_offset => <b>, content_size => <b>]) -> table(pos BIGINT)
//
// Decodes an Iceberg deletion-vector into the absolute row positions it marks deleted:
//   - with content_offset+content_size: a bare `deletion-vector-v1` blob at that byte range (the
//     shape UC's scan-plan response points at; UCDeletionVectorData::FromBlob).
//   - without them: the whole file, read as a puffin container (or a lone bare blob) via
//     UCPuffinReader — every deletion-vector blob it holds is decoded and unioned.
//
// Exposes the same decode BuildUCDeleteFilter applies, so the delete path is inspectable and
// testable from plain SQL without a live scan-plan server.
//
// Public utility (not `__internal_`): intended to become a general, non-UC-specific
// deletion-vector reader (a shared-lib candidate); it lives in this extension for now.
class UCReadDeletionVectorFunction : public TableFunction {
public:
	UCReadDeletionVectorFunction();
};

} // namespace duckdb
