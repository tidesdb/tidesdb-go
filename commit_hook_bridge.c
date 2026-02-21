// Package tidesdb_go
// Copyright (C) TidesDB
//
// Original Author: Alex Gaetano Padula
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	https://www.mozilla.org/en-US/MPL/2.0/
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include <tidesdb/db.h>
#include "_cgo_export.h"

int commit_hook_bridge(const tidesdb_commit_op_t *ops, int num_ops,
                       uint64_t commit_seq, void *ctx)
{
    return goCommitHookCallback((tidesdb_commit_op_t *)ops, num_ops, commit_seq, ctx);
}

int set_commit_hook_bridge(tidesdb_column_family_t *cf, void *ctx)
{
    return tidesdb_cf_set_commit_hook(cf, (tidesdb_commit_hook_fn)commit_hook_bridge, ctx);
}
