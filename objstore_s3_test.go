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

//go:build tidesdb_s3

// These tests are compiled and run only with the "tidesdb_s3" build tag against a
// TidesDB C library built with TIDESDB_WITH_S3=ON. Point them at a reachable
// S3-compatible endpoint (e.g. a local MinIO) via the TIDESDB_S3_* env vars, or
// they are skipped. Run with: go test -tags tidesdb_s3 -run TestObjStoreS3 ./...
package tidesdb_go

import (
	"os"
	"testing"
)

func TestObjStoreS3Create(t *testing.T) {
	endpoint := os.Getenv("TIDESDB_S3_ENDPOINT")
	bucket := os.Getenv("TIDESDB_S3_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("set TIDESDB_S3_ENDPOINT and TIDESDB_S3_BUCKET to run the S3 connector test")
	}

	store, err := ObjStoreS3Create(
		endpoint,
		bucket,
		os.Getenv("TIDESDB_S3_PREFIX"),
		os.Getenv("TIDESDB_S3_ACCESS_KEY"),
		os.Getenv("TIDESDB_S3_SECRET_KEY"),
		os.Getenv("TIDESDB_S3_REGION"),
		os.Getenv("TIDESDB_S3_USE_SSL") == "1",
		os.Getenv("TIDESDB_S3_USE_PATH_STYLE") == "1",
	)
	if err != nil {
		t.Fatalf("ObjStoreS3Create failed: %v", err)
	}
	if store == nil || store.store == nil {
		t.Fatalf("Expected a non-nil S3 connector handle")
	}
	t.Logf("Created S3 connector for bucket %q at %s", bucket, endpoint)
}

func TestObjStoreS3CreateConfig(t *testing.T) {
	endpoint := os.Getenv("TIDESDB_S3_ENDPOINT")
	bucket := os.Getenv("TIDESDB_S3_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("set TIDESDB_S3_ENDPOINT and TIDESDB_S3_BUCKET to run the S3 connector test")
	}

	cfg := S3Config{
		Endpoint:     endpoint,
		Bucket:       bucket,
		Prefix:       os.Getenv("TIDESDB_S3_PREFIX"),
		AccessKey:    os.Getenv("TIDESDB_S3_ACCESS_KEY"),
		SecretKey:    os.Getenv("TIDESDB_S3_SECRET_KEY"),
		Region:       os.Getenv("TIDESDB_S3_REGION"),
		UseSSL:       os.Getenv("TIDESDB_S3_USE_SSL") == "1",
		UsePathStyle: os.Getenv("TIDESDB_S3_USE_PATH_STYLE") == "1",
	}

	store, err := ObjStoreS3CreateConfig(cfg)
	if err != nil {
		t.Fatalf("ObjStoreS3CreateConfig failed: %v", err)
	}
	if store == nil || store.store == nil {
		t.Fatalf("Expected a non-nil S3 connector handle")
	}
	t.Logf("Created S3 connector (config form) for bucket %q at %s", bucket, endpoint)
}
