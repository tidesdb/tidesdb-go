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

// The S3 object store connector is only available when the TidesDB C library was
// built with TIDESDB_WITH_S3=ON. Because the tidesdb_objstore_s3_* symbols are
// otherwise unresolved at link time, these bindings are gated behind the
// "tidesdb_s3" build tag. Build or test with: go build -tags tidesdb_s3 ./...
package tidesdb_go

/*
#cgo LDFLAGS: -ltidesdb
#include <tidesdb/db.h>
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// S3Config is the full configuration for an S3-compatible object store connector,
// including TLS and multipart tuning that the positional ObjStoreS3Create cannot
// express. The zero value is secure (TLS verification on, no custom CA) and uses
// the library's built-in multipart sizes.
type S3Config struct {
	Endpoint              string // S3 endpoint (required), e.g. "s3.amazonaws.com" or "minio.local:9000"
	Bucket                string // bucket name (required)
	Prefix                string // key prefix (e.g. "production/db1/"), may be empty
	AccessKey             string // AWS access key ID (required)
	SecretKey             string // AWS secret access key (required)
	Region                string // AWS region (e.g. "us-east-1"), empty for the default / MinIO
	UseSSL                bool   // true for HTTPS, false for HTTP
	UsePathStyle          bool   // true for path-style URLs (MinIO), false for virtual-hosted (AWS)
	TLSCAPath             string // custom CA bundle file path, empty for the system bundle
	TLSInsecureSkipVerify bool   // true disables TLS peer+host verification (test only, insecure)
	MultipartThreshold    uint64 // object size at/above which multipart upload is used; 0 = default
	MultipartPartSize     uint64 // multipart chunk size in bytes; 0 = default
}

// ObjStoreS3Create creates an S3-compatible object store connector (AWS S3, MinIO, etc.)
// using positional parameters with secure defaults. For TLS and multipart tuning use
// ObjStoreS3CreateConfig.
//
// The TidesDB C library must have been built with TIDESDB_WITH_S3=ON; otherwise this
// package will not link. Build with the "tidesdb_s3" tag to enable these bindings.
func ObjStoreS3Create(endpoint, bucket, prefix, accessKey, secretKey, region string, useSSL, usePathStyle bool) (*ObjStore, error) {
	cEndpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cEndpoint))
	cBucket := C.CString(bucket)
	defer C.free(unsafe.Pointer(cBucket))
	cAccessKey := C.CString(accessKey)
	defer C.free(unsafe.Pointer(cAccessKey))
	cSecretKey := C.CString(secretKey)
	defer C.free(unsafe.Pointer(cSecretKey))

	var cPrefix *C.char
	if prefix != "" {
		cPrefix = C.CString(prefix)
		defer C.free(unsafe.Pointer(cPrefix))
	}
	var cRegion *C.char
	if region != "" {
		cRegion = C.CString(region)
		defer C.free(unsafe.Pointer(cRegion))
	}

	store := C.tidesdb_objstore_s3_create(cEndpoint, cBucket, cPrefix, cAccessKey, cSecretKey,
		cRegion, boolToInt(useSSL), boolToInt(usePathStyle))
	if store == nil {
		return nil, fmt.Errorf("failed to create S3 object store for bucket %q at %s", bucket, endpoint)
	}

	return &ObjStore{store: store}, nil
}

// ObjStoreS3CreateConfig creates an S3-compatible connector from a full configuration
// struct (TLS + multipart). ObjStoreS3Create is a thin wrapper over this with
// secure/default settings.
//
// The TidesDB C library must have been built with TIDESDB_WITH_S3=ON; otherwise this
// package will not link. Build with the "tidesdb_s3" tag to enable these bindings.
func ObjStoreS3CreateConfig(config S3Config) (*ObjStore, error) {
	cEndpoint := C.CString(config.Endpoint)
	defer C.free(unsafe.Pointer(cEndpoint))
	cBucket := C.CString(config.Bucket)
	defer C.free(unsafe.Pointer(cBucket))
	cAccessKey := C.CString(config.AccessKey)
	defer C.free(unsafe.Pointer(cAccessKey))
	cSecretKey := C.CString(config.SecretKey)
	defer C.free(unsafe.Pointer(cSecretKey))

	var cPrefix, cRegion, cTLSCAPath *C.char
	if config.Prefix != "" {
		cPrefix = C.CString(config.Prefix)
		defer C.free(unsafe.Pointer(cPrefix))
	}
	if config.Region != "" {
		cRegion = C.CString(config.Region)
		defer C.free(unsafe.Pointer(cRegion))
	}
	if config.TLSCAPath != "" {
		cTLSCAPath = C.CString(config.TLSCAPath)
		defer C.free(unsafe.Pointer(cTLSCAPath))
	}

	cConfig := C.tidesdb_objstore_s3_config_t{
		endpoint:                 cEndpoint,
		bucket:                   cBucket,
		prefix:                   cPrefix,
		access_key:               cAccessKey,
		secret_key:               cSecretKey,
		region:                   cRegion,
		use_ssl:                  boolToInt(config.UseSSL),
		use_path_style:           boolToInt(config.UsePathStyle),
		tls_ca_path:              cTLSCAPath,
		tls_insecure_skip_verify: boolToInt(config.TLSInsecureSkipVerify),
		multipart_threshold:      C.size_t(config.MultipartThreshold),
		multipart_part_size:      C.size_t(config.MultipartPartSize),
	}

	store := C.tidesdb_objstore_s3_create_config(&cConfig)
	if store == nil {
		return nil, fmt.Errorf("failed to create S3 object store for bucket %q at %s", config.Bucket, config.Endpoint)
	}

	return &ObjStore{store: store}, nil
}
