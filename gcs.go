package barkup

import (
	"bufio"
	"context"
	"io"
	"os"

	"cloud.google.com/go/storage"
)

// Allow for GCS exporter
type GCS struct {
	ProjectID  string
	BucketName string
}

// Store stores the result in the given directory path for the container specified in the caller
func (o *GCS) Store(result *ExportResult, directory string) *Error {
	if result.Error != nil {
		return result.Error
	}
	ctx := context.Background()
	file, err := os.Open(result.Path)
	if err != nil {
		return makeErr(err, "")
	}
	defer file.Close()

	buffy := bufio.NewReader(file)

	_, _, err = upload(ctx, buffy, o.ProjectID, o.BucketName, result.Filename(), false)
	return makeErr(err, "")
}

func upload(ctx context.Context, r io.Reader, projectID, bucket, name string, public bool) (*storage.ObjectHandle, *storage.ObjectAttrs, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, nil, err
	}

	bh := client.Bucket(bucket)
	// Next check if the bucket exists
	if _, err = bh.Attrs(ctx); err != nil {
		return nil, nil, err
	}

	obj := bh.Object(name)
	w := obj.NewWriter(ctx)
	if _, err := io.Copy(w, r); err != nil {
		return nil, nil, err
	}
	if err := w.Close(); err != nil {
		return nil, nil, err
	}

	if public {
		if err := obj.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
			return nil, nil, err
		}
	}

	attrs, err := obj.Attrs(ctx)
	return obj, attrs, err
}
