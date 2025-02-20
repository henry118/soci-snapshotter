/*
   Copyright The Soci Snapshotter Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package fs

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/containerd/archive"
	"github.com/containerd/containerd/archive/compression"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"
	"oras.land/oras-go/v2/errdef"
)

type Unpacker interface {
	// Unpack takes care of getting the layer specified by descriptor `desc`,
	// decompressing it, putting it in the directory with the path `mountpoint`
	// and applying the difference to the parent layers if there is any.
	// After that the layer can be mounted as non-remote snapshot.
	Unpack(ctx context.Context, desc ocispec.Descriptor, mountpoint string, mounts []mount.Mount) error
}

type Archive interface {
	// Apply decompresses the compressed stream represented by reader `r` and
	// applies it to the directory `root`.
	Apply(ctx context.Context, root string, r io.Reader, opts ...archive.ApplyOpt) (int64, error)
}

type layerArchive struct {
}

func NewLayerArchive() Archive {
	return &layerArchive{}
}

type readCloserWrapper struct {
	io.Reader
	compression compression.Compression
	closer      func() error
}

func (r *readCloserWrapper) Close() error {
	if r.closer != nil {
		return r.closer()
	}
	return nil
}

func (r *readCloserWrapper) GetCompression() compression.Compression {
	return r.compression
}

func cmdStream(cmd *exec.Cmd, in io.Reader) (io.ReadCloser, error) {
	reader, writer := io.Pipe()

	cmd.Stdin = in
	cmd.Stdout = writer

	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	go func() {
		if err := cmd.Wait(); err != nil {
			writer.CloseWithError(fmt.Errorf("%s: %s", err, errBuf.String()))
		} else {
			writer.Close()
		}
	}()

	return reader, nil
}

func rapidgzipDecompress(ctx context.Context, buf io.Reader) (io.ReadCloser, error) {
	return cmdStream(exec.CommandContext(ctx, "/usr/local/bin/rapidgzip", "-d", "-c"), buf)
}

func rapidGzipStream(archive io.Reader) (compression.DecompressReadCloser, error) {
	buf := bufio.NewReader(archive)
	// _, err := buf.Peek(10)
	// if err != nil && err != io.EOF {
	// 	// Note: we'll ignore any io.EOF error because there are some odd
	// 	// cases where the layer.tar file will be empty (zero bytes) and
	// 	// that results in an io.EOF from the Peek() call. So, in those
	// 	// cases we'll just treat it as a non-compressed stream and
	// 	// that means just create an empty layer.
	// 	// See Issue docker/docker#18170
	// 	return nil, err
	// }
	ctx, cancel := context.WithCancel(context.Background())
	gzReader, err := rapidgzipDecompress(ctx, buf)
	if err != nil {
		cancel()
		return nil, err
	}

	return &readCloserWrapper{
		Reader:      gzReader,
		compression: compression.Gzip,
		closer: func() error {
			cancel()
			return gzReader.Close()
		},
	}, nil
}

func (la *layerArchive) Apply(ctx context.Context, root string, r io.Reader, opts ...archive.ApplyOpt) (int64, error) {
	// we use containerd implementation here
	// decompress first and then apply
	//decompressReader, err := rapidGzipStream(r)
	decompressReader, err := compression.DecompressStream(r)
	if err != nil {
		return 0, fmt.Errorf("cannot decompress the stream: %w", err)
	}
	defer decompressReader.Close()
	return archive.Apply(ctx, root, decompressReader, opts...)
}

type layerUnpacker struct {
	fetcher Fetcher
	archive Archive
}

func NewLayerUnpacker(fetcher Fetcher, archive Archive) Unpacker {
	return &layerUnpacker{
		fetcher: fetcher,
		archive: archive,
	}
}

func (lu *layerUnpacker) Unpack(ctx context.Context, desc ocispec.Descriptor, mountpoint string, mounts []mount.Mount) error {
	var (
		offset int64 = 0
		_100Mb int64 = 100 * 1024 * 1024
		errg   errgroup.Group
	)

	base := "/var/lib/soci-snapshotter-grpc/unpack"
	os.MkdirAll(base, 0611)
	tmpfile := filepath.Join(base, fmt.Sprintf("%s-%d", desc.Digest.String(), time.Nanosecond))

	w, err := os.OpenFile(tmpfile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("cannot create temporary file: %w", err)
	}
	w.Truncate(desc.Size)
	w.Close()

	for offset = 0; offset < desc.Size; offset += _100Mb {
		offset := offset
		errg.Go(func() error {
			rc, _, ferr := lu.fetcher.Fetch(ctx, desc)
			if ferr != nil {
				return fmt.Errorf("cannot fetch layer: %w", ferr)
			}
			defer rc.Close()

			fw, ferr := os.OpenFile(tmpfile, os.O_RDWR, 0644)
			if ferr != nil {
				return fmt.Errorf("cannot create temporary file: %w", ferr)
			}
			defer fw.Close()

			rs, ok := rc.(io.ReadSeekCloser)
			if !ok {
				io.Copy(fw, rc)
				return nil
			}
			rs.Seek(offset, io.SeekCurrent)
			fw.Seek(offset, io.SeekCurrent)
			size := min(_100Mb, desc.Size-offset)
			if n, _ := io.CopyN(fw, rc, size); int64(n) != size {
				return fmt.Errorf("failed to read/write n (%d) != expected (%d)", n, size)
			}
			return nil
		})
	}
	if err := errg.Wait(); err != nil {
		return err
	}

	// if !local {
	// again:
	// 	err := lu.fetcher.Store(ctx, desc, rc)
	// 	if err != nil {
	// 		if errdefs.IsAlreadyExists(err) || errors.Is(err, errdef.ErrAlreadyExists) {
	// 			rc.Close()
	// 		} else if errdefs.IsUnavailable(err) {
	// 			goto again
	// 		} else {
	// 			rc.Close()
	// 			return fmt.Errorf("cannot store layer: %w", err)
	// 		}
	// 	}
	// 	rc.Close()
	// 	rc, _, err = lu.fetcher.Fetch(ctx, desc)
	// 	if err != nil {
	// 		return fmt.Errorf("cannot fetch layer: %w", err)
	// 	}
	// }
	// defer rc.Close()

	var eg errgroup.Group

	eg.Go(func() error {
		f, err := os.Open(tmpfile)
		if err != nil {
			return err
		}
		defer f.Close()
	again:
		err = lu.fetcher.Store(ctx, desc, f)
		if err != nil {
			if errdefs.IsAlreadyExists(err) || errors.Is(err, errdef.ErrAlreadyExists) {
			} else if errdefs.IsUnavailable(err) {
				goto again
			} else {
				return fmt.Errorf("cannot store layer: %w", err)
			}
		}
		return nil
	})

	eg.Go(func() error {
		f, err := os.Open(tmpfile)
		if err != nil {
			return err
		}
		defer f.Close()
		var parents []string
		if len(mounts) > 0 {
			parents, err = getLayerParents(mounts[0].Options)
		}
		if err != nil {
			return fmt.Errorf("cannot get layer parents: %w", err)
		}
		opts := []archive.ApplyOpt{
			archive.WithConvertWhiteout(archive.OverlayConvertWhiteout),
		}
		if len(parents) > 0 {
			opts = append(opts, archive.WithParents(parents))
		}
		_, err = lu.archive.Apply(ctx, mountpoint, f, opts...)
		if err != nil {
			return fmt.Errorf("cannot apply layer: %w", err)
		}
		return nil
	})

	return eg.Wait()
}

func getLayerParents(options []string) (lower []string, err error) {
	const lowerdirPrefix = "lowerdir="

	for _, o := range options {
		if strings.HasPrefix(o, lowerdirPrefix) {
			lower = strings.Split(strings.TrimPrefix(o, lowerdirPrefix), ":")
		}
	}
	return
}
