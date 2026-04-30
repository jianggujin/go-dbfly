package dbfly

import (
	"embed"
	"io"
	"io/fs"
	"os"
)

type SourceInfo struct {
	// 文件路径
	Path string
}

// Source SQL源信息
type Source interface {
	// Read 读取源中指定路径的文件内容
	Read(string) ([]byte, error)
}

// FSSource 嵌入文件系统源实现
type FSSource struct {
	fsys []fs.FS
}

func NewFSSource(fsys ...fs.FS) *FSSource {
	return &FSSource{
		fsys: fsys,
	}
}

func (s *FSSource) Read(path string) ([]byte, error) {
	var errs Error
	for i, sys := range s.fsys {
		file, err := sys.Open(path)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		content, readErr := io.ReadAll(file)
		file.Close()
		if readErr == nil {
			return content, nil
		}
		errs = append(errs, Wrap(readErr, "read from fs[%d] failed", i))
	}
	return nil, Wrap(errs, "read source %s failed", path)
}

func NewEmbedFSSource(efs ...embed.FS) *FSSource {
	fsys := make([]fs.FS, len(efs))
	for i, e := range efs {
		fsys[i] = e
	}
	return NewFSSource(fsys...)
}

func NewLocalFSSource(dirs ...string) *FSSource {
	fsys := make([]fs.FS, len(dirs))
	for i, dir := range dirs {
		fsys[i] = os.DirFS(dir)
	}
	return NewFSSource(fsys...)
}
