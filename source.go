package dbfly

import (
	"embed"
	"github.com/hashicorp/go-version"
	"os"
	"path/filepath"
	"strings"
)

type SourceInfo struct {
	Version *version.Version
	Uid     string
}

// SQL源信息
type Source interface {
	// 扫描源中包含的需要合并的文件信息
	Scan() ([]*SourceInfo, error)
	// 读取源中指定uid的文件内容
	Read(string) ([]byte, error)
}

// 嵌入文件系统源实现
type EmbedFSSource struct {
	Fs    embed.FS
	Paths []string
}

func (s *EmbedFSSource) Scan() ([]*SourceInfo, error) {
	var infos []*SourceInfo
	for _, path := range s.Paths {
		files, err := s.Fs.ReadDir(path)
		if err != nil {
			// 未找到，则表明不需要升级
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".xml") {
				continue
			}
			v, err := version.NewVersion(strings.TrimSuffix(f.Name(), ".xml"))
			if err != nil {
				return nil, err
			}
			infos = append(infos, &SourceInfo{
				Version: v,
				Uid:     path + "/" + f.Name(),
			})
		}
	}
	return infos, nil
}

func (s *EmbedFSSource) Read(uid string) ([]byte, error) {
	return s.Fs.ReadFile(uid)
}

// 文件系统源实现
type FSSource struct {
	Paths []string
}

func (s *FSSource) Scan() ([]*SourceInfo, error) {
	var infos []*SourceInfo
	for _, path := range s.Paths {
		files, err := os.ReadDir(path)
		if err != nil {
			// 未找到，则表明不需要升级
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".xml") {
				continue
			}
			v, err := version.NewVersion(strings.TrimSuffix(f.Name(), ".xml"))
			if err != nil {
				return nil, err
			}
			infos = append(infos, &SourceInfo{
				Version: v,
				Uid:     filepath.Join(path, f.Name()),
			})
		}
	}
	return infos, nil
}

func (s *FSSource) Read(uid string) ([]byte, error) {
	return os.ReadFile(uid)
}
