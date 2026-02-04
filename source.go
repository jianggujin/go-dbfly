package dbfly

import (
	"embed"
	"errors"
	"fmt"
	"github.com/hashicorp/go-version"
	"os"
	"strings"
)

type SourceInfo struct {
	// 版本
	Version *version.Version
	// 唯一编号
	Uid string
	// 是否为纯脚本
	Script bool
}

// Source SQL源信息
type Source interface {
	// Scan 扫描源中包含的需要合并的文件信息
	Scan() ([]*SourceInfo, error)
	// Read 读取源中指定uid的文件内容
	Read(string) ([]byte, error)
}

// EmbedFSSource 嵌入文件系统源实现
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
			if f.IsDir() {
				continue
			}
			verStr := ""
			script := false
			if strings.HasSuffix(f.Name(), ".xml") {
				verStr = strings.TrimSuffix(f.Name(), ".xml")
			} else if strings.HasSuffix(f.Name(), ".sql") {
				verStr = strings.TrimSuffix(f.Name(), ".sql")
				script = true
			}
			if verStr == "" {
				continue
			}
			v, err := version.NewVersion(verStr)
			if err != nil {
				return nil, err
			}
			infos = append(infos, &SourceInfo{
				Version: v,
				Uid:     path + "/" + f.Name(),
				Script:  script,
			})
		}
	}
	return infos, nil
}

func (s *EmbedFSSource) Read(uid string) ([]byte, error) {
	return s.Fs.ReadFile(uid)
}

// FSSource 文件系统源实现
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
			if f.IsDir() {
				continue
			}
			verStr := ""
			script := false
			if strings.HasSuffix(f.Name(), ".xml") {
				verStr = strings.TrimSuffix(f.Name(), ".xml")
			} else if strings.HasSuffix(f.Name(), ".sql") {
				verStr = strings.TrimSuffix(f.Name(), ".sql")
				script = true
			}
			if verStr == "" {
				continue
			}
			v, err := version.NewVersion(verStr)
			if err != nil {
				return nil, err
			}
			infos = append(infos, &SourceInfo{
				Version: v,
				Uid:     path + "/" + f.Name(),
				Script:  script,
			})
		}
	}
	return infos, nil
}

func (s *FSSource) Read(uid string) ([]byte, error) {
	return os.ReadFile(uid)
}

// EmbedSource 嵌入源实现
type EmbedSource struct {
	Sources map[string]*EmbedSourceInfo
}

type EmbedSourceInfo struct {
	// 是否为纯脚本
	Script bool
	// 内容
	Content []byte
}

func (s *EmbedSource) Scan() ([]*SourceInfo, error) {
	var infos []*SourceInfo
	if s.Sources == nil {
		return []*SourceInfo{}, nil
	}
	for vers, source := range s.Sources {
		ver, err := version.NewVersion(vers)
		if err != nil {
			return nil, err
		}
		infos = append(infos, &SourceInfo{
			Version: ver,
			Uid:     vers,
			Script:  source.Script,
		})
	}
	return infos, nil
}

func (s *EmbedSource) Read(uid string) ([]byte, error) {
	source, ok := s.Sources[uid]
	if !ok {
		return nil, errors.New(fmt.Sprintf("source [%s] not exists", uid))
	}
	return source.Content, nil
}
