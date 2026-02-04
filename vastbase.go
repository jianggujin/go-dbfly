package dbfly

// VastbaseMigratory VastBase合并实现
type VastbaseMigratory struct {
	Migratory
}

// NewVastbaseMigratory 创建一个VastBase合并实现实例
func NewVastbaseMigratory() Migratory {
	return &VastbaseMigratory{
		Migratory: NewPostgresMigratory(),
	}
}
