package dbfly

import "testing"

func TestDryRunDriver_Query(t *testing.T) {
	driver := NewDryRunDriver()
	driver.Query("select * from ddd where dd = 11", 900, 888)
}
