package main

import (
	"esdump/cmds"
	"testing"
)

func Test_Export(t *testing.T){
	cmds.ExportData(cmds.Output,"http://brige:9200","tmp_index")
}
