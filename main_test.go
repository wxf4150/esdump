package main

import (
	"compress/gzip"
	"esdump/cmds"
	//"io/ioutil"
	"log"
	"os"
	"testing"
)

func Test_Gzip(t *testing.T){
	log.Println(string([]byte("a\r\nb")))
	return
	infile,err:=os.Open("/home/wxf/.ssh/config")
	if err != nil {
		log.Fatalln(err)
	}
	_,err1:=gzip.NewReader(infile)
	t.Log("gzip new reader",err1)
	//ioutil.ReadAll(greader)

}
func Test_Export(t *testing.T){
	cmds.ExportData(cmds.Output,"http://brige:9200","tmp_index","")
}
