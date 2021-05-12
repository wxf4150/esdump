package cmds

import (
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/olivere/elastic/v7"
	"github.com/spf13/cobra"
	"io"
	"log"
	"math"
	"os"
	"time"
)
var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "elasticsearch export",
	Long:  `elasticsearch export`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Printf("export index %s to %s",IndexName,Output)
		ExportData(Output,EsUrl,IndexName,MatchBody)
	},
}

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "elasticsearch import",
	Long:  `elasticsearch import`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Printf("import index %s from %s",IndexName,Input)
		err:=ImportData(Input,EsUrl,IndexName)
		if err != nil {
			log.Println(err)
		}
	},
}
var Output string
var Input string
var MatchBody string
func init(){
	exportCmd.Flags().StringVarP(&Output,"o","o","./tmp_export.json.gz","export dest filename; use - for stdout")
	exportCmd.Flags().StringVarP(&MatchBody,"MatchBody","m","{\"match_all\":{}}","MatchBody, empty for match_all; example:{\"range\": {\"timestamp\": {\"gte\": \"2021-04-20\"}}}")

	importCmd.Flags().StringVarP(&Input,"i","i","./tmp_import.json.gz","import filename; use - for stdin")
	//importCmd.MarkFlagRequired("i")
	//exportCmd.MarkFlagRequired("o")

	RootCmd.AddCommand(exportCmd)
	RootCmd.AddCommand(importCmd)
}

func ImportData(inputFile ,esUrl,indexName string)(err error){
	var infile *os.File
	if inputFile=="-"{
		infile=os.Stdin
	}else{
		infile,err=os.Open(inputFile)
	}
	if err != nil {
		log.Fatal("open inputFile",inputFile,err)
	}

	defer infile.Close()
	zipReader,err1 := gzip.NewReader(infile)
	if err1 != nil {
		return err1
	}
	defer zipReader.Close()
	if err1 != nil {
		return err1
	}
	iserv:=GetEsIndexService(esUrl,indexName)

	bsLen:=[4]byte{}
	_,err=io.ReadFull(zipReader,bsLen[:])
	if err!=nil{
		return err
	}
	dataLen:=binary.BigEndian.Uint32(bsLen[:])
	counter:=0;
	for  dataLen>0{
		counter++
		dataBs:=make([]byte,dataLen)
		_,err=io.ReadFull(zipReader,dataBs)
		if err!=nil{
			return err
		}
		//log.Printf("count %d, dataLen:%d",counter,dataLen)
		item:=new(hitItem)
		err=json.Unmarshal(dataBs,item)
		if err != nil {
			return err
		}
		//提交数据
		req:= elastic.NewBulkIndexRequest()
		req.Id(item.ID).Doc(item.RawData)
		iserv.Add(req)
		if iserv.NumberOfActions()>999{
			_,err=iserv.Do(context.Background())
			if err != nil {
				log.Println(err)
			}
			log.Printf("row count %d",counter)
		}

		bsLen=[4]byte{}
		_,err=io.ReadFull(zipReader,bsLen[:])
		if err!=nil{
			if errors.Is(err,io.EOF){
				err=nil
				goto LAST
			}
			log.Println("bsLen read err",err)
			return err
		}
		dataLen=binary.BigEndian.Uint32(bsLen[:])
	}
	LAST:
	if iserv.NumberOfActions()>0{
		_,err=iserv.Do(context.Background())
		if err != nil {
			log.Println("es err", err)
		}
	}
	log.Printf("finish import row count %d",counter)
	return
}
func ExportData(outputFile ,esUrl,indexName,matchBody string)(err error) {
	var ofile *os.File
	if outputFile=="-"{
		ofile=os.Stdout
	}else{
		ofile, err= os.OpenFile(outputFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	}
	defer ofile.Close()

	if err != nil {
		log.Print("open file err", err)
		return err
	}
	zip := gzip.NewWriter(ofile)
	defer zip.Flush()
	defer zip.Close()
	ss:=GetEsScrollService(esUrl,indexName)
	if matchBody!=""{
		rawQuery:=elastic.NewRawStringQuery(matchBody)
		ss=ss.Query(rawQuery)
		log.Println("export match:",matchBody)
	}
	pager:=ss.Size(100)//.Query(elastic.MatchAllQuery{})
	pcounter := 0
	count:=0
	bsCounter:=0
	for{
		pcounter++;
		//for test
		//if pcounter > 5 {
		//	break
		//}
		res,err:=pager.Do(context.Background())
		if err == nil {
			for _, hit := range res.Hits.Hits {
				item:=hitItem{hit.Id,hit.Source}
				bs,_:=json.Marshal(&item)
				dataLen := [4]byte{}
				binary.BigEndian.PutUint32(dataLen[:], uint32(len(bs)))
				_, err = zip.Write(dataLen[:])
				if err != nil {
					return err
				}
				_, err = zip.Write(bs)
				if err != nil {
					return err
				}
				count++
				bsCounter+=len(bs)
				if count%200==0{
					log.Printf("total exported %d items; total_raw_bytes: %.2f MB", count, getMb(int64(bsCounter)))
				}
			}
			if len(res.Hits.Hits)<100{
				goto RETURN
			}
		}
		if err != nil {
			log.Println(err)
			time.Sleep(time.Second)
		}
	}
	RETURN:
	if err != nil {
		log.Print(err)
	}
	stat,_:=ofile.Stat()
	fsize:=getMb(stat.Size())
	log.Printf("total exported %d items; total_raw_bytes: %.2f MB;the gzip size: %.2f MB", count, getMb(int64(bsCounter)),fsize)
	return err
}
func getMb(size int64) float64{
	tmpf:=float64(size)/(1024*1024)*100
	tmpf=math.Trunc(tmpf)/100
	return tmpf
}
type hitItem struct {
	ID string
	RawData json.RawMessage
}