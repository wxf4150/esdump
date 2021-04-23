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
	"os"
	"time"
)
var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "elasticsearch export",
	Long:  `elasticsearch export`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Printf("export index %s to %s",IndexName,Output)
		ExportData(Input)
	},
}

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "elasticsearch import",
	Long:  `elasticsearch import`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Printf("import index %s from %s",IndexName,Input)
		err:=ImportData(Input)
		if err != nil {
			log.Println(err)
		}
	},
}
var Output string
var Input string
func init(){
	exportCmd.Flags().StringVar(&Output,"o","./tmp_export.json.gz","export desk filename")
	importCmd.Flags().StringVar(&Input,"i","./tmp_import.json.gz","import  filename")
	//importCmd.MarkFlagRequired("i")
	//exportCmd.MarkFlagRequired("o")

	RootCmd.AddCommand(exportCmd)
	RootCmd.AddCommand(importCmd)
}

func ImportData(inputFile string)(err error){
	infile,err1:=os.Open(inputFile)
	defer infile.Close()
	zipReader,err1 := gzip.NewReader(infile)
	if err1 != nil {
		return err1
	}
	defer zipReader.Close()
	if err1 != nil {
		return err1
	}
	iserv:=GetEsIndexService(EsUrl,IndexName)

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
		if iserv.NumberOfActions()>99{
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
	return
}
func ExportData(outputFile string)(err error) {
	start:=time.Now()
	sfile, err1 := os.OpenFile(outputFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	defer sfile.Close()
	if err1 != nil {
		log.Print("open file err", err1)
		return err1
	}
	zip := gzip.NewWriter(sfile)
	defer zip.Close()
	defer zip.Flush()

	ss:=GetEsScrollService(EsUrl,IndexName)
	pager:=ss.Size(100).Query(elastic.MatchAllQuery{})
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
					log.Printf("total exported %d items; total_raw_bytes: %f MB", count, float64(int64(float64(bsCounter)/1024/1024*100))/100)
				}
			}
			if len(res.Hits.Hits)<100{
				goto RETURN
			}
		}
	}
	RETURN:
	if err != nil {
		log.Print(err)
	}
	log.Printf("total exported %d items; total_raw_bytes: %f MB", count, float64(int64(float64(bsCounter)/1024/1024*100))/100)
	log.Printf("time spend %s",time.Now().Sub(start).String())
	return err
}
type hitItem struct {
	ID string
	RawData json.RawMessage
}
//func getBsData(chClient *channel.Client,chainCode,prekey ,startKey string)(bs []byte,err error){
//	log.Printf("chainCode: %s prekey: %s startKey: %s",chainCode,prekey ,startKey)
//	res, err1 := chClient.Query(channel.Request{ChaincodeID: chainCode,
//		Fcn:  "GetAllProto",
//		Args: [][]byte{[]byte(prekey),[]byte(startKey), []byte(CONF.Limit) },
//	})
//	if err1 != nil {
//		return nil ,err1
//	}
//	if res.ChaincodeStatus != 200 {
//		err = errors.New(string(res.Payload))
//		return
//	}
//	if res.Payload == nil {
//		log.Println("无新数据")
//		return
//	}
//	return res.Payload,nil
//}

//func main(){
//	if CONF.Cmd=="export"{
//		err:=ExportData(CONF.PreKey,CONF.StartKey,CONF.Output)
//		log.Println(err)
//		return
//	}
//	if CONF.Cmd=="import"{
//		err:=ImportData(CONF.Input)
//		log.Println(err)
//		return
//	}
//}