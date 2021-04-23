package cmds

import (
	"crypto/tls"
	"github.com/olivere/elastic/v7"
	"log"
	"net/http"
)

func getEsClient(esUrl string) *elastic.Client{
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	elasClt, err := elastic.NewClient(
		elastic.SetHttpClient(httpClient),
		elastic.SetSniff(false),
		elastic.SetURL(esUrl))
	if err != nil {
		panic("es err:" + err.Error()+" esUrl:"+esUrl)
	}
	log.Println("init elasticsearch Client  with url:",esUrl)
	return elasClt
}
func GetEsScrollService(es,indexName string)(*elastic.ScrollService) {
	searchClt:=getEsClient(es)
	ss:=searchClt.Scroll().Index(indexName)
	return ss
}

func GetEsIndexService(es,indexName string)(*elastic.BulkService) {
	searchClt:=getEsClient(es)
	indexService:=searchClt.Bulk().Index(indexName)
	return indexService
}


