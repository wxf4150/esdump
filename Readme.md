- elasticsearch  import export very quick.
- write in go 
- export ~~is~~ gziped 
- speedup to 84 times than nodejs-[elasticsearch-dump](https://github.com/elasticsearch-dump/elasticsearch-dump)

 
 usage:
 ```shell script
go build 
./esdump export --index my_index  -o ./my_index.json.gz  #export  my_index to file  myindex.json.gz
./esdump import --index my_index1 -i ./my_index.json.gz  #import   file  my_index.json.gz  to my_index1
./esdump export --es http://server1:9200 -o - --index tmp_index | ssh server2 ./esdump import --es http://localhost:9200 --index tmp_index1  -i - #export server1 tmp_index to stdout and pipe to next Import

#export which match body
./esdump  export --es http://server1:9200  --MatchBody '{"range": {"eventTimestamp": {"gte": "2021-05-07T10:32:20.170178Z"}}}' --index events
#export data a minites ago
./esdump export --es http://server1:9200 -m "{\"range\": {\"eventTimestamp\": {\"gte\": \"`date  -d  "1 minutes ago" +%s `000\"}}}" --index events -o - | ./esdump import --index events -i - 
 ./esdump -h
./esdump import -h
./esdump expport -h
 ```

**note**:
- when use import;  you should setting the target index's _mapping .


command help:
```shell script
 ./esdump -h
es import export

Usage:
  esdump [flags]
  esdump [command]

Available Commands:
  export      elasticsearch export
  help        Help about any command
  import      elasticsearch import

Flags:
      --es string      es url (default "http://localhost:9200")
  -h, --help           help for esdump
      --index string   index name (default "my_index")

Use "esdump [command] --help" for more information about a command.


./esdump  export -h
elasticsearch export

Usage:
  esdump export [flags]

Flags:
  -m, --MatchBody string   MatchBody, empty for match_all; example:{"range": {"timestamp": {"gte": "2021-04-20"}}} (default "{\"match_all\":{}}")
  -h, --help       help for export
      --o string   export desk filename; use - for stdout (default "./tmp_export.json.gz")

Global Flags:
      --es string      es url (default "http://localhost:9200")
      --index string   index name (default "my_index")


 ./esdump import -h
elasticsearch import

Usage:
  esdump import [flags]

Flags:
  -h, --help       help for import
      --i string   import  filename; use - for stdin (default "./tmp_import.json.gz")

Global Flags:
      --es string      es url (default "http://localhost:9200")
      --index string   index name (default "my_index")

```


why it so quick?

- esdump write in golang .
- when export,  esdump never decode/encode the res.hits.source to an json object, it only save the res.hits.source bytes to gzip stream directly.  
-  nodejs(elasticsearch-dump) may  decode/encode all "elasticsearch respose body" to json object when export.

note:  res.hits.source is the document body from elasticsearch respose body


the export format is below and very simple:
```shell script
{"ID":"163820696","RawData":{"id":163820696,"asset":"","imageUrl":""}}
{"ID":"163820697","RawData":{"id":163820696,"asset":"","imageUrl":""}}
{"ID":"163820698","RawData":{"id":163820696,"asset":"","imageUrl":""}}
...
...
```
one document one row.
the field "RawData" in the  document.

sorry my bad english