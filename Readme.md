- elasticsearch  import export very quick.
- write in go 
- export is gziped 
- speedup to 84 times than nodejs-[elasticsearch-dump](https://github.com/elasticsearch-dump/elasticsearch-dump)  
 - i tested on 60000 docs of raw bytes 106M ;
 

 
 usage:
 ```shell script
go build 
./esdump export --index my_index  -o ./my_index.json.gz  #export  my_index to file  myindex.json.gz
./esdump import --index my_index1 -i ./my_index.json.gz  #import   file  my_index.json.gz  to my_index1
./esdump export --es http://server1:9200 -o - --index tmp_index | ./esdump import --es http://server2:9200 --index tmp_index1  -i - #export server1 tmp_index to stdout and pipe to next Import
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