- elasticsearch  import export very quick.
- write in go 
- export is gziped 
- speedup to 84 times than nodejs-[elasticsearch-dump](https://github.com/elasticsearch-dump/elasticsearch-dump)  
 - i tested on 60000 docs of raw bytes 106M ;
 
 usage:
 - go build 
 - ./esdump -h
 - ./esdump import -h
 - ./esdump expport -h

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
      --o string   export desk filename (default "./tmp_export.json.gz")

Global Flags:
      --es string      es url (default "http://localhost:9200")
      --index string   index name (default "my_index")

```