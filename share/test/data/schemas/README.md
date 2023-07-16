## commons schemas and data

The objective of this folder is to provide test cases on avro schemas and datas for each SDK.  

Each subfolder is composed of a  :
- schema.json file, for schema
- data.avro file that contains some records
- README.md that briefly explains the tested used case. 

Steps for tests are :
- read schema (with schema.json file).
- read data file (data.avro file)
- Check it can write record in output temp file.
