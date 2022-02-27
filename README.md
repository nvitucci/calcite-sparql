# Calcite SPARQL adapter

This adapter can be used to execute SQL queries over a SPARQL endpoint via [Apache Calcite](https://calcite.apache.org/) and [Apache Jena](https://jena.apache.org/). The project is a work in progress and it is not meant to be run in production.

## Description

The adapter can be used in several modes to expose the RDF data in a SPARQL endpoint as data:

- the `property` mode, which exposes every RDF property (the `?p` of a `?s ?p ?o` pattern) as a table with two columns `s` (subject) and `o` (object);
- the `class` mode, which exposes every RDF class (the `?cl` of a `?s rdf:type ?cl` pattern) as a table whose columns are all the properties that link items of such class (the `?s` of the same pattern) to objects;
- the `mapping` mode, where selected properties are directly mapped to columns of a specified table.

### Example of the model file

The SPARQL endpoint in the following examples is a Jena in-memory endpoint created from a local file. Replace with e.g. `"jdbc:jena:remote:query=https://dbpedia.org/sparql"` to use a remote endpoint.

1. Using the `property` mode:
```json
{
  "version": "1.0",
  "defaultSchema": "sparql",
  "schemas": [
    {
      "name": "sparql",
      "type": "custom",
      "factory": "com.datagrafting.sql2sparql.calcite.SparqlSchemaFactory",
      "operand": {
        "endpoint": "jdbc:jena:mem:dataset=data.nq",
        "tableMode": "property"
      }
    }
  ]
}
```

2. Using the `class` mode:

```json
{
  "version": "1.0",
  "defaultSchema": "sparql",
  "schemas": [
    {
      "name": "sparql",
      "type": "custom",
      "factory": "com.datagrafting.sql2sparql.calcite.SparqlSchemaFactory",
      "operand": {
        "endpoint": "jdbc:jena:mem:dataset=data.nq",
        "tableMode": "class"
      }
    }
  ]
}
```

3. Using the `mapping` mode:

```json
{
  "version": "1.0",
  "defaultSchema": "sparql",
  "schemas": [
    {
      "name": "sparql",
      "type": "custom",
      "factory": "com.datagrafting.sql2sparql.calcite.SparqlSchemaFactory",
      "operand": {
        "endpoint": "jdbc:jena:mem:dataset=data.nq",
        "tableMode": "mapping",
        "tableMappings": [
          {
            "name": "Person",
            "columns": [
              {
                "name": "name",
                "property": "http://xmlns.com/foaf/0.1/name"
              },
              {
                "name": "age",
                "property": "http://xmlns.com/foaf/0.1/age"
              }
            ]
          }
        ]
      }
    }
  ]
}
```

### Basic Usage

- Java [example](examples/java/src/main/java/com/datagrafting/sql2sparql/examples/SparqlClassTableRemote.java)
- Python (Jupyter notebook) examples using [jaydebeapi](examples/python/Query%20DBPedia%20with%20jaydebeapi.ipynb) and [Apache Arrow](examples/python/Query%20DBPedia%20with%20Apache%20Arrow.ipynb)

### Running the examples

Compile the main library using Java 11:

```shell
cd core
mvn clean package -DskipTests
cd ..
```

or using Java 8:

```shell
cd core
mvn clean package -P java1.8 -DskipTests
cd ..
```

Please note that compiling with Java 8 forces Jena version to 3.17.0 (last version before switching to Java 11). 

For the Java examples:

```shell
cd example
mv clean package
/path/to/java -cp target/classes:../../core/target/calcite-sparql-core-0.0.1-SNAPSHOT.jar com.datagrafting.sql2sparql.examples.SparqlClassTableRemote
```

For the Python examples, Python 3 and Jupyter Notebook need to be installed first. Then, the notebooks can be run from the [examples/python](examples/python) directory.

### Pushed-down SQL constructs

- `SELECT *` and `SELECT` with any number of columns
- `WHERE` (with `=`, `<>`, `<`, `>`, `<=`, `>=`)
- `ORDER BY`
- `LIMIT`

## Releasing

When releasing, please make sure to update both the changelog and the citation file.

## Citing the project

If you use this project in an article, please cite it as specified in the [CITATION.cff](./CITATION.cff) file.

## Acknowledgements

Many thanks to:

- [Paul Jackson](https://github.com/PaulJackson123) for interesting discussions on the mapping from RDF to SQL schema mapping.
