# Calcite SPARQL adapter

This adapter can be used to execute SQL queries over a SPARQL endpoint via [Apache Calcite](https://calcite.apache.org/) and [Apache Jena](https://jena.apache.org/). The project is a work in progress and it is not meant to be run in production.

## Description

The adapter can be used in two different modes to expose the RDF data in a SPARQL endpoint as data:

- the `property` mode, which exposes every RDF property (the `?p` of a `?s ?p ?o` pattern) as a table with two columns `s` (subject) and `o` (object);
- the `class` mode, which exposes every RDF class (the `?cl` of a `?s rdf:type ?cl` pattern) as a table whose columns are all the properties that link items of such class (the `?s` of the same pattern) to objects. 

### Example of the model file

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

The SPARQL endpoint in this case is a Jena in-memory endpoint created from a local file. Replace with e.g. `"jdbc:jena:remote:query=https://dbpedia.org/sparql"` to use a remote endpoint.

### Basic Usage

- Java [example](examples/java/src/main/java/com/datagrafting/sql2sparql/examples/SparqlClassTableRemote.java)
- Python (Jupyter notebook) examples using [jaydebeapi](examples/python/Query%20DBPedia%20with%20jaydebeapi.ipynb) and [Apache Arrow](examples/python/Query%20DBPedia%20with%20Apache%20Arrow.ipynb)

### Running the examples

```shell
cd core
mvn clean package -DskipTests
cd ..
```

For the Java examples:

```shell
cd example
mv clean package
/path/to/java -cp target/classes:../core/target/calcite-sparql-core-0.0.1-SNAPSHOT.jar com.datagrafting.sql2sparql.examples.SparqlClassTableRemote
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
