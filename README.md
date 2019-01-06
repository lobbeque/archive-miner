# Archive-miner

A [Spark](https://spark.apache.org/) library for mining and indexing web archives files (Digital Archive File Format: DAFF) into a Solr search engine. Primarily designed for the exploration of the archived data of the [e-Diasporas Atlas](http://www.e-diasporas.fr/). 

## Current dependencies 

### INA dlweb dependencies

   1. daff-io
   2. dlweb-commons

See directly with the [INA dlweb](https://institut.ina.fr/collections/le-web-media) for more informations about those dependencies.

### Spark dependencies 

   1. Spark 2.2.0 

Get the Spark sources from the [official repository](https://spark.apache.org/downloads.html), then create a working directory and copy the sources:

```
mkdir -p ~/spark/
cp -r ~/spark-2.2.0-bin-hadoop2.7/* ~/spark/
``` 

### Other dependencies

   1. Hadoop-tools 2.1
   2. Restlet 2.3.0
   3. Scala-reflect 2.10.4
   4. Spark-solr 3.2.0
   3. [Rivelaine](https://github.com/lobbeque/rivelaine) 2.0

## Usage 

Download or clone the source file:

```
git clone git@github.com:lobbeque/archive-miner.git
```

Build the source code using [sbt](https://www.scala-sbt.org/) and see `~/archive-miner/build.sbt` for some specific configurations: 

```
cd ~/archive-miner/
sbt assembly
```

Copy the resulting `.jar` file in the working directory: 

```
cp ./target/scala-2.10/archive-miner-assembly-1.0.0.jar ~/spark/
```

Create a configuration file:

```
cd ~/spark/
touch conf.json
```

Edit the fields as follows:

```
{
  "metaPath"  : "~/webArchives/metadata.daff",
  "dataPath"  : "~/WebArchives/data.daff",
  "solrHost"  : "localhost:2118",
  "solrColl"  : "fragments_test",
  "type"      : "fragments",
  "rivelaineUrl" : "http://localhost:2200/getFragment?",
  "partitionSize" : 5000,
  "urlFilter" : ["site1.com"],
  "dates" : "2000-01-01 2010-01-01"
}
```

**Important:** pay attention to `solrColl` (name of the targeted solr collection), `type` (`fragments` by default), `partitionSize` (size of the spark partitions), `urlFilter` (filter the Web archives by site name such as `["site1.com","site2.com"]`) and `dates` (filter the Web archives by date such as `"from to"`). You also need to have a running instance of [archive-search](https://github.com/lobbeque/archive-search) and [rivelaine](https://github.com/lobbeque/rivelaine). 

Run spark using archive-miner:

```
./bin/spark-submit --class qlobbe.ArchiveReader --driver-memory 10g --num-executors 60 --executor-cores 10 --executor-memory 10g ./archive-miner-assembly-1.0.0.jar "./conf_test.json"
``` 

See the [Spark documentation](https://spark.apache.org/docs/latest/configuration.html) for specific configurations.

## Licence

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program.  If not, see <https://www.gnu.org/licenses/>.