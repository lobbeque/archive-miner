# Archive-search

A [Spark](https://spark.apache.org/) library for mining and indexing web archives files (Digital Archive File Format: DAFF) into a Solr search engine. Primarily designed for the exploration of the archived data of the [e-Diasporas Atlas](http://www.e-diasporas.fr/). 

## Current dependencies 

### INA dlweb dependencies

   1. daff-io
   2. dlweb-commons

See directly with the [INA dlweb](https://institut.ina.fr/collections/le-web-media) for more informations about those dependencies.

### Other dependencies

   1. Hadoop-tools 2.1
   2. Restlet 2.3.0
   3. Scala-reflect 2.10.4
   4. Spark-solr 3.2.0
   3. [Rivelaine](https://github.com/lobbeque/rivelaine) 2.0

## Usage 

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

## Licence

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program.  If not, see <https://www.gnu.org/licenses/>.