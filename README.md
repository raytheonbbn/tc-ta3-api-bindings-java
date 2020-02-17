# Introduction
This is the landing page for the Java APIs for TA3 services.

# Subprojects
The following subprojects are part of this project:
 1. Avro API:  https://github.com/raytheonbbn/tc-ta3-api-bindings-java/tree/master/tc-bbn-avro
 2. Kafka API:  https://github.com/raytheonbbn/tc-ta3-api-bindings-java/tree/master/tc-bbn-kafka

# Installation
Installation instructions are captured in the subprojects, but note that there is a dependency on the ta3-serialization-schema project.  These projects should be built and installed in the following order:
 1. ta3-serialization-schema
 2. tc-bbn-avro
 3. tc-bbn-kafka
