# Install

- pip install -r requirements.txt

# Principle

## Livy

Livy is a REST interface for Spark. Multiple jobs can be driven from python.
The python web application initiates livy connections at startup.
It maintains them alive and ready to work.
Depending on the user action the web application asks livy to run spark jobs.
The postgres jdbc jar must be passed by jars, while the other must be in the
spark folder otherwise this crashes.

## Solr

Solr can be called from python.
