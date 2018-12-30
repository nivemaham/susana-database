# Install

- pip install -r requirements.txt

# Principle

## Livy

Livy is a REST interface for Spark. Multiple jobs can be driven from python.
The python web application initiates livy connections at startup.
It maintains them alive and ready to work.
Depending on the user action the web application asks livy to run spark jobs.

## Solr

Solr can be called from python.
