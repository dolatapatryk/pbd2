#!/bin/sh

beeline -u 'jdbc:hive2://localhost:10000 projekt projekt' -f hive.hql
