# INFSCI 1540 Project Report - American Football Quarterbacks

### What data are we maintaining in our system?
Our data engineering system maintains a list of NFL quarterbacks over the last 4 years, their **current** teams, the games they played in, and their standard fantasy football score each game.(.04*passings yard + .1*rushing yards + 4*passing touchdowns+.6*rushing touchdowns - 2*fumbles)

We seperated games by weeks because its how NFL seasons are split and organized the weeks of the 2020-2023 seasons into 1 table so the weeks do not overlap.

### Our docker containers
1. Php Web Server - Gotta double check what this is for
2. Mysql ODB Server - Our operational database to store our tables
3. Mysql DW Server - Our data warehouse holding onto our fact table and player performance information
4. ODB Phpmyadmin Server - Allows us to interact with the ODB sql server
5. DW Phpmyadmin Server - Allows us to interact with the DW sql server
6. Kafka Broker - Allows our producers and consumers to interact with each other
7. Zookeeper - Keeps track of our current broker and tells producers and consumers to go through our broker

### Star Diagram
```mermaid
---
title: Star Chart
---
erDiagram
  PLAYER }|--|| FACT: Dimension
  PLAYER {
    int TeamId
    string Name
    string Pos
  }
  PPpW }|--|| FACT: Dimension
  PPpW {
    int PlayerID
    int TeamID
    int WeekID
    double fScore
  }
  FACT {
    int PlayerID
    int TeamID
    int WeekID
    double Score
  }
  TEAM }|--|| FACT: Dimension
  TEAM {
    string Name
    string Abreviation
    string Conference
    string Division
  }
  WEEK }|--|| FACT: Dimension
  WEEK {
    int WeekNumber
    int Season
  }
  
