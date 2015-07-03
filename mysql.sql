 CREATE TABLE `system_parameter` (
  `PK` decimal(10,0) NOT NULL,
  `GROUPNAME` varchar(80) DEFAULT NULL,
  `CODE` varchar(40) DEFAULT NULL,
  `NAME` varchar(80) DEFAULT NULL,
  `VALUE1` varchar(200) DEFAULT NULL,
  `VALUE2` varchar(200) DEFAULT NULL,
  `MEMO` varchar(200) DEFAULT NULL,
  `CHANGABLE` char(1) DEFAULT NULL,
  PRIMARY KEY (`PK`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;


load data local infile '/home/opg/liveRec/system_parameter.txt' into table system_parameter  CHARACTER SET  utf8 fields terminated by ",";