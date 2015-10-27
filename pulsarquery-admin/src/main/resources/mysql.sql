CREATE TABLE `DBDashboard` (
	`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	`name` VARCHAR(100) NOT NULL COLLATE 'utf8_bin',
	`owner` VARCHAR(64) NOT NULL,
	`config` BLOB NULL,
	`displayname` VARCHAR(64) NULL DEFAULT NULL,
	`createtime` DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`lastupdatetime` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (`id`),
	UNIQUE INDEX `id` (`id`),
	UNIQUE INDEX `name` (`name`),
	INDEX `owner` (`owner`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
AUTO_INCREMENT=5
;

CREATE TABLE `DBDatasource` (
	`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	`name` VARCHAR(100) NOT NULL COLLATE 'utf8_bin',
	`type` VARCHAR(255) NULL DEFAULT NULL,
	`displayname` VARCHAR(64) NULL DEFAULT NULL,
	`owner` VARCHAR(64) NOT NULL,
	`endpoint` VARCHAR(1028) NOT NULL,
	`properties` BLOB NULL,
	`comment` VARCHAR(100) NULL DEFAULT NULL,
	`createtime` DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
	`lastupdatetime` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`readonly` BIT(1) NOT NULL DEFAULT b'0',
	PRIMARY KEY (`id`),
	UNIQUE INDEX `id` (`id`),
	UNIQUE INDEX `name` (`name`),
	INDEX `owner` (`owner`)
)
COLLATE='gb2312_chinese_ci'
ENGINE=InnoDB
AUTO_INCREMENT=6
;

CREATE TABLE `DBGroup` (
	`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	`name` VARCHAR(100) NOT NULL DEFAULT '0',
	`owner` VARCHAR(64) NOT NULL DEFAULT '0',
	`comment` VARCHAR(256) NULL DEFAULT '0',
	`displayname` VARCHAR(64) NULL DEFAULT NULL,
	`createTime` DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
	`lastupdatetime` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (`id`),
	UNIQUE INDEX `name` (`name`),
	INDEX `owner` (`owner`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
AUTO_INCREMENT=6
;

CREATE TABLE `DBTables` (
	`id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
	`name` VARCHAR(32) NOT NULL DEFAULT '0',
	`datasourcename` VARCHAR(100) NOT NULL DEFAULT '0',
	`columns` BLOB NULL,
	`comment` VARCHAR(50) NULL DEFAULT '0',
	`createtime` DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
	`lastupdatetime` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (`id`)
)
COLLATE='gb2312_chinese_ci'
ENGINE=InnoDB
;

CREATE TABLE `DBUser` (
	`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	`name` VARCHAR(64) NOT NULL DEFAULT '',
	`comment` VARCHAR(128) NULL DEFAULT NULL,
	`password` VARCHAR(255) NULL DEFAULT NULL,
	`email` VARCHAR(255) NULL DEFAULT NULL,
	`image` VARCHAR(255) NULL DEFAULT NULL,
	`enabled` BIT(1) NULL DEFAULT b'1',
	`createTime` DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
	`lastupdatetime` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (`id`),
	UNIQUE INDEX `id` (`id`),
	UNIQUE INDEX `name` (`name`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
AUTO_INCREMENT=31
;

CREATE TABLE `DBUserGroup` (
	`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	`groupName` VARCHAR(100) NOT NULL DEFAULT '0',
	`userName` VARCHAR(64) NOT NULL DEFAULT '0',
	`createTime` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
	`lastupdatetime` DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (`id`),
	UNIQUE INDEX `groupName_userName` (`groupName`, `userName`),
	INDEX `userName` (`userName`),
	INDEX `groupName` (`groupName`),
	CONSTRAINT `FK_DBUserGroup_DBGroup` FOREIGN KEY (`groupName`) REFERENCES `DBGroup` (`name`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
AUTO_INCREMENT=25
;

CREATE TABLE `DBRightGroup` (
	`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	`groupName` VARCHAR(100) NOT NULL DEFAULT '0',
	`rightType` INT(11) NOT NULL DEFAULT '0',
	`rightName` VARCHAR(128) NOT NULL DEFAULT '0',
	`createTime` DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
	`lastupdatetime` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (`id`),
	UNIQUE INDEX `groupName_rightType_rightName` (`groupName`, `rightType`, `rightName`),
	INDEX `groupName` (`groupName`),
	CONSTRAINT `FK_DBRightGroup_DBGroup` FOREIGN KEY (`groupName`) REFERENCES `DBGroup` (`name`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
AUTO_INCREMENT=18
;

insert into DBGroup(name,owner,displayname) values('public','admin','public');
insert into DBGroup(name,owner,displayname) values('admin','admin','admin');
insert into DBRightGroup(groupName,rightType,rightName) values('admin',0,'SYS_MANAGE_GROUP');
insert into DBRightGroup(groupName,rightType,rightName) values('admin',0,'SYS_MANAGE_DATASOURCE');
insert into DBRightGroup(groupName,rightType,rightName) values('admin',0,'SYS_MANAGE_DASHBOARD');
insert into DBRightGroup(groupName,rightType,rightName) values('admin',0,'SYS_VIEW_GROUP');
insert into DBRightGroup(groupName,rightType,rightName) values('admin',0,'SYS_VIEW_DATASOURCE');
insert into DBRightGroup(groupName,rightType,rightName) values('admin',0,'SYS_VIEW_DASHBOARD');
insert into DBRightGroup(groupName,rightType,rightName) values('admin',0,'ADD_GROUP');
insert into DBRightGroup(groupName,rightType,rightName) values('admin',0,'ADD_DATASOURCE');
insert into DBRightGroup(groupName,rightType,rightName) values('admin',0,'ADD_DASHBOARD');	
			
insert into DBRightGroup(groupName,rightType,rightName) values('public',0,'ADD_DATASOURCE');
insert into DBRightGroup(groupName,rightType,rightName) values('public',0,'ADD_DASHBOARD');
insert into DBRightGroup(groupName,rightType,rightName) values('public',1,'tracking_VIEW');
insert into DBRightGroup(groupName,rightType,rightName) values('public',1,'trackingdruid_VIEW');
insert into DBRightGroup(groupName,rightType,rightName) values('public',1,'pulsarholap_VIEW');
insert into DBUser(name,password) values('admin','098f6bcd4621d373cade4e832627b4f6');
insert into DBUserGroup(groupName,userName) values('admin','admin');
