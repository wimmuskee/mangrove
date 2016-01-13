drop table if exists oairecords;

create table oairecords (
counter int(11) not null primary key auto_increment,
identifier varchar(36) not null,
original_id varchar(100) not null,
collection_id int(8) not null,
setspec varchar(40) not null,
updated int(11) not null,
deleted int(1) not null default 0,
lom blob not null,
oaidc blob not null);

alter table oairecords add unique unique_identifier (identifier);
alter table oairecords add unique original_identifier (original_id);
alter table oairecords add index index_collectionid (collection_id);
