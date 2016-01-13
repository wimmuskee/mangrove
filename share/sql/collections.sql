drop table if exists collections;

create table collections (
id int(8) not null primary key auto_increment,
configuration varchar(40) not null,
updated int(11) not null default 0 );

alter table collections add unique unique_configuration (configuration);
