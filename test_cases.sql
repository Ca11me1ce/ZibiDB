CREATE TABLE table_name (column_name1 data_type not_null unique, column_name2 data_type null) primary key (column_name1, column_name2) foreign key (column_name_f, column_namef1) references table_name (column_name) on_delete delete on_update cascade;

CREATE TABLE Person (id int not_null unique, position char not_null, name char not_null, address char not_null) primary key (id) foreign key (position) references work (position_id) on delete no_action on update cascade;

CREATE TABLE P (idd int not_null unique, positiond char not_null unique, named char not_null, addressd char not_null) primary key (idd) foreign key (positiond) references work (position_id) on delete no_action on update cascade;

CREATE TABLE PP (idda int not_null unique, positionda char not_null unique, nameda char not_null, addressda char not_null) primary key (idda) foreign key (positionda) references work (position_ida) on delete no_action on update cascade;

insert into person (id, position, name, address) values (1, 'manager', 'Yang', 'home')

insert into perSON (id, position, name, address) values (2, 'eater', 'Yijing', 'homeless')

insert into perSON (id, position, name, address) values (7, '', 'Yijing', 'nb')

insert into p values (1, 'manager', 'Yang', 'home')

insert into p (idd, positiond, named, addressd) values (2, 'eater', 'Yijing', 'homeless')

insert into p (idd, positiond, named, addressd) values (7, 'eaters', 'Yijing', 'nb')

insert into pp (idda, positionda, nameda, addressda) values (1, 'lll', 'popop', 'yaocusi')

insert into pp (idda, positionda, nameda, addressda) values (9, 'fangfu', 'shenti', 'beotaokong')

