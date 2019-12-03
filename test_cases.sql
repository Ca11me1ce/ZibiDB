CREATE TABLE table_name (column_name1 data_type not_null unique, column_name2 data_type null) primary key (column_name1, column_name2) foreign key (column_name_f, column_namef1) references table_name (column_name) on_delete delete on_update cascade;

CREATE TABLE Person (id int not_null unique, position char not_null, name char not_null, address char not_null) primary key (id) foreign key (position) references work (position_id) on delete no_action on update cascade;

CREATE TABLE P (id int not_null unique, position char not_null unique, name char not_null, address char not_null) primary key (id) foreign key (position) references work (position_id) on delete no_action on update cascade;

insert into person (id, position, name, address) values (1, 'manager', 'Yang', 'home')

insert into perSON (id, position, name, address) values (2, 'eater', 'Yijing', 'homeless')

insert into perSON (id, position, name, address) values (7, '', 'Yijing', 'nb')

SELECT CustomerName, City FROM Customers where city=arlington

CREATE TABLE Person (id int not_null unique) primary key (id) foreign key (position) references work (position_id) on delete cascade

insert into p values (1, 'manager', 'Yang', 'home')

insert into p (id, position, name, address) values (2, 'eater', 'Yijing', 'homeless')

insert into p (id, position, name, address) values (7, 'eater', 'Yijing', 'nb')
