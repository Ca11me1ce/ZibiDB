CREATE TABLE database_name.table_name (column_name1 data_type not_null unique, column_name2 data_type null) primary_key (column_name1, column_name2) foreign_key (column_name_f, column_namef1) references database_name.table_name (column_name) on_delete delete on_update cascade;

CREATE TABLE NotAP.Person (id int not_null unique, position char not_null, name char not_null, address char not_null) primary_key (id) foreign_key (position) references NotAP.work (position_id) on_delete no_action on_update cascade;

insert into notap.person (id, position, name, address) values (1, 'manager', 'Yang', 'home')