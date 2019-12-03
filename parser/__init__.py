def exit(action):
    return {
        'mainact' : 'exit'
    }

def create(action):
    if action[1].upper() == 'DATABASE':
        return{
            'mainact' : 'create',
            'type' : 'database',
            'name' : action[2].lower()
        }

    elif action[1].upper() == 'INDEX':
        return create_index(action)


    elif action[1].upper() == 'TABLE':

        table_info = action[3:]
        table_name = action[2].lower()

        std_type=['CHAR', 'FLOAT', 'INT']

        # Get attributes
        # (column_name1 data_type not_null, column_name2 data_type null)
        table_attrs=[]
        while True:
            if '(' in table_info[0]:
                table_attrs.append(table_info.pop(0).replace('(', ''))
            elif ')' in table_info[0]:
                table_attrs.append(table_info.pop(0).replace(')', ''))
                break
            else:
                table_attrs.append(table_info.pop(0))
        # Each elements contains attrbute type constrain
        table_attrs=' '.join(table_attrs).split(',')

        # for each elem in table_attrs, get attribute name, types and constrains(null status and unique status)
        attrs=[]
        _type=[]
        null_status=[]
        unique_status=[]
        for i in table_attrs:
            tmp=i.strip().split(' ')
            attrs.append(tmp[0].lower())
            _type.append(tmp[1].upper())

            # There is null or/and unique status
            if len(tmp)==3: 
                # print(tmp)
                if tmp[2].upper()=='NOT_NULL' or tmp[2].upper()=='NULL':
                    null_status.append(tmp[2].upper())
                    unique_status.append('')

                elif tmp[2].upper()=='UNIQUE' or tmp[2].upper()=='NOT_UNIQUE':
                    unique_status.append(tmp[2].upper())
                    null_status.append('')
                        
            # Only have (attrs and type)
            elif len(tmp)==2:
                null_status.append('')
                unique_status.append('')
            elif len(tmp)==4:
                if tmp[2].upper()!='NOT_NULL' and tmp[2].upper()!='NULL': raise Exception('ERROR: Invalid syntax.')
                if tmp[3].upper()!='NOT_UNIQUE' and tmp[3].upper()!='UNIQUE': raise Exception('ERROR: Invalid syntax.')
                null_status.append(tmp[2].upper())
                unique_status.append(tmp[3].upper())
            else: raise Exception('ERROR: Invalid syntax.')
        # print(attrs)
        # print(_type)
        # print(null_status)
        # print(unique_status)

        # Get primary key
        primary_key=[]
        if table_info:
            if table_info[0].upper()=='PRIMARY':
                table_info.pop(0)   # Pop PRIMARY

                # Pop KEY
                if table_info.pop(0).upper()=='KEY':
                    if '(' in table_info[0] and ')' in table_info[0]:
                        primary_key.append(table_info.pop(0).strip('()'))
                    else:
                        while True:
                            if '(' in table_info[0]:
                                primary_key.append(table_info.pop(0).strip('( ').lower())
                            elif ')' in table_info[0]:
                                primary_key.append(table_info.pop(0).strip(') ').lower())
                                break
                            else:
                                primary_key.append(table_info.pop(0).strip().lower())
                else: raise Exception('ERROR: Invalid syntax.')
        # print(primary_key)
        foreign_key=[]
        if table_info:
            if table_info[0].upper()=='FOREIGN':
                table_info.pop(0)   # Pop foreign

                # Pop 'KEY'
                if table_info.pop(0).upper()=='KEY':
                    if '(' in table_info[0] and ')' in table_info[0]:
                        foreign_key.append(table_info.pop(0).strip('()'))
                    else:
                        while True:
                            if '(' in table_info[0]:
                                foreign_key.append(table_info.pop(0).strip('( ').lower())
                            elif ')' in table_info[0]:
                                foreign_key.append(table_info.pop(0).strip(') ').lower())
                                break
                            else:
                                foreign_key.append(table_info.pop(0).strip().lower())
                else: raise Exception('ERROR: Invalid syntax.')

        ref_table=[]
        ref_column=[]
        ref_columns=[]
        if table_info:
            while table_info[0].upper()=='REFERENCES':
                table_info.pop(0)   # Pop REFERENCES

                # [ref_database, ref_table]
                ref_name=table_info.pop(0)# Table basename

                ref_table.append(ref_name)

                if '(' in table_info[0] and ')' in table_info[0]:
                    ref_column.append(table_info.pop(0).strip('() '))
                    ref_columns.append(ref_column)
                else:
                    while True:
                        if '(' in table_info[0]:
                            ref_column.append(table_info.pop(0).strip('( ').lower())
                        elif ')' in table_info[0]:
                            ref_column.append(table_info.pop(0).strip(') ').lower())
                            ref_columns.append(ref_column)
                            break
                        else:
                            ref_column.append(table_info.pop(0).strip().lower())
                if not table_info: break
        # TODO: Each reference should have a on_delete and on_update
        # print(table_info)
        on_delete='NO_ACTION'
        if table_info:
            if table_info[0].upper()=='ON':

                if table_info[1].upper()=='DELETE':
                    table_info.pop(0)    # Pop on
                    table_info.pop(0)    # Pop delete
                    on_delete=table_info.pop(0)

        # print(table_info)
        on_update='NO_ACTION'
        if table_info:
            if table_info[0].upper()=='ON':
                    if table_info[1].upper()=='UPDATE':
                        table_info.pop(0)    # Pop on
                        table_info.pop(0)    # Pop 'UPDATE'
                        on_update=table_info.pop(0)

        ref_info=[{'schema': ref_table, 'columns': ref_columns, 'on_delete': on_delete.upper(), 'on_update': on_update.upper()}]
        # print(ref_info)

        attrs_ls=[]
        for i in range(len(attrs)):
            if _type[i] not in std_type:
                raise Exception('ERROR: Invalid type.')
            attrs_ls.append({
                    'name':attrs[i],
                    'type': _type[i],
                    'notnull': 1 if null_status[i].upper()=='NOT_NULL' else 0,
                    'unique': 1 if unique_status[i].upper()=='UNIQUE' else 0,
            })
        # print(attrs_ls)
        
        foreignk = {}

        for attri in foreign_key:
            foreignk[attri] = ref_info[0]
            i += 1
        
        info={
            'name':table_name,
            'attrs':attrs_ls,
            'primary': primary_key,
            'foreign': foreignk,
        }

        return{
            'mainact' : 'create',
            'type' : 'table',
            'name' : table_name,
            'attrls' : attrs,
            'info' : info
        }

    else:
        raise Exception('Syntax error! Recommend : create table/database ')

def drop(action):
    if action[1].upper() == 'DATABASE':
        return{
            'mainact' : 'drop',
            'type' : 'database',
            'name' : action[2].lower()
        }

    elif action[1].upper()=='INDEX':
        return drop_index(action)

    elif action[1].upper() == 'TABLE':
        return{
            'mainact' : 'drop',
            'type' : 'table',
            'table_name' : action[2].lower()
        }
        
def insert(action):
    if action[1].upper() == 'INTO':
        """
        TODO:
        Put new insert parser there and fit into attr:list and data:list below.
        """
        attrs = []
        data = []

        '''
        '''
        action.pop(0) # Pop Insert
        action.pop(0) # Pop INTO
        table_name=action.pop(0).lower()    #Pop table name
        if action[0].upper()=='VALUES':    # insert table values ()
            # No attrs, only values
            action.pop(0) #Pop VALUE
            if '(' in action[0]:
                for value in action:
                    elem=value
                    if '.' in value:
                        try:
                            value=float(value)
                        except:
                            raise Exception('ERROR 1: Invalid symtax')
                    elif "'" in value:
                        value=value.strip("()' ")
                    else:
                        try:
                            value=int(value)
                        except:
                            raise Exception('ERROR 2: Invalid symtax')

                    data.append(value)
                    if ')' in elem:
                        return{
                            'mainact' : 'insert',
                            'table_name' : table_name,
                            'attrs' : attrs,
                            'data' : data
                        }

            else:
                raise Exception('ERROR 3: Invalid syntax')
            raise Exception('ERROR 4: Invalid syntax')
        else:   # attrs and data

            # Get attrs
            if '(' in action[0]:
                count=0
                for elem in action:
                    if ')' in elem:
                        attrs.append(elem.strip('() ,'))
                        count+=1
                        break
                    attrs.append(elem.strip('() ,'))
                    count+=1
                for _ in range(count):
                    action.pop(0)
                if action==[]: 
                    raise Exception('ERROR 5: No data')
                # print(attrs)
                if len(attrs)!=len(set(attrs)): 
                    raise Exception('ERROR 6: Duplicated attributes')
                if action[0].upper()!='VALUES':
                    raise Exception('ERROR 7: Invalid syntax')
                action.pop(0)

            else:
                raise Exception('ERROR 8: Invalid syntax')

            if '(' in action[0]:
                for value in action:
                    elem=value
                    value=value.strip('() ,')
                    if '.' in value:
                        try:
                            value=float(value)
                        except:
                            raise Exception('ERROR 9: Invalid symtax')
                    elif "'" in value:
                        value=value.strip("()' ,")
                    else:
                        try:
                            value=int(value)
                        except:
                            print(value)
                            raise Exception('ERROR 10: Invalid symtax')


                    if ')' in elem:
                        data.append(value)
                        if len(attrs)!=len(data):
                            raise Exception('ERROR 11: Data length is not correponding to attributes.')

                        return{
                            'mainact' : 'insert',
                            'table_name' : table_name,
                            'attrs' : attrs,
                            'data' : data
                        }
                    data.append(value)
                raise Exception('ERROR 12: Invalid syntax')
                
            else:
                raise Exception('ERROR 13: Invalid syntax')

    else:
        raise Exception('Syntax error! Recommend : insert into ')

def select(action):
    if action[0].upper()=='SELECT':
        action.pop(0)

    # Get distinct
    _distinct=0
    if action[0].upper()=='DISTINCT':
        _distinct=1
        action.pop(0)
    print('distinct: ', _distinct)
    key_words=['FROM', 'WHERE', 'ORDER', 'GROUP', 'BY']

    # Get attrs, and aggragate function
    select_attrs=[]
    # attrs_dict=dict()
    while action[0].upper() not in key_words:
        select_attrs.append(action.pop(0).lower().strip(', '))  # attr list

    attrs_dict=parse_attrs(select_attrs)
    print('attrs: ', attrs_dict)

    # Get table names
    # Table names
    select_tables=[]
    # print(action)
    if action.pop(0).upper()=='FROM':
        while action[0].upper() not in key_words:
            select_tables.append(action.pop(0).lower().strip(', '))
            if action==[]:
                break
    else: raise Exception('ERROR: Invalid syntax.')
    print('tables: ', select_tables)

    # Where clause
    where_clause=[]
    where_expression=dict()
    # conditions=[]
    # op=[]
    if action:
        if action[0].upper()=='WHERE':
            action.pop(0)
            while action[0].upper() not in key_words:
                where_clause.append(action.pop(0).strip(', '))
                if not action:
                    break
            conditions=reorder_where_clause(where_clause)

            # where clause poland expression
            where_expression=parse_conditions(conditions)   # Parse where clause
    print('where clause: ', where_expression)

    # Get group by clause
    groupBy_clause=[]
    groupBy_expression=dict()
    if action:
        if action[0].upper()=='GROUP':
            action.pop(0)   # Pop group
            # pop BY
            if action.pop(0).upper()!='BY': raise Exception('ERROR: Invalid syntax.') 
            while action[0].upper() not in key_words:
                groupBy_clause.append(action.pop(0).strip(', '))
                if not action:
                    break
            if groupBy_clause[0].upper()=='HAVING': raise Exception('ERROR: Invalid syntax.')

            groupBy_expression=parse_groupBy(groupBy_clause, attrs_dict)
    print('GROUP BY CLAUSE: ', groupBy_expression)

    orderBy_clause=[]
    orderBy_expression=dict()
    if action:
        if action[0].upper()=='ORDER':
            action.pop(0)   # Pop order
            if action.pop(0).upper()!='BY':
                raise Exception('ERROR: Invalid syntax')
            while action[0].upper() not in key_words:
                orderBy_clause.append(action.pop(0).lower().strip(', '))
                if not action:
                    break
            orderBy_expression=parse_orderBy(orderBy_clause)
    print('ORDER BY CLAUSE: ', orderBy_expression)
    if action!=[]:
        raise Exception('ERROR: Invalid syntax.')
    return {
        'mainact': 'select',
        'attrs': attrs_dict,    # dict->{attr: aggregate function, } such as {id: MAX, }
        'tables': select_tables,    # list->[table_names]
        'where': where_expression,  # list->[{attr: , value: , symbol: , tag:}, op, ] Poland expression
        # dict->{group_by: [attrs], conditions: [Poland expression like where_clause]}
        'groupby': groupBy_expression,  
        'orderby': orderBy_expression   # dict->{order_by: [attrs], order: DESC/ASC/NO_ACTION}
    }

def reorder_where_clause(where_clause):
    conditions=[]
    temp=[]
    op=[]
    for i in range(len(where_clause)):      
        condition=dict()
        if (where_clause[i].upper() in ['OR', 'AND', '(', ')'] and where_clause[i-2].upper()!='BETWEEN') or i==len(where_clause)-1:
            if i==len(where_clause)-1:
                temp.append(where_clause[i])
            else:
                op.append(where_clause[i].upper())
                if where_clause[i+1].upper() in ['OR', 'AND', '(', ')']:
                    continue                

            if temp:
                tag=0
                temp=' '.join(temp)
                if '<=' in temp:

                    tmp=temp.split('<=')
                    try:
                        value=float(tmp[1])
                    except:
                        value=tmp[1]
                        tag=1
                    condition={'attr': tmp[0].lower(), 'value': value, 'symbol': '<=', 'tag': tag}
                elif '>=' in temp:
                    tmp=temp.split('>=')
                    try:
                        value=float(tmp[1])
                    except:
                        value=tmp[1]
                        tag=1
                    condition={'attr': tmp[0].lower(), 'value': value, 'symbol': '>=', 'tag': tag}
                elif '<>' in temp:
                    if tmp[1][0]!="'" and tmp[1][len(tmp[1])-1]!="'":
                        tag=1
                    tmp=temp.split('<>')
                    condition={'attr': tmp[0].lower(), 'value': tmp[1].strip("'"), 'symbol': '<>', 'tag': tag}
                elif '=' in temp:
                    tmp=temp.split('=')
                    if tmp[1][0]!="'" and tmp[1][len(tmp[1])-1]!="'":
                        tag=1

                    value = tmp[1].strip('() ,')
                    try:
                        value=int(value)
                        tag=0
                    except:
                        tag=1

                    condition={'attr': tmp[0].lower(), 'value': value, 'symbol': '=', 'tag': tag}
                elif '<' in temp:
                    tmp=temp.split('<')
                    try:
                        value=float(tmp[1])
                    except:
                        value=tmp[1]
                        tag=1
                    condition={'attr': tmp[0].lower(), 'value': value, 'symbol': '<', 'tag': tag}
                elif '>' in temp:
                    tmp=temp.split('>')
                    try:
                        value=float(tmp[1])
                    except:
                        value=tmp[1]
                        tag=1
                    condition={'attr': tmp[0].lower(), 'value': value, 'symbol': '>', 'tag': tag}
                elif ' LIKE ' in temp.upper():
                    tmp=temp.split('LIKE')
                    condition={'attr': tmp[0].lower(), 'value': tmp[1], 'symbol': 'LIKE', 'tag': 0 }
                elif ' NOT LIKE ' in temp.upper():
                    tmp=temp.split('LIKE')
                    condition={'attr': tmp[0].lower(), 'value': tmp[1], 'symbol': 'NOT LIKE', 'tag': 0 }
                elif 'BETWEEN' in temp.upper():
                    tmp=temp.split(' ')
                    tmp_attr=tmp.pop(0).lower() #Pop attr
                    if tmp.pop(0).upper()!='BETWEEN': raise Exception('ERROR: Invalid Where Clause.')   #Pop Between
                    
                    try:
                        if float(tmp[0])>float(tmp[2]):
                            raise Exception('ERROR: Invalid where clause')
                    except: raise Exception('ERROR: Invalid where clause')

                    # v1
                    conditions.append({
                        'attr': tmp_attr,
                        'value': tmp.pop(0),
                        'symbol': '>=',
                        'tag': 0 
                    })

                    if tmp.pop(0).upper()!='AND': raise Exception('ERROR: Invalid Where Clause.')   # Pop AND
                    conditions.append('AND')
                    # v2
                    conditions.append({
                        'attr': tmp_attr,
                        'value': tmp.pop(0),
                        'symbol': '<=',
                        'tag': 0 
                    })
                    temp=[]
                    if op:
                        while op:
                            conditions.append(op.pop(0))
                    continue
                elif 'BETWEEN' in temp.upper():
                    tmp=temp.split(' ')
                    tmp_attr=tmp.pop(0).lower() #Pop attr
                    if tmp.pop(0).upper()!='BETWEEN': raise Exception('ERROR: Invalid Where Clause.')   #Pop Between
                    
                    try:
                        if float(tmp[0])>float(tmp[2]):
                            raise Exception('ERROR: Invalid where clause')
                    except: raise Exception('ERROR: Invalid where clause')

                    # v1
                    conditions.append({
                        'attr': tmp_attr,
                        'value': tmp.pop(0),
                        'symbol': '<=',
                        'tag': 0 
                    })

                    if tmp.pop(0).upper()!='AND': raise Exception('ERROR: Invalid Where Clause.')   # Pop AND
                    conditions.append('AND')
                    # v2
                    conditions.append({
                        'attr': tmp_attr,
                        'value': tmp.pop(0),
                        'symbol': '>=',
                        'tag': 0 
                    })
                    temp=[]
                    if op:
                        while op:
                            conditions.append(op.pop(0))
                    continue
                elif ' IN ' in temp.upper():
                    tmp=temp.split(' ')
                    tmp_attr=tmp.pop(0).lower()    # Pop attr
                    if tmp.pop(0).upper()!='IN': raise Exception('ERROR: Invalid Where Clause.')  # Pop IN
                    tmp=','.join(tmp).strip('() ').split(',')
                    count=0
                    for val in tmp:
                        conditions.append({
                            'attr': tmp_attr,
                            'value': val.strip(', '),
                            'symbol': '=',
                            'tag': 0 
                        })
                        count+=1
                        if count!=len(tmp):
                            conditions.append('OR')
                    temp=[]
                    if op:
                        while op:
                            conditions.append(op.pop(0))
                    continue
                elif ' NOT IN ' in temp.upper():
                    tmp=temp.split(' ')
                    tmp_attr=tmp.pop(0).lower()    # Pop attr
                    if tmp.pop(0).upper()!='IN': raise Exception('ERROR: Invalid Where Clause.')  # Pop IN
                    tmp=','.join(tmp).strip('() ').split(',')
                    count=0
                    for val in tmp:
                        conditions.append({
                            'attr': tmp_attr,
                            'value': val.strip(', '),
                            'symbol': '<>',
                            'tag': 0 
                        })
                        count+=1
                        if count!=len(tmp):
                            conditions.append('OR')
                    temp=[]
                    if op:
                        while op:
                            conditions.append(op.pop(0))
                    continue
                else: raise Exception('ERROR: Invalid where clause')
                conditions.append(condition)
                if op:
                    while op:
                        conditions.append(op.pop(0))
            # else: raise Exception('ERROR: Invalid where clause')
            temp=[]
        else:
            temp.append(where_clause[i])
    return conditions

def parse_attrs(attrs):
    # Input: list, aggragete attrs and normal attrs
    # Output: dict, key is attr, value is aggragation
    # ex. {id: max}
    # print(attrs)
    key_words=['MAX', 'MIN', 'AVG', 'COUNT', 'SUM']

    parse_attrs=dict()
    for attr in attrs:
        if '(' in attr and attr[-1]==')':
            temp=attr.split('(')
            # temp 0 is aggragate
            # temp 1 is attr
            agg_word=temp[0].strip('() ,').upper()
            attr_tmp=temp[1].strip('() ,').lower()
            if agg_word not in key_words: raise Exception('ERROR: Invalid syntax.')

            parse_attrs[attr_tmp]=agg_word
            
        elif '(' not in attr and ')' not in attr:
            parse_attrs[attr]='NORMAL'
        else: raise Exception('ERROR: Invalid syntax.')
    return parse_attrs

def parse_groupBy(groupBy_clause, attrs):
    att=attrs.copy()

    # TODO: Check attr and groupBy attr
    print(groupBy_clause)
    groupBy=[]
    having=[]
    for i in range(len(groupBy_clause)):
        if groupBy_clause[i].upper()=='HAVING':
            having=groupBy_clause[i+1:]
            break
        groupBy.append(groupBy_clause[i])

    if len(groupBy)==len(attrs):
        if set(groupBy)!=set(attrs.keys()):
            raise Exception('ERROR 1: Invalid group by clause.')
        if len(set(attrs.values()))!=1:
            raise Exception('ERROR 2: Group by attrs cannot have aggregate function')
        if 'NORMAL' not in list(attrs.values()):
            raise Exception('ERROR 3: Group by attrs cannot have aggregate function')
    else:
        for elem in groupBy:
            del att[elem]
        if 'NORMAL' in list(att.values()):
            raise Exception('ERROR 4: Invalid group by clause')


    conditions=reorder_where_clause(having)
    expression=parse_conditions(conditions)

    return {
        'group_by': groupBy,
        'conditions': expression
    }

def parse_orderBy(orderBy_clause):
    # Input: list, order by clause
    # Output: dict, {order_by: attr, order: desc/asc/no_action}
    print(orderBy_clause)
    key_words=['DESC', 'ASC']
    order_status='NO_ACTION'
    orderBy=[]

    if orderBy_clause[-1].upper() in key_words:
        order_status=orderBy_clause[-1].upper().strip()
        orderBy=orderBy_clause[: len(orderBy_clause)-1]
    else:
        orderBy=orderBy_clause

    return {
        'order_by': orderBy,
        'order': order_status
    }

def parse_conditions(con_ls):
    # Poland expression
    # op=['OR', 'AND']
    stack=[]
    ops=[]
    for item in con_ls:
        if str(item).upper() in ['OR', 'AND']:
            while len(ops)>=0:
                if len(ops)==0:
                    ops.append(item)
                    break
                op=ops.pop()
                if op=='(':
                    ops.append(op)
                    ops.append(item)
                    break
                else:
                    stack.append(op)
        elif item=='(':
            ops.append(item)
        elif item==')':
            while len(ops)>=0:
                op=ops.pop()
                if op=='(':
                    break
                else:
                    stack.append(op)
        else:
            stack.append(item)

    while len(ops)>0:
        stack.append(ops.pop())
    return stack

def save(action):
    return{
        'mainact' : 'save',
        'name' : action[-1]
    }

def use(action):
    return{
        'mainact' : 'use',
        'name' : action[-1]
    }

def show(action):
    if len(action) != 2:
        raise Exception('Syntax error! Recommend : show databases/tables ')
    if action[1] == 'databases':
        return{
            'mainact' : 'show',
            'type' : 'database'
        }
    elif action[1] == 'tables':
        return{
            'mainact' : 'show',
            'type' :'table'
        }
    else:
        raise Exception('Syntax error! Recommend : show databases/tables ')

def update(action):
    
    if action[0].upper()=='UPDATE':
        action.pop()
    table_name=action.pop()
    if action.pop().upper()!='SET': raise Exception('ERROR 1: Not SET syntax.')

    set_dict=[]
    while action[0].upper()!='WHERE':
        condition=action.pop().strip(', ')
        if '=' not in condition:
            raise Exception('ERROR 2: Invalid syntax.')
        tmp=condition.split('=')
        set_dict.append({
            'attr': tmp[0].lower(),
            'value': tmp[1],
        })
        if action==[]:
            break
        
    where_expression=[]
    if action:
        if action[0].upper()!='WHERE':
            raise Exception('ERROR 3: Invalid syntax.')
        action.pop()    #Pop where
        conditions=reorder_where_clause(action)

        # where clause poland expression
        where_expression=parse_conditions(conditions)   # Parse where clause
    return {
        'table': table_name,    # str->table name
        'set': set_dict,    # list->[{attr:, value:}]
        'where': where_expression,  #   list-> like where clause
    }

def delete(action):
    if action[0].upper()=='DELETE':
        action.pop()
    if action.pop(0).upper()!='FROM':
        raise Exception('ERROR 1: Invalid syntax.')
    table_name=action.pop(0).lower()

    where_expression=[]
    if action:
        if action.pop(0).upper()!='WHERE':
            raise Exception('ERROR 2: Invalid syntax')
        conditions=reorder_where_clause(action)

        # where clause poland expression
        where_expression=parse_conditions(conditions)   # Parse where clause
    return{
        'table': table_name,
        'where': where_expression,
    }

def create_index(action):
    if action[0].upper()=='CREATE':
        action.pop(0)
    if action.pop(0).upper()!='INDEX':
        raise Exception('ERROR 1: Invalid syntax.')

    idex_name=action.pop(0)

    if action.pop(0).upper()!='ON':
        raise Exception('ERROR 2: Invalid syntax.')
    table_name=action.pop(0)

    if '(' not in action[0] or ')' not in action[-1]:
        raise Exception('ERROR 3: Invalid syntax.')
    _str=' '.join(action).strip('() ')
    attrs=_str.split(',')
    attrs=list(map(str.strip, attrs))
    return {
        'mainact': 'create',
        'type': 'index',
        'index_name': idex_name,
        'table': table_name,
        'attrs': attrs,
    }

def drop_index(action):
    if action[0].upper()=='DROP':
        action.pop(0)

    if action.pop(0).upper()!='INDEX':
        raise Exception('ERROR 1: Invalid syntax.')
    idex_name=action.pop(0)

    if action.pop(0).upper()!='ON':
        raise Exception('ERROR 2: Invalid syntax.')

    table_name=action.pop(0)

    if action:
        raise Exception('ERROR 3: Invalid syntax.')

    return {
        'mainact': 'drop',
        'type': 'index',
        'index_name': idex_name,
        'table': table_name,
    }



main_action = {
    'EXIT' : exit,
    'CREATE' : create,
    'DROP' : drop,
    'INSERT' : insert,
    'SELECT' : select,
    'SAVE' : save,
    'SHOW' : show,
    'USE' : use,
    'UPDATE': update,
    'DELETE': delete,
}

def parse(commandline):
    # save the original commandline to use in error messages
    tmpcmdl = commandline

    # split commandline into list and the first word is main action
    action = commandline.split(' ')
    while True:
        if '' not in action:
            break
        action.remove('')

    mainaction = action[0].upper()
    validaction = ""
    for ac in main_action:
        validaction = validaction + " " + ac

    if mainaction not in main_action:
        raise Exception('Syntax error: ' + tmpcmdl + ' Valid action:' + validaction)
        
    act = main_action[mainaction](action)

    return act