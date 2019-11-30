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
                    data.append(value.strip('() '))
                    if ')' in elem:
                        return{
                            'mainact' : 'insert',
                            'table_name' : table_name,
                            'attrs' : attrs,
                            'data' : data
                        }

            else:
                raise Exception('ERROR: Invalid syntax')
            raise Exception('ERROR: Invalid syntax')
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
                    raise Exception('ERROR: No data')
                # print(attrs)
                if len(attrs)!=len(set(attrs)): 
                    raise Exception('ERROR: Duplicated attributes')
                if action[0].upper()!='VALUES':
                    raise Exception('ERROR: Invalid syntax')
                action.pop(0)

            else:
                raise Exception('ERROR: Invalid syntax')

            if '(' in action[0]:
                for elem in action:
                    if ')' in elem:
                        data.append(elem.strip('() ,'))
                        if len(attrs)!=len(data):
                            raise Exception('ERROR: Data length is not correponding to attributes.')

                        return{
                            'mainact' : 'insert',
                            'table_name' : table_name,
                            'attrs' : attrs,
                            'data' : data
                        }
                    data.append(elem.strip('() ,'))
                raise Exception('ERROR: Invalid syntax')
                
            else:
                raise Exception('ERROR: Invalid syntax')

    else:
        raise Exception('Syntax error! Recommend : insert into ')

def select(action):
    return{
        'mainact' : 'select',
        'content' : action[1:]
    }

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

main_action = {
    'EXIT' : exit,
    'CREATE' : create,
    'DROP' : drop,
    'INSERT' : insert,
    'SELECT' : select,
    'SAVE' : save,
    'SHOW' : show,
    'USE' : use,
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