CREATE OR REPLACE PACKAGE BODY LND_MSTAR.XML_COMMON AS

    type t_varchar_varchar_arr is table of varchar2(100) index by varchar2(100);
    type t_number_varchar_arr is table of varchar2(100) index by varchar2(100);                 
    type t_varchar_arr is table of t_varchar_varchar_arr index by varchar2(100);
    
    m_short_names t_varchar_varchar_arr;
    m_tables t_varchar_arr;
    m_prefix varchar2(10) := null;
    
    
    procedure add_log(log_type varchar2, log_data varchar2) is
    begin
      insert into lnd_mstar.MSTAR_LND_LOG values(sysdate, log_type, log_data);
      commit;
    end;


    procedure initialize_short_names(a_table_prefix varchar2) is
        cursor c_short_names is
            select * from xml_name_alias where prefix = a_table_prefix;
    begin
        m_short_names.delete;
        for v_short_names in c_short_names loop
            m_short_names (upper((v_short_names.name || '`' || v_short_names.type))) := v_short_names.short_name;
        end loop;
    end;
    

    procedure initialize_tables(a_table_prefix varchar2) is
      v_cols t_varchar_varchar_arr;
      cursor cols is
        select table_name, column_name from USER_TAB_COLUMNS where table_name like a_table_prefix || '_%';
    begin
        m_prefix := a_table_prefix;
        m_tables.delete;   
        for col in cols loop
          if (not m_tables.exists(col.table_name)) then
            v_cols.delete;
            m_tables(col.table_name) := v_cols;
          end if;
          m_tables(col.table_name)(col.column_name) := 1;
        end loop;
    end;    
    
    
    function table_exists(a_table_name varchar2) return boolean is
    begin
      return m_tables.exists(a_table_name);                  
    end;
                    
    function column_exists(a_table_name varchar2, a_column_name varchar2) return boolean is
    begin
        return m_tables(a_table_name).exists(a_column_name);
    end;    
    
    procedure add_table (a_table_name varchar2) is
        v_cols t_varchar_varchar_arr;
    begin
        add_log('add_table','create table ' || a_table_name || ' (key number, parent varchar2(30), parent_key number) nologging');
        execute immediate 'create table ' || a_table_name || ' (key number, parent varchar2(30), parent_key number) nologging';
        commit;
        v_cols.delete;
        m_tables(a_table_name) := v_cols;
    end;    
             
    procedure add_column (a_table_name varchar2, a_column_name in varchar2) is
    begin     
        add_log('add_column','alter table ' || a_table_name || ' ADD (' || a_column_name || ' varchar2(4000))');
        execute immediate 'alter table ' || a_table_name || ' ADD (' || a_column_name || ' varchar2(4000))';     
        commit;
        m_tables(a_table_name)(a_column_name) := 1;
    end;
    
    function derive_short_name (a_name varchar2, a_type char, a_table_prefix varchar2) return varchar2 is
        v_derived_value varchar2(1000);
    begin    
        if (a_type = 'T') then
            v_derived_value := upper(a_table_prefix || '_' || replace(a_name,':',''));
            if (length(v_derived_value) > 30) then
                v_derived_value := regexp_replace(v_derived_value, '[AEIOU]', '');
            end if;        
        else
            v_derived_value := upper (case when substr (a_name, 1, 1) = '_' then 'A' || replace(a_name,':','') else replace(a_name,':','') end);
            if (length(v_derived_value) > 30) then
                v_derived_value := regexp_replace(v_derived_value, '[AEIOUY]', '');
            end if;
            v_derived_value := substr (v_derived_value, 1, 27);
            if  v_derived_value = 'INDEX' then
                v_derived_value := 'C_INDEX';
            elsif v_derived_value = 'DATE' then
                v_derived_value := 'C_DATE';
            elsif v_derived_value = 'PUBLIC' then
                v_derived_value := 'C_PUBLIC';
            elsif v_derived_value = 'LEVEL' then
                v_derived_value := 'C_LEVEL';
            elsif v_derived_value = 'OPTION' then
                v_derived_value := 'C_OPTION';                                                          
            end if;                
        end if;
        m_short_names (upper(a_name || '`' || a_type)) := v_derived_value;
        insert into xml_name_alias values (upper(a_name), a_type, upper(v_derived_value), a_table_prefix);
        commit;
        return v_derived_value;
    end;
    
    function get_short_name(a_name varchar2, a_type char, a_table_prefix varchar2) return varchar2 is
        v_return_value varchar2(100);
    begin
        if (m_short_names.count() = 0) then        
            initialize_short_names(a_table_prefix);
        end if;
        
        if a_name is null then 
            v_return_value := null;
        elsif (not m_short_names.exists(upper(a_name || '`' || a_type))) then 
            v_return_value := derive_short_name(a_name, a_type, a_table_prefix);            
        else
            v_return_value := m_short_names (upper(a_name || '`' || a_type));
        end if;            
                     
        return v_return_value;
    end;            
    
    function is_leaf(a_node dbms_xmldom.DOMNode) return boolean
    is
    begin
        if (not dbms_xmldom.haschildnodes(a_node) or dbms_xmldom.getnodename(dbms_xmldom.getfirstchild(a_node)) = '#text') then
            return true;
        else
            return false;
        end if;
    end;
    
    function get_leaf_value(a_node dbms_xmldom.DOMNode) return varchar2
    is
    begin
        if (dbms_xmldom.getnodetype(a_node) = dbms_xmldom.attribute_node) then
            return dbms_xmldom.getnodevalue(a_node);                    
        elsif (is_leaf(a_node)) then
            return dbms_xmldom.getnodevalue(dbms_xmldom.getfirstchild(a_node));
        end if;
    end;
    
    function get_leaf_name(a_node dbms_xmldom.DOMNode) return varchar2
    is
        v_return_value varchar2(4000);
    begin
        v_return_value := null;
        if (dbms_xmldom.getnodetype(a_node) = dbms_xmldom.attribute_node) then
            v_return_value := dbms_xmldom.getnodename(a_node);                    
        elsif (is_leaf(a_node)) then
            v_return_value := dbms_xmldom.getnodename(a_node);
        end if;
        return v_return_value;
    end;
    
    function get_leaf_nodes(a_node in out NOCOPY dbms_xmldom.domnode) return t_nodelist
    is
        v_return_value t_nodelist;
        v_attributes dbms_xmldom.DOMNamedNodeMap;
        v_children dbms_xmldom.DOMNodeList;
    begin
        v_return_value := t_nodelist();
        v_attributes := dbms_xmldom.getattributes(a_node);
        for j in 0..dbms_xmldom.getlength(v_attributes) - 1 loop
            v_return_value.extend(1);
            v_return_value(v_return_value.count()) := dbms_xmldom.item(v_attributes, j);
        end loop;
        v_children := dbms_xmldom.getChildNodes(a_node); 
        for j in 0..dbms_xmldom.getlength(v_children) - 1 loop
            if (is_leaf(dbms_xmldom.item(v_children, j))) then
                v_return_value.extend(1);
                v_return_value(v_return_value.count()) := dbms_xmldom.item(v_children, j);
            end if;                
        end loop; 
       
        return v_return_value;
    end;        
    
    function get_complex_nodes(a_node in out NOCOPY dbms_xmldom.domnode) return t_nodelist
    is
        v_return_value t_nodelist;
        v_children dbms_xmldom.DOMNodeList;
    begin
        v_return_value := t_nodelist();
        v_children := dbms_xmldom.getChildNodes(a_node); 
        for j in 0..dbms_xmldom.getlength(v_children) - 1 loop
            if (not is_leaf(dbms_xmldom.item(v_children, j))) then
                v_return_value.extend(1);
                v_return_value(v_return_value.count()) := dbms_xmldom.item(v_children, j);
            end if;                
        end loop;     
        return v_return_value;
    end;


    procedure store_node(a_node_name varchar2, a_leaves in out NOCOPY t_nodelist, a_key number, a_parent_name varchar2, a_parent_key number, a_table_prefix varchar2) is
        v_sql varchar2(32000);
        v_sql_values varchar2(32000); 
        v_column_name varchar2(4000);
        v_table_name varchar2(4000);
        v_col_counts t_number_varchar_arr;
    begin    
        v_col_counts.delete;                    
        v_table_name := get_short_name (a_node_name, 'T', a_table_prefix);
        if (not table_exists(v_table_name)) then
            add_table (v_table_name);                           
        end if;

        v_sql := 'insert /*+ append */ into ' || v_table_name || ' (key, parent, parent_key'; 
        v_sql_values := ') values (' || a_key || ',''' || get_short_name (a_parent_name, 'T', a_table_prefix) || ''', ' || case when a_parent_key is null then 'null' else to_char(a_parent_key) end;        
        
        for i in 1..a_leaves.count() loop
            v_column_name := get_short_name (xml_common.get_leaf_name(a_leaves(i)), 'C', a_table_prefix);                                
            if (not v_col_counts.exists(v_column_name)) then
                v_col_counts(v_column_name) := 0;
            else
                v_col_counts(v_column_name) := v_col_counts(v_column_name) + 1;
            end if;             
                                                                       
            if (v_col_counts(v_column_name) > 0) then                             
                v_column_name := v_column_name || '_' || v_col_counts(v_column_name);                
            end if;      
            if (not column_exists(v_table_name, v_column_name)) then
                add_column (v_table_name, v_column_name);                           
            end if;                                    
            v_sql := v_sql || ', ' || v_column_name;                                            
            v_sql_values := v_sql_values || ', ''' || replace (substr(xml_common.get_leaf_value(a_leaves(i)),1,4000), '''', '''''') || '''';
        end loop;                      
        v_sql := v_sql || v_sql_values || ')';
 
        --commit;
        --dbms_output.put_line(v_sql);
        execute immediate v_sql;
        
        v_sql := '';
        v_sql_values := '';
        a_leaves.delete;
  
    end;


    function get_next_key return number is
    begin
        return XML_SEQ.nextval;
    end;
         
    
    procedure persist_node(a_node in out NOCOPY dbms_xmldom.domnode, a_table_prefix varchar2, a_parent_name VARCHAR2 default null, a_parent_key number default null, a_lvl number default 0)
    is
        v_leaves xml_common.t_nodelist;
        v_non_leaves xml_common.t_nodelist;   
        v_key number;
    begin       
        if (nvl(m_prefix,'null') <> a_table_prefix) then
          m_prefix := a_table_prefix;
          initialize_tables(a_table_prefix);
        end if;
        v_leaves := get_leaf_nodes(a_node);
        v_key := get_next_key();
        store_node(dbms_xmldom.getnodename(a_node), v_leaves, v_key, a_parent_name, a_parent_key, a_table_prefix);
                
        
        v_non_leaves := xml_common.get_complex_nodes(a_node);
        for i in 1..v_non_leaves.count() loop
            persist_node(v_non_leaves(i), a_table_prefix, dbms_xmldom.getnodename(a_node), v_key, a_lvl + 2);
        end loop;
        
        v_leaves.delete;
        v_non_leaves.delete;
        v_non_leaves.delete;
        DBMS_XMLDOM.FREENODE(a_node);
        
        commit;
    end;    
    
                     
END;
/