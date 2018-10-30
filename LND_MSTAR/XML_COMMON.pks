CREATE OR REPLACE PACKAGE LND_MSTAR.XML_COMMON AS
    type t_nodelist is table of dbms_xmldom.domnode;

    function is_leaf(a_node dbms_xmldom.DOMNode) return boolean;
    function get_leaf_value(a_node dbms_xmldom.DOMNode) return varchar2;
    function get_leaf_name(a_node dbms_xmldom.DOMNode) return varchar2;
    function get_leaf_nodes(a_node in out NOCOPY dbms_xmldom.domnode) return t_nodelist;
    function get_complex_nodes(a_node in out NOCOPY dbms_xmldom.domnode) return t_nodelist;
    function get_short_name(a_name varchar2, a_type char, a_table_prefix varchar2) return varchar2;
    procedure persist_node(a_node in out NOCOPY dbms_xmldom.domnode, a_table_prefix varchar2, a_parent_name VARCHAR2 default null, a_parent_key number default null, a_lvl number default 0);
    procedure initialize_short_names(a_table_prefix varchar2);
    procedure initialize_tables(a_table_prefix varchar2);
END;
/