# coding=utf-8

from lxml import etree


def getfile(inputpath, defaultbasename):
    table_name = ''
    dic = {}
    # 读取文件
    file = open(inputpath, encoding='utf-8')
    lines = file.readlines(100000)
    # 遍历每行
    for line in lines:
        line = line.strip()
        # 如果该行没有实际的内容，则跳过
        if len(line) < 3 \
                or line.startswith(")")\
                or line.lower().startswith("UNIQUE KEY ".lower())\
                or line.lower().startswith("KEY ".lower())\
                or line.lower().startswith("PRIMARY KEY ".lower()):
            continue
        if line.startswith(")"):
            continue
        if "create table" in line.lower():
            table_name = line.replace("(", "").rstrip().split(" ")[-1].replace("`", "")
            if "." not in table_name:
                database_name = "defaultDatabaseName" if defaultbasename == "*" else defaultbasename
                table_name = database_name + "." + table_name
            cols = []
            dic[table_name] = cols
        else:
            col_name = line.strip().split(" ")[0]
            col_name = col_name.replace("`", "")
            dic[table_name].append(col_name)

    file.close()
    return dic


def ctl_field(ctlfieldlist):
    ctl_col = [
        "trans_tag",
        "rec_time",
        "log_file_name",
        "log_rec_pos",
        "log_rec_size",
        "operate",
        "proc_batch",
        "flag"
    ]
    if ctlfieldlist == "*":
        return ctl_col
    else:
        return list(ctlfieldlist)


def set_xml(inputpath, outputpath, defaultbasename, ctl_field_list):
    ctl_cols = ctl_field(ctl_field_list)
    dic = getfile(inputpath, defaultbasename)  # 获取到字段元素
    base_dic = {}  # 保存库名和表名

    # 构建库表字典，形如：{base1: [], base2: []}
    for tbname in dic.keys():
        base_dic[tbname.split(".")[0]] = []

    # 构建库表字典，形如：{base1: [table1,table2,...], base2: [table3,table4,...]}
    for fullname in dic.keys():
        bn = fullname.split(".")[0]
        tbn = fullname.split(".")[1]
        base_dic[bn].append(tbn)

    # 创建根节点
    schemas = etree.Element("schemas")

    for base in base_dic:
        schema = etree.Element('schema')  # 库节点
        schema.set("sourceDatabase", base)  # 库节点属性：库名
        for tb in base_dic[base]:
            table = etree.Element('table')  # 表节点
            table.set("insertSize", "20")  # 表节点属性
            table.set("sourceTableName", tb)
            table.set("condition", "")
            table.set("mysqlConnStrName", "")
            table.set("destinationTable", "destinationBase." + tb)
            full_tb_name = base + "." + tb
            for col in dic[full_tb_name]:
                field = etree.Element("filed")  # 字段节点
                field.set("fromCol", col)  # 字段属性
                field.set("toCol", col)
                table.append(field)  # 将字段添加到表
            for ctl_col in ctl_cols:
                field = etree.Element("ctlField")
                field.set("toCol", ctl_col)
                field.set("value", "")
                table.append(field)
            schema.append(table)  # 将表添加到库
        schemas.append(schema)  # 将库添加到根节点
    tree = etree.ElementTree(schemas)  # 生成tree对象
    tree.write(
        outputpath,
        pretty_print=True,
        xml_declaration=True,
        encoding='utf-8'
    )  # 写入到文件


if __name__ == '__main__':
    print("\n" + "*" * 40 + "NOTE" + "*" * 40)
    print(" " * 5 + "input filepath like '/file/createSql/filename'")
    print(" " * 5 + "output filepath like '/file/createSchema/filename'")
    print(" " * 5 + "set default database like 'klingon', or '*' for unnecessary")
    print(" " * 5 + "set controlField like [ctlField1,ctlField2,...], or '*' for all")
    print("*" * 80 + "\n")
    input_path = input("input path \n-->")
    output_path = input("output path \n-->")
    default_basename = input("default basename \n-->")
    ctlField_list = input("control fieldList \n-->")
    set_xml(input_path, output_path, default_basename, ctlField_list)
    # dic = getfile()
    # for k in dic.keys():
    #     for v in dic[k]:
    #         print(k, " - ", v)
