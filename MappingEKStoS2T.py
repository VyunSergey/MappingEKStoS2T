import pandas as pd
# import xlrd
# import xlwt


if __name__ == '__main__':
    files_path = 'C:\\Users\\User\\PycharmProjects\\MappingEKStoS2T\\'

    EKS_tables_file_name = 'classes.csv'
    with open(file=files_path + EKS_tables_file_name, mode='r', encoding='utf-8') as EKS_tables_file:
        EKS_tables_file_header = EKS_tables_file.readline()
    EKS_tables_file_col_names = EKS_tables_file_header.replace('\n', '').split(';')
    EKS_tables_file_col_ID = EKS_tables_file_col_names[0]

    EKS_columns_file_name = 'classes_attributes.csv'
    with open(file=files_path + EKS_columns_file_name, mode='r', encoding='utf-8') as EKS_columns_file:
        EKS_columns_file_header = EKS_columns_file.readline()
    EKS_columns_file_col_names = EKS_columns_file_header.replace('\n', '').split(';')
    EKS_columns_file_col_ID = EKS_columns_file_col_names[0]
    EKS_columns_file_fin_col_names = [
        'id', 'name', 'entity_id', 'parent_id', 'has_instances', 'init_state_id', 'short_name', 'agregate',
        'base_class_id', 'interface', 'storage_group', 'properties', 'ods_validfrom', 'ods_opc', 'ods_sequenceid'
    ]

    EKS_tables_df = pd.read_csv(filepath_or_buffer=files_path + EKS_tables_file_name, delimiter=';', header=0,
                                names=EKS_tables_file_col_names, index_col=None, encoding='utf-8')
    EKS_tables_df.fillna('', inplace=True)
    EKS_tables_fin_df = EKS_tables_df[EKS_columns_file_fin_col_names]
    print(EKS_tables_fin_df)

    EKS_columns_df = pd.read_csv(filepath_or_buffer=files_path + EKS_columns_file_name, delimiter=';', header=0,
                                 names=EKS_columns_file_col_names, index_col=None, encoding='utf-8')
    EKS_columns_df.fillna('', inplace=True)
    print(EKS_columns_df)

    EKS_tables_with_columns_df = EKS_tables_fin_df.merge(right=EKS_columns_df, how='left',
                                                         left_on=EKS_tables_file_col_ID,
                                                         right_on=EKS_columns_file_col_ID)
    EKS_tables_with_columns_df.fillna('', inplace=True)

    EKS_table_AC_FIN_df = EKS_tables_with_columns_df.loc[
        EKS_tables_with_columns_df[EKS_tables_file_col_ID] == 'AC_FIN']
    EKS_table_AC_FIN_arr = EKS_table_AC_FIN_df.values

    print(EKS_table_AC_FIN_arr)

    result_file_name = 'result.csv'
    with open(file=files_path + result_file_name, mode='w', encoding='utf-8') as result_file:
        result_file.write(';'.join(EKS_tables_file_col_names + EKS_columns_file_fin_col_names) + '\n')
        for line in EKS_table_AC_FIN_arr:
            result_file.write(';'.join(map(lambda x: str(x), line)) + '\n')
