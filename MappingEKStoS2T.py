import pandas as pd
# import xlrd
# import xlwt


def read_csv(file_path, encoding, delimiter, exclude_columns=None):
    with open(file=file_path, mode='r', encoding=encoding) as file:
        file_line = file.readline()
    file_header = list(map(lambda x: str(x).strip().lower(), file_line.replace('\n', '').split(delimiter)))
    if exclude_columns is not None:
        file_header_cleared = [column_name for column_name in file_header if column_name not in exclude_columns]
    else:
        file_header_cleared = file_header
    file_col_id = file_header_cleared[0]

    file_data_frame = pd.read_csv(filepath_or_buffer=file_path, delimiter=delimiter, header=0,
                                  names=file_header, index_col=None, encoding=encoding)
    file_fin_data_frame = file_data_frame[file_header_cleared]
    file_fin_data_frame.fillna('', inplace=True)
    return [file_col_id, file_header_cleared, file_fin_data_frame]


def write_csv(file_path, encoding, delimiter, header, file_data):
    with open(file=file_path, mode='w', encoding=encoding) as file:
        file.write(delimiter.join(header) + '\n')
        for file_line in file_data:
            file.write(delimiter.join(map(lambda x: str(x), file_line)) + '\n')


if __name__ == '__main__':
    files_path = 'C:\\Users\\User\\PycharmProjects\\MappingEKStoS2T\\'
    EKS_tables_file_name = 'classes.csv'
    EKS_columns_file_name = 'classes_attributes.csv'

    EKS_tables_exclude_columns = ['has_instances', 'init_state_id', 'short_name', 'agregate', 'base_class_id',
                                  'target_class_id', 'criteria_id', 'data_size', 'data_precision', 'min_value',
                                  'max_value', 'default_value', 'autonomous', 'is_collection', 'modified', 'pad_style',
                                  'pad_char', 'pad_length', 'interface', 'kernel', 'param_group', 'tag', 'validator_id',
                                  'input_mask', 'trigger_id', 'storage_group', 'init_method_id', 'lob_storage_group',
                                  'temp_type', 'has_type', 'key_attr', 'data_precision_min', 'properties',
                                  'ods_validfrom', 'ods_opc', 'ods_sequenceid']

    EKS_tables_col_id, EKS_tables_header, EKS_tables_df = read_csv(file_path=files_path+EKS_tables_file_name,
                                                                   encoding='utf-8', delimiter=';',
                                                                   exclude_columns=EKS_tables_exclude_columns)
    print(EKS_tables_col_id)
    print(EKS_tables_header)
    print(EKS_tables_df.shape)

    EKS_columns_col_id, EKS_columns_header, EKS_columns_df = read_csv(file_path=files_path+EKS_columns_file_name,
                                                                      encoding='utf-8', delimiter=';',
                                                                      exclude_columns=[])
    print(EKS_columns_col_id)
    print(EKS_columns_header)
    print(EKS_columns_df.shape)

    EKS_tables_with_columns_df = EKS_tables_df.merge(right=EKS_columns_df, how='left',
                                                     left_on=EKS_tables_col_id,
                                                     right_on=EKS_columns_col_id)
    EKS_tables_with_columns_df.fillna('', inplace=True)
    EKS_tables_with_columns_df.drop([EKS_columns_col_id], axis=1, inplace=True)
    EKS_columns_header_cleared = [column for column in EKS_columns_header if column != EKS_columns_col_id]

    EKS_tables_dict = {}
    EKS_tables_names = sorted(list(set(EKS_tables_with_columns_df[EKS_tables_col_id].values)))
    for EKS_table_name in EKS_tables_names:
        EKS_tables_dict[EKS_table_name] = EKS_tables_with_columns_df.loc[
            EKS_tables_with_columns_df[EKS_tables_col_id] == EKS_table_name].values

    print(EKS_tables_dict['ABONENT'])

    result_file_name = 'result.csv'
    write_csv(file_path=files_path+result_file_name, encoding='utf-8', delimiter=';',
              header=EKS_tables_header+EKS_columns_header_cleared, file_data=EKS_tables_dict['ABONENT'])
