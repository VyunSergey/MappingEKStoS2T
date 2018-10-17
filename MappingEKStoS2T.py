import pandas as pd
# import xlrd
# import xlwt


if __name__ == '__main__':
    files_path = 'C:\\Users\\User\\PycharmProjects\\MappingEKStoS2T\\'

    EKS_tables_file = 'classes.csv'
    EKS_tables_file_col_names = [
        'id', 'name', 'entity_id', 'parent_id', 'has_instances', 'init_state_id', 'short_name', 'agregate',
        'base_class_id', 'target_class_id', 'criteria_id', 'data_size', 'data_precision', 'min_value', 'max_value',
        'default_value', 'autonomous', 'is_collection', 'modified', 'pad_style', 'pad_char', 'pad_length', 'interface',
        'kernel', 'param_group', 'tag', 'validator_id', 'input_mask', 'trigger_id', 'storage_group', 'init_method_id',
        'lob_storage_group', 'temp_type', 'has_type', 'key_attr', 'data_precision_min', 'properties', 'ods_validfrom',
        'ods_opc', 'ods_sequenceid'
    ]

    EKS_columns_file_ = 'classes_attributes.csv'
    EKS_columns_file_col_names = [
        'CLASS_ID', 'ATTR_ID', 'SELF_CLASS_ID', 'NAME', 'REQUIRED', 'POSITION', 'FILTERABLE', 'BROWSEABLE', 'SEQUENCED',
        'MODIFIED'
    ]

    EKS_tables_df = pd.read_csv(filepath_or_buffer=files_path+EKS_tables_file, delimiter=';', header=1,
                                names=EKS_tables_file_col_names, index_col=None, encoding='utf-8')
    EKS_tables_df.fillna('', inplace=True)
    print(EKS_tables_df)

    EKS_columns_df = pd.read_csv(filepath_or_buffer=files_path+EKS_columns_file_, delimiter=';', header=1,
                                 names=EKS_columns_file_col_names, index_col=None, encoding='utf-8')
    EKS_columns_df.fillna('', inplace=True)
    print(EKS_columns_df)
