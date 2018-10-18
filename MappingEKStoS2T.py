import pandas as pd
# import xlrd
# import xlwt


if __name__ == '__main__':
    files_path = 'C:\\Users\\User\\PycharmProjects\\MappingEKStoS2T\\'

    EKS_tables_file_name = 'classes.csv'
    with open(file=files_path + EKS_tables_file_name, mode='r', encoding='utf-8') as EKS_tables_file:
        EKS_tables_file_header = EKS_tables_file.readline()
    EKS_tables_file_col_names = EKS_tables_file_header.replace('\n', '').split(';')

    EKS_columns_file_name = 'classes_attributes.csv'
    with open(file=files_path + EKS_columns_file_name, mode='r', encoding='utf-8') as EKS_columns_file:
        EKS_columns_file_header = EKS_columns_file.readline()
    EKS_columns_file_col_names = EKS_columns_file_header.replace('\n', '').split(';')

    EKS_tables_df = pd.read_csv(filepath_or_buffer=files_path + EKS_tables_file_name, delimiter=';', header=1,
                                names=EKS_tables_file_col_names, index_col=None, encoding='utf-8')
    EKS_tables_df.fillna('', inplace=True)
    print(EKS_tables_df)

    EKS_columns_df = pd.read_csv(filepath_or_buffer=files_path + EKS_columns_file_name, delimiter=';', header=1,
                                 names=EKS_columns_file_col_names, index_col=None, encoding='utf-8')
    EKS_columns_df.fillna('', inplace=True)
    print(EKS_columns_df)
